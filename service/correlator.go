package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
)

const (
	correlationPolicy  = "pamawas-correlation-v1"
	correlationVersion = 1

	maxEventsPerCycle       = 500
	defaultCycleTimeout     = 2 * time.Minute
	defaultShutdownTimeout  = 15 * time.Second
	outboxRetention         = 24 * time.Hour
	outboxCleanupEveryTicks = 120 // 120 * 30s = 1 hour

	// outboxReasonIncidentCreated is the API-level investigation reason written
	// to investigation_outbox.reason. The schema constrains that column to
	// 'incident_created'/'incident_resolved'; it is NOT the correlation reason
	// (that lives on incident_events.correlation_reason).
	outboxReasonIncidentCreated = "incident_created"
)

// Correlation decisions recorded per event (correlation_decisions.decision).
const (
	decisionNewIncident      = "new_incident"
	decisionExtendedIncident = "extended_incident"
	decisionResolvedIncident = "resolved_incident"
	// decisionIgnored: the event produced no incident. A resolution with no open
	// incident for its condition is the only case.
	decisionIgnored = "ignored"
)

// incidentCandidate is an open incident that may receive the event being
// processed. Only the columns the mutation needs are loaded.
type incidentCandidate struct {
	ID       string
	Status   string
	Severity string
}

// eventIdentity returns the condition an event describes.
//
// Correlation is an identity problem, not a similarity problem. Two alerts
// that share a service and a title are still two conditions when they differ
// in any other label (koba-labs "Monitoring Target Down" on docker_cadvisor
// versus on mariadb), and the same alert's firing and resolved events are one
// condition. Grafana's `fingerprint` is a hash of exactly that label set, so
// when it is present the match is exact.
//
// Events without a fingerprint fall back to the only identity they carry:
// environment plus service plus alert title. That is coarser, but it is still
// deterministic — an event either belongs to a condition or it does not.
func eventIdentity(event models.Event) string {
	if fingerprint := strings.TrimSpace(event.Labels["grafana_fingerprint"]); fingerprint != "" {
		return "fp:" + fingerprint
	}
	return "cond:" + strings.ToLower(strings.TrimSpace(event.Service)) + "|" + strings.ToLower(strings.TrimSpace(event.Title))
}

// CircuitState is the typed state of the circuit breaker. Replaces the
// previous raw strings ("closed"/"open"/"half-open") per the design review
// recommendation to use an enum for breaker state.
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half-open"
)

// circuitBreaker tracks consecutive failures to the investigator and stops sending requests when unhealthy.
type circuitBreaker struct {
	mu           sync.Mutex
	failures     int
	lastFailure  time.Time
	state        CircuitState
	threshold    int
	resetTimeout time.Duration
}

func newCircuitBreaker(threshold int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:        CircuitClosed,
		threshold:    threshold,
		resetTimeout: resetTimeout,
	}
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	}
	return true
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = CircuitClosed
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.threshold {
		cb.state = CircuitOpen
	}
}

type Correlator struct {
	db            *sql.DB
	timeWindow    time.Duration
	interval      time.Duration
	mode          string
	mu            sync.Mutex
	lastRun       time.Time
	running       bool
	wg            sync.WaitGroup
	ctx           context.Context
	cancelFunc    context.CancelFunc
	startTime     time.Time
	metrics       *metrics.Metrics
	now           func() time.Time
	newIncidentID func(time.Time) string

	// Investigation outbox worker
	investigatorURL string
	serviceToken    string
	httpClient      *http.Client

	circuitBreaker  *circuitBreaker
	cycleTimeout    time.Duration
	shutdownTimeout time.Duration

	// Retry budget for outbox delivery attempts. When attempts reaches
	// outboxMaxAttempts the row is copied to investigation_outbox_dead_letter
	// and marked failed_terminal (no further retries).
	outboxMaxAttempts int
}

// CorrelatorConfig carries the constructor parameters for NewCorrelator.
// Circuit-breaker and retry-budget settings are centralized here (loaded from
// config.Config); callers must supply validated values — config.Config.
// Validate is the single place that enforces ranges.
type CorrelatorConfig struct {
	TimeWindow              time.Duration
	Interval                time.Duration
	Mode                    string
	Metrics                 *metrics.Metrics
	InvestigatorURL         string
	ServiceToken            string
	CircuitBreakerThreshold int
	CircuitBreakerReset     time.Duration
	OutboxMaxAttempts       int
}

func NewCorrelator(db *sql.DB, cfg CorrelatorConfig) *Correlator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Correlator{
		db:              db,
		timeWindow:      cfg.TimeWindow,
		interval:        cfg.Interval,
		mode:            cfg.Mode,
		ctx:             ctx,
		cancelFunc:      cancel,
		startTime:       time.Now(),
		metrics:         cfg.Metrics,
		now:             time.Now,
		newIncidentID:   func(time.Time) string { return "inc_" + uuid.NewString() },
		investigatorURL: cfg.InvestigatorURL,
		serviceToken:    cfg.ServiceToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		circuitBreaker:    newCircuitBreaker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerReset),
		cycleTimeout:      defaultCycleTimeout,
		shutdownTimeout:   defaultShutdownTimeout,
		outboxMaxAttempts: cfg.OutboxMaxAttempts,
	}
}

func (c *Correlator) MuLock()              { c.mu.Lock() }
func (c *Correlator) MuUnlock()            { c.mu.Unlock() }
func (c *Correlator) LastRun() time.Time   { return c.lastRun }
func (c *Correlator) Running() bool        { return c.running }
func (c *Correlator) StartTime() time.Time { return c.startTime }

// CircuitBreakerState returns the current circuit breaker state for observability.
func (c *Correlator) CircuitBreakerState() string {
	c.circuitBreaker.mu.Lock()
	defer c.circuitBreaker.mu.Unlock()
	return string(c.circuitBreaker.state)
}

func (c *Correlator) Correlate() error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = true
	if c.metrics != nil {
		c.metrics.CorrelatorRunning.Set(1)
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		if c.metrics != nil {
			c.metrics.CorrelatorRunning.Set(0)
		}
		c.mu.Unlock()
	}()
	c.wg.Add(1)
	defer c.wg.Done()

	// Each correlation cycle is bounded by a context timeout.
	cycleCtx, cancel := context.WithTimeout(c.ctx, c.cycleTimeout)
	defer cancel()

	start := c.now()
	processed := 0
	created := 0
	for processed < maxEventsPerCycle {
		didProcess, didCreate, err := c.processNextEvent(cycleCtx)
		if err != nil {
			if c.metrics != nil {
				c.metrics.CorrelationCyclesTotal.WithLabelValues("error").Inc()
			}
			return err
		}
		if !didProcess {
			break
		}
		processed++
		if didCreate {
			created++
		}
	}
	c.mu.Lock()
	c.lastRun = c.now()
	c.mu.Unlock()
	if c.metrics != nil {
		c.metrics.EventsProcessedTotal.Add(float64(processed))
		c.metrics.IncidentsCreatedTotal.Add(float64(created))
		c.metrics.LastRunTimestamp.Set(float64(c.lastRun.Unix()))
		c.metrics.CycleDuration.Observe(c.now().Sub(start).Seconds())
		c.metrics.CorrelationCyclesTotal.WithLabelValues("success").Inc()
	}
	return nil
}

func (c *Correlator) processNextEvent(ctx context.Context) (processed, created bool, err error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	event, scanErr := fetchPendingEvent(ctx, tx)
	if scanErr != nil {
		if scanErr == sql.ErrNoRows {
			commitErr := tx.Commit()
			if commitErr != nil {
				return false, false, commitErr
			}
			rollback = false
			return false, false, nil
		}
		return false, false, scanErr
	}

	identity := eventIdentity(event)
	candidate, matched, err := loadOpenIncidentByIdentity(ctx, tx, event.Environment, identity)
	if err != nil {
		return false, false, err
	}
	now := c.now()

	// Only a firing event can open an incident. Anything else either attaches
	// to the incident for its identity or, when there is none, is recorded and
	// skipped.
	//
	// A resolution that finds no open incident must NOT create one: the event
	// says the condition recovered, so an incident created from it would claim
	// the opposite, and no further event is coming to resolve it. This is what
	// produced open incidents that were "down 15 minutes" in a digest that also
	// listed them as recovered.
	//
	// An unlinked resolution is not silently normal, though. It means the firing
	// never reached the correlator — a dropped webhook, a restart, a filtering
	// mistake — and it is counted and logged as such so it can be alerted on
	// rather than hidden.
	if !matched && event.Status != "firing" {
		if err := recordCorrelationDecision(ctx, tx, event.ID, decisionIgnored, "", now); err != nil {
			return false, false, err
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return false, false, commitErr
		}
		rollback = false
		if c.metrics != nil {
			c.metrics.UnlinkedEventsTotal.WithLabelValues(event.Status).Inc()
		}
		log.Warn().
			Str("event_id", event.ID).
			Str("status", event.Status).
			Str("service", event.Service).
			Str("title", event.Title).
			Str("identity", identity).
			Msg("Event matched no open incident; recorded without creating one")
		return true, false, nil
	}

	incidentID, reason, created, mutationErr := c.applyIncidentMutation(ctx, tx, event, identity, candidate, matched, now)
	if mutationErr != nil {
		return false, false, mutationErr
	}

	_, execErr := tx.ExecContext(ctx, `INSERT INTO incident_events (incident_id,event_id,correlation_reason,policy_version,attached_at) VALUES ($1,$2,$3,$4,$5)`, incidentID, event.ID, reason, correlationVersion, now)
	if execErr != nil {
		return false, false, execErr
	}
	if err := recordCorrelationDecision(ctx, tx, event.ID, decisionForCorrelationReason(reason), incidentID, now); err != nil {
		return false, false, err
	}
	// Only dispatch an investigation for a newly created or extended incident.
	// A resolution needs no investigation; it is handled by the reporter's
	// resolve-notification flow.
	//
	// `reason` on the outbox row is the API-level investigation reason
	// (schema CHECK allows only 'incident_created'/'incident_resolved'). The
	// correlation reason lives on incident_events.correlation_reason and must
	// not be written here.
	if reason != "resolution_of_open_incident" {
		key := investigationRequestKey(incidentID, event.ID)
		_, execErr = tx.ExecContext(ctx, `INSERT INTO investigation_outbox (id,incident_id,contract_version,request_key_hash,status,attempts,next_attempt_at,reason,created_at,updated_at) VALUES ($1,$2,$3,$4,'pending',0,$5,$6,$7,$7) ON CONFLICT (request_key_hash) DO NOTHING`, outboxID(key), incidentID, correlationVersion, key, now, outboxReasonIncidentCreated, now)
		if execErr != nil {
			return false, false, execErr
		}
	}
	commitErr := tx.Commit()
	if commitErr != nil {
		return false, false, commitErr
	}
	rollback = false
	return true, created, nil
}

// fetchPendingEvent selects the next unprocessed event from the transaction.
// Returns sql.ErrNoRows when no pending event exists.
func fetchPendingEvent(ctx context.Context, tx *sql.Tx) (models.Event, error) {
	var event models.Event
	var labels []byte
	row := tx.QueryRowContext(ctx, `SELECT e.id, e.source, COALESCE(e.source_event_id, ''), e.type, e.occurred_at, COALESCE(e.service, ''), e.environment, e.severity, e.title, e.status, e.labels FROM events e WHERE NOT EXISTS (SELECT 1 FROM correlation_decisions cd WHERE cd.event_id = e.id AND cd.policy_version = $1) ORDER BY e.occurred_at, e.id FOR UPDATE SKIP LOCKED LIMIT 1`, correlationVersion)
	if err := row.Scan(&event.ID, &event.Source, &event.SourceEventID, &event.Type, &event.Timestamp, &event.Service, &event.Environment, &event.Severity, &event.Title, &event.Status, &labels); err != nil {
		return event, err
	}
	if err := json.Unmarshal(labels, &event.Labels); err != nil {
		return event, err
	}
	// Validate the event before it is correlated.
	if event.ID == "" {
		return event, errors.New("event has empty id")
	}
	if event.Environment == "" {
		return event, errors.New("event has empty environment")
	}
	if event.Timestamp.IsZero() {
		return event, errors.New("event has zero timestamp")
	}
	return event, nil
}

// loadOpenIncidentByIdentity finds the one open incident for a condition.
//
// There is at most one: migration 005 enforces
// UNIQUE (environment, identity_key) WHERE status IN ('open','investigating').
// That constraint is the real matcher — this query only has to look it up.
// Resolved incidents are deliberately not candidates; an alert that fires
// again after recovering is a new occurrence of the condition, and the
// reporter's recurrence count is what tells the on-call engineer how often it
// has happened.
func loadOpenIncidentByIdentity(ctx context.Context, tx *sql.Tx, environment, identity string) (incidentCandidate, bool, error) {
	var candidate incidentCandidate
	err := tx.QueryRowContext(ctx, `
		SELECT id, status, severity
		FROM incidents
		WHERE environment = $1
		  AND identity_key = $2
		  AND status IN ('open', 'investigating')
		FOR UPDATE
	`, environment, identity).Scan(&candidate.ID, &candidate.Status, &candidate.Severity)
	if err == sql.ErrNoRows {
		return incidentCandidate{}, false, nil
	}
	if err != nil {
		return incidentCandidate{}, false, err
	}
	return candidate, true, nil
}

// recordCorrelationDecision marks an event as processed and records what the
// correlator did with it.
//
// This is the "seen" marker, deliberately separate from incident_events: that
// table is the incident↔event link, and an event that produces no incident has
// nothing to link.
func recordCorrelationDecision(ctx context.Context, tx *sql.Tx, eventID, decision, incidentID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO correlation_decisions (event_id, policy_version, decision, incident_id, decided_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5)
		ON CONFLICT (event_id, policy_version) DO NOTHING
	`, eventID, correlationVersion, decision, incidentID, now)
	return err
}

// decisionForCorrelationReason maps a correlation reason onto its recorded
// decision.
func decisionForCorrelationReason(reason string) string {
	switch reason {
	case "new_incident":
		return decisionNewIncident
	case "resolution_of_open_incident":
		return decisionResolvedIncident
	default:
		return decisionExtendedIncident
	}
}

// applyIncidentMutation decides whether to INSERT a new incident or UPDATE an
// existing matched candidate, then executes the appropriate statement.
// Returns the resolved incident ID, correlation reason, whether a new incident
// was created, and any execution error.
// applyIncidentMutation opens an incident for a new condition, or extends the
// open incident for an existing one.
//
// The incident's identity columns — title, identity_key, affected_services —
// are written once, when the incident is opened, and never overwritten. They
// describe the condition; a later event can only move the incident's status,
// its last-seen time and its severity. Overwriting them is what let a RAM
// incident turn into a "Monitoring Target Down" incident for another host.
func (c *Correlator) applyIncidentMutation(ctx context.Context, tx *sql.Tx, event models.Event, identity string, candidate incidentCandidate, matched bool, now time.Time) (string, string, bool, error) {
	reduction := event.Status == "resolved" && matched
	reason := "new_incident"
	if matched {
		reason = "same_condition"
		if reduction {
			reason = "resolution_of_open_incident"
		}
	}
	var created bool
	var incidentID string
	if !matched {
		incidentID = c.newIncidentID(event.Timestamp)
		_, err := tx.ExecContext(ctx, `
			INSERT INTO incidents
				(id,title,identity_key,status,started_at,last_event_at,resolved_at,severity,environment,affected_services,correlation_policy,correlation_version,created_at,updated_at)
			VALUES ($1,$2,$3,'open',$4,$4,NULL,$5,$6,$7,$8,$9,$10,$10)
		`, incidentID, event.Title, identity, event.Timestamp, event.Severity, event.Environment, pq.Array(nonEmpty(event.Service)), correlationPolicy, correlationVersion, now)
		if err != nil {
			return "", "", false, err
		}
		created = true
	} else {
		incidentID = candidate.ID
		status := candidate.Status
		if reduction {
			status = "resolved"
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE incidents
			SET status = $1,
			    last_event_at = GREATEST(last_event_at, $2),
			    resolved_at = CASE WHEN $1 = 'resolved' THEN GREATEST(COALESCE(resolved_at, $2), $2) ELSE NULL END,
			    severity = $3,
			    updated_at = $4
			WHERE id = $5
		`, status, event.Timestamp, event.Severity, now, incidentID)
		if err != nil {
			return "", "", false, err
		}
	}
	return incidentID, reason, created, nil
}

func nonEmpty(s string) []string {
	if s == "" {
		return []string{}
	}
	return []string{s}
}

func investigationRequestKey(incidentID, eventID string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", incidentID, eventID, correlationVersion)))
	return hex.EncodeToString(sum[:])
}
func outboxID(key string) string { return "irout_" + key[:26] }

func (c *Correlator) StartWorker() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.Correlate(); err != nil {
				log.Error().Err(err).Msg("Correlation error")
			}
		}
	}
}

func (c *Correlator) StartOutboxWorker() {
	if c.investigatorURL == "" {
		log.Info().Msg("Investigator URL not configured, skipping outbox worker")
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(outboxRetention)
	defer cleanupTicker.Stop()
	cleanupTickCount := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.processOutbox(); err != nil {
				log.Error().Err(err).Msg("Outbox processing error")
			}
			cleanupTickCount++
			if cleanupTickCount >= outboxCleanupEveryTicks {
				if err := c.cleanupDeliveredOutbox(); err != nil {
					log.Error().Err(err).Msg("Outbox cleanup error")
				}
				cleanupTickCount = 0
			}
		case <-cleanupTicker.C:
			if err := c.cleanupDeliveredOutbox(); err != nil {
				log.Error().Err(err).Msg("Outbox cleanup error")
			}
		}
	}
}

// outboxItem represents a single pending entry in the investigation_outbox table.
type outboxItem struct {
	id              string
	incidentID      string
	contractVersion int
	requestKeyHash  string
	attempts        int
	reason          string // "incident_created" or "incident_resolved"
}

func (c *Correlator) processOutbox() error {
	const batchSize = 10
	const maxBatchesPerRun = 5

	batches := 0
	for batches < maxBatchesPerRun {
		batches++

		items, err := c.fetchAndLeaseOutboxItems(batchSize)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			c.updateOutboxPendingGauge()
			return nil
		}

		// Process each item outside the transaction. A single slow or failed
		// investigator call must not block the rest of the batch.
		for _, item := range items {
			if err := c.processOutboxItem(item); err != nil {
				log.Error().Err(err).Str("outbox_id", item.id).Str("incident_id", item.incidentID).Msg("Outbox item processing failed")
			}
		}
	}
	c.updateOutboxPendingGauge()
	return nil
}

// updateOutboxPendingGauge sets the correlator_outbox_pending gauge to the
// number of rows still awaiting delivery (pending, retryable, or leased with a
// live lease). The gauge was previously declared but never populated.
func (c *Correlator) updateOutboxPendingGauge() {
	if c.metrics == nil {
		return
	}
	var count int
	if err := c.db.QueryRowContext(c.ctx, `
		SELECT COUNT(*) FROM investigation_outbox
		WHERE status = 'pending'
		   OR status = 'retryable'
		   OR (status = 'leased' AND (lease_expires_at IS NULL OR lease_expires_at > now()))
	`).Scan(&count); err != nil {
		log.Error().Err(err).Msg("Failed to count pending outbox rows")
		return
	}
	c.metrics.OutboxPendingGauge.Set(float64(count))
}

// fetchAndLeaseOutboxItems queries pending outbox items and leases them in a
// single transaction so row locks are released promptly after commit.
func (c *Correlator) fetchAndLeaseOutboxItems(batchSize int) ([]outboxItem, error) {
	var items []outboxItem

	err := c.inTx(c.ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(c.ctx, `
			SELECT id, incident_id, contract_version, request_key_hash, attempts, reason
			FROM investigation_outbox
			WHERE status = 'pending'
			   OR (status = 'retryable' AND next_attempt_at <= now())
			   OR (status = 'leased' AND lease_expires_at <= now())
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		`, batchSize)
		if err != nil {
			return err
		}
		scanItems, scanErr := scanOutboxRows(rows)
		closeErr := rows.Close()
		if scanErr != nil {
			return scanErr
		}
		if closeErr != nil {
			return closeErr
		}
		items = scanItems

		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			if err := c.leaseOutboxItem(tx, item); err != nil {
				return err
			}
		}
		return nil
	})

	return items, err
}

// inTx executes fn within a transaction, committing on success and rolling back
// on error. All rollback failures are logged but do not mask the original error.
func (c *Correlator) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("failed to rollback transaction")
			}
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}

// scanOutboxRows reads all pending outbox items from the rows iterator.
func scanOutboxRows(rows *sql.Rows) ([]outboxItem, error) {
	var items []outboxItem
	for rows.Next() {
		var item outboxItem
		if err := rows.Scan(&item.id, &item.incidentID, &item.contractVersion, &item.requestKeyHash, &item.attempts, &item.reason); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// processOutboxItem leases a single outbox item (already leased by processOutbox),
// calls the investigator API outside of any transaction, and applies the response
// status to the outbox record in its own short transaction.
func (c *Correlator) processOutboxItem(item outboxItem) error {
	// The outbox worker is a background loop with no inbound request to inherit
	// a trace from, so it starts one here. Without this, every investigation
	// the correlator dispatches is invisible in Tempo even though the HTTP
	// request that carries it is a normal, traceable call.
	ctx, span := otel.Tracer("pamawas-correlator").Start(
		c.ctx,
		"outbox.deliver_investigation_request",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("pamawas.incident_id", item.incidentID),
			attribute.Int("pamawas.outbox_attempts", item.attempts),
		),
	)
	defer span.End()

	// If the circuit breaker is open, skip the HTTP call and retry later —
	// unless the retry budget is already exhausted, in which case the row is
	// dead-lettered instead of looping forever behind the breaker.
	if !c.circuitBreaker.allow() {
		if item.attempts >= c.outboxMaxAttempts {
			return c.deadLetterExhausted(item)
		}
		return c.markOutboxRetryable(item.id, item.attempts, true)
	}

	reqBody := map[string]interface{}{
		"contract_version":    item.contractVersion,
		"incident_id":         item.incidentID,
		"reason":              "incident_created",
		"correlation_version": item.contractVersion,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.investigatorURL+"/api/v1/investigations", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	// Carry the trace so the investigator's span joins this one instead of
	// starting a second, unrelated trace.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	req.Header.Set("Content-Type", "application/json")
	if c.serviceToken != "" {
		req.Header.Set("X-Service-Token", c.serviceToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.circuitBreaker.recordFailure()
		if item.attempts+1 >= c.outboxMaxAttempts {
			return c.deadLetterExhausted(item)
		}
		return c.markOutboxRetryable(item.id, item.attempts, false)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close response body")
		}
	}()

	deliveryStart := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.OutboxDeliveryDuration.Observe(time.Since(deliveryStart).Seconds())
		}
	}()

	return c.applyOutboxResponseStatus(item, resp)
}

// deadLetterExhausted copies an out-of-budget row to the dead-letter table and
// marks it failed_terminal, in one transaction. Used when the circuit breaker
// is open or the transport itself fails so many times that no HTTP response
// status is ever observed — the budget must still apply.
func (c *Correlator) deadLetterExhausted(item outboxItem) error {
	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()
	if _, err := tx.ExecContext(c.ctx, `
		INSERT INTO investigation_outbox_dead_letter
		    (id, incident_id, contract_version, request_key_hash, reason, attempts, safe_error_code)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`, item.id, item.incidentID, item.contractVersion, item.requestKeyHash,
		item.reason, item.attempts, "RETRY_BUDGET_EXHAUSTED"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(c.ctx, `
		UPDATE investigation_outbox
		SET status = 'failed_terminal', safe_error_code = 'RETRY_BUDGET_EXHAUSTED', updated_at = now()
		WHERE id = $1
	`, item.id); err != nil {
		return err
	}
	rollback = false
	if err := tx.Commit(); err != nil {
		return err
	}
	log.Warn().
		Str("outbox_id", item.id).
		Str("incident_id", item.incidentID).
		Int("attempts", item.attempts).
		Int("max_attempts", c.outboxMaxAttempts).
		Msg("outbox retry budget exhausted; row dead-lettered")
	return nil
}

// leaseOutboxItem transitions a single outbox item to the 'leased' state.
//
// A row that was already 'leased' with an expired lease (lease_expires_at <=
// now()) is a stale claim: the worker that took it died or hung before it could
// finish. Re-claiming it must not inflate the attempt counter, otherwise a
// single stuck row would burn through the retry budget and reach
// failed_terminal prematurely. Such rows are reset to a fresh attempt.
func (c *Correlator) leaseOutboxItem(tx *sql.Tx, item outboxItem) error {
	_, err := tx.ExecContext(c.ctx, `
		UPDATE investigation_outbox
		SET status = 'leased',
		    lease_expires_at = now() + interval '5 minutes',
		    attempts = CASE
			WHEN status = 'leased' AND lease_expires_at <= now() THEN 0
			ELSE attempts + 1
		    END,
		    next_attempt_at = CASE
			WHEN status = 'leased' AND lease_expires_at <= now() THEN now()
			ELSE next_attempt_at
		    END,
		    updated_at = now()
		WHERE id = $1
	`, item.id)
	return err
}

// computeOutboxBackoff returns the exponential backoff duration for an outbox
// item retry. Backoff grows 1m, 2m, 4m, 8m, 16m, 32m and is capped at 1 hour.
// It is computed in minutes first (as int64) to avoid time.Duration (int64 ns)
// overflow when attempts is large, then converted to time.Duration.
func computeOutboxBackoff(attempts int) time.Duration {
	backoffMinutes := int64(1)
	if attempts > 0 {
		shift := uint(attempts)
		if shift > 6 {
			shift = 6 // 1<<6 = 64 minutes, capped to 60 below
		}
		backoffMinutes = int64(1) << shift
	}
	if backoffMinutes > 60 {
		backoffMinutes = 60
	}
	if backoffMinutes < 1 {
		backoffMinutes = 1
	}
	return time.Duration(backoffMinutes) * time.Minute
}

// backoffMinutes returns the retry backoff in whole minutes for a given attempt
// count. It is the integer form of computeOutboxBackoff, used to build a
// Postgres interval in markOutboxRetryable. Returning minutes (not a Go
// time.Duration) avoids the lib/pq bigint/seconds bug where a time.Duration
// bound into `now() + $1` is interpreted by Postgres as seconds, pushing
// next_attempt_at centuries into the future and disabling retries entirely.
func backoffMinutes(attempts int) int64 {
	return int64(computeOutboxBackoff(attempts).Minutes())
}

// markOutboxRetryable transitions a single outbox item to the 'retryable' state
// after a transient error, using exponential backoff based on the attempt count.
//
// The backoff is written as an explicit Postgres interval ($1 * interval
// '1 minute') so the unit is unambiguous regardless of how the driver encodes
// the bind parameter. Previously the bind was a Go time.Duration (nanoseconds),
// which lib/pq sent as a bigint; Postgres then evaluated `timestamptz + bigint`
// as seconds, landing next_attempt_at in the year 3928 and permanently
// disabling retries for those rows.
func (c *Correlator) markOutboxRetryable(id string, attempts int, circuitOpen bool) error {
	var backoff int64
	if circuitOpen {
		// Circuit breaker reset timeout is 30s; express as 1 minute minimum so
		// the interval arithmetic stays correct and the row is retried promptly.
		backoff = 1
	} else {
		backoff = backoffMinutes(attempts)
	}
	if backoff < 1 {
		backoff = 1
	}

	if c.metrics != nil {
		c.metrics.OutboxRetryableTotal.Inc()
	}

	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()
	_, err = tx.ExecContext(c.ctx, `
		UPDATE investigation_outbox
		SET status = 'retryable', next_attempt_at = now() + ($1 * interval '1 minute'), updated_at = now()
		WHERE id = $2
	`, backoff, id)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}

// applyOutboxResponseStatus maps an investigator HTTP response status code to
// the corresponding outbox status update, managed in its own transaction.
//
// A 5xx on a row that has reached its retry budget (attempts+1 >=
// outboxMaxAttempts) is dead-lettered instead of retried: the row is copied to
// investigation_outbox_dead_letter (which has no FK to incidents, so it
// survives incident deletion) and marked failed_terminal with
// safe_error_code='RETRY_BUDGET_EXHAUSTED'. This is the P1 "no infinite retry
// loops" guarantee from the design review.
func (c *Correlator) applyOutboxResponseStatus(item outboxItem, resp *http.Response) error {
	var status string
	var safeErrorCode string
	recordFailure := false
	deadLetter := false

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		status = "delivered"
		c.circuitBreaker.recordSuccess()
		if c.metrics != nil {
			c.metrics.OutboxDeliveredTotal.WithLabelValues("success").Inc()
		}
	case resp.StatusCode == 409:
		// 409 means the investigator already has a run for this
		// (incident_id, request_key_hash): the dispatch reached it and was
		// idempotently rejected, which is a success for the outbox worker.
		status = "delivered"
		safeErrorCode = "INVESTIGATOR_CONFLICT"
		c.circuitBreaker.recordSuccess()
		if c.metrics != nil {
			c.metrics.OutboxDeliveredTotal.WithLabelValues("conflict").Inc()
		}
	case resp.StatusCode >= 500:
		recordFailure = true
		if c.metrics != nil {
			c.metrics.OutboxFailedTotal.WithLabelValues("server_error").Inc()
		}
		if item.attempts+1 >= c.outboxMaxAttempts {
			deadLetter = true
			status = "failed_terminal"
			safeErrorCode = "RETRY_BUDGET_EXHAUSTED"
		} else {
			status = "retryable"
		}
	default:
		status = "failed_terminal"
		safeErrorCode = "INVESTIGATOR_ERROR"
		if c.metrics != nil {
			c.metrics.OutboxFailedTotal.WithLabelValues("client_error").Inc()
		}
	}

	if recordFailure {
		c.circuitBreaker.recordFailure()
	}

	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	if deadLetter {
		// Copy the exhausted row into the dead-letter table before flipping its
		// status, in the same transaction, so a row can never be lost between
		// "retryable forever" and "terminal with no trace".
		if _, err = tx.ExecContext(c.ctx, `
			INSERT INTO investigation_outbox_dead_letter
			    (id, incident_id, contract_version, request_key_hash, reason, attempts, safe_error_code)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING
		`, item.id, item.incidentID, item.contractVersion, item.requestKeyHash,
			item.reason, item.attempts+1, safeErrorCode); err != nil {
			return err
		}
		log.Warn().
			Str("outbox_id", item.id).
			Str("incident_id", item.incidentID).
			Int("attempts", item.attempts+1).
			Int("max_attempts", c.outboxMaxAttempts).
			Msg("outbox retry budget exhausted; row dead-lettered")
	}

	query := `UPDATE investigation_outbox SET status = $1, updated_at = now() WHERE id = $2`
	args := []interface{}{status, item.id}
	if safeErrorCode != "" {
		query = `UPDATE investigation_outbox SET status = $1, safe_error_code = $2, updated_at = now() WHERE id = $3`
		args = []interface{}{status, safeErrorCode, item.id}
	}
	if status == "retryable" {
		query = `UPDATE investigation_outbox SET status = $1, updated_at = now(),
		         next_attempt_at = now() + ($2 * interval '1 minute') WHERE id = $3`
		args = []interface{}{status, backoffMinutes(item.attempts), item.id}
	}
	_, err = tx.ExecContext(c.ctx, query, args...)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}

func (c *Correlator) Stop() error {
	c.cancelFunc()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(c.shutdownTimeout):
		return errors.New("correlator shutdown timed out waiting for in-flight correlation cycle")
	}
}

// cleanupDeliveredOutbox removes delivered outbox items older than the retention
// window to prevent unbounded table growth.
func (c *Correlator) cleanupDeliveredOutbox() error {
	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()
	// Bind the cutoff as a timestamp rather than binding the duration and
	// subtracting it in SQL: an untyped parameter next to `now()` makes
	// Postgres resolve `now() - $1` as timestamp-minus-timestamp, which yields
	// an interval and fails with "operator does not exist: timestamptz <
	// interval". Computing the cutoff here is unambiguous.
	cutoff := time.Now().Add(-outboxRetention)
	_, err = tx.ExecContext(c.ctx, `
		DELETE FROM investigation_outbox
		WHERE status = 'delivered' AND updated_at < $1
	`, cutoff)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}
