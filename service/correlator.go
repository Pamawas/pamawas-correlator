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
	"github.com/wneessen/go-mail"

	"github.com/Pamawas/pamawas-correlator/config"
	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
)

const (
	correlationPolicy  = "pamawas-correlation-v1"
	correlationVersion = 1

	maxEventsPerCycle     = 500
	defaultCycleTimeout   = 2 * time.Minute
	defaultShutdownTimeout = 15 * time.Second
	circuitBreakerThreshold = 5
	circuitBreakerResetTimeout = 30 * time.Second
	outboxRetention       = 24 * time.Hour
	outboxCleanupEveryTicks = 120 // 120 * 30s = 1 hour
)

var identityLabelScores = map[string]int{"alert_rule": 40, "namespace": 20, "workload": 20, "instance": 20}

type incidentCandidate struct {
	ID            string
	Status        string
	LastEventAt   time.Time
	StartedAt     time.Time
	CreatedAt     time.Time
	Environment   string
	Service       string
	Source        string
	SourceEventID string
	Severity      string
	Labels        map[string]string
}

func selectCandidate(event models.Event, candidates []incidentCandidate) (incidentCandidate, int, bool) {
	var selected incidentCandidate
	selectedScore := -1
	found := false
	for _, candidate := range candidates {
		if candidate.Environment != event.Environment || (candidate.Status != "" && candidate.Status != "open" && candidate.Status != "investigating") {
			continue
		}
		if event.Timestamp.After(candidate.LastEventAt.Add(15*time.Minute)) || event.Timestamp.Before(candidate.LastEventAt.Add(-5*time.Minute)) || event.Timestamp.After(candidate.StartedAt.Add(6*time.Hour)) {
			continue
		}
		score := candidateScore(event, candidate)
		if score < 50 {
			continue
		}
		if !found || score > selectedScore || score == selectedScore && candidateBefore(candidate, selected) {
			selected, selectedScore, found = candidate, score, true
		}
	}
	return selected, selectedScore, found
}

func candidateScore(event models.Event, candidate incidentCandidate) int {
	score := 0
	if event.SourceEventID != "" && event.Source == candidate.Source && event.SourceEventID == candidate.SourceEventID {
		score += 100
	}
	if grafanaFingerprintMatches(event, candidate) {
		score += 100
	}
	if event.Service != "" && event.Service == candidate.Service {
		score += 50
	}
	for label, weight := range identityLabelScores {
		if event.Labels[label] != "" && event.Labels[label] == candidate.Labels[label] {
			score += weight
		}
	}
	return score
}

func candidateBefore(a, b incidentCandidate) bool {
	if !a.LastEventAt.Equal(b.LastEventAt) {
		return a.LastEventAt.After(b.LastEventAt)
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.Before(b.CreatedAt)
	}
	return a.ID < b.ID
}

func acceptedResolution(event models.Event, candidate incidentCandidate) bool {
	if event.SourceEventID != "" && event.Source == candidate.Source && event.SourceEventID == candidate.SourceEventID {
		return true
	}
	if grafanaFingerprintMatches(event, candidate) {
		return true
	}
	if event.Service == "" || event.Service != candidate.Service {
		return false
	}
	for label := range identityLabelScores {
		if event.Labels[label] != "" && event.Labels[label] == candidate.Labels[label] {
			return true
		}
	}
	return false
}

func grafanaFingerprintMatches(event models.Event, candidate incidentCandidate) bool {
	fingerprint := event.Labels["grafana_fingerprint"]
	return event.Source == "grafana" && candidate.Source == "grafana" &&
		fingerprint != "" && fingerprint == candidate.Labels["grafana_fingerprint"]
}

// circuitBreaker tracks consecutive failures to the investigator and stops
// sending requests when the downstream is unhealthy.
type circuitBreaker struct {
	mu            sync.Mutex
	failures      int
	lastFailure   time.Time
	state         string // "closed", "open", "half-open"
	threshold     int
	resetTimeout  time.Duration
}

func newCircuitBreaker(threshold int, resetTimeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		state:       "closed",
		threshold:   threshold,
		resetTimeout: resetTimeout,
	}
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == "open" {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = "half-open"
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
	cb.state = "closed"
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	if cb.failures >= cb.threshold {
		cb.state = "open"
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

	// Notification channels for incident-resolved alerts (high/critical only)
	discordWebhookURL  string
	telegramBotToken   string
	telegramChatID     string
	telegramAPIBaseURL string
	emailSMTPHost      string
	emailSMTPPort      int
	emailUsername      string
	emailPassword      string
	emailFrom          string
	emailTo            string

	circuitBreaker  *circuitBreaker
	cycleTimeout    time.Duration
	shutdownTimeout time.Duration
}

func NewCorrelator(db *sql.DB, timeWindow, interval time.Duration, mode string, m *metrics.Metrics, investigatorURL, serviceToken string, cfg config.Config) *Correlator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Correlator{
		db:              db,
		timeWindow:      timeWindow,
		interval:        interval,
		mode:            mode,
		ctx:             ctx,
		cancelFunc:      cancel,
		startTime:       time.Now(),
		metrics:         m,
		now:             time.Now,
		newIncidentID:   func(time.Time) string { return "inc_" + uuid.NewString() },
		investigatorURL: investigatorURL,
		serviceToken:    serviceToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		discordWebhookURL:  cfg.DiscordWebhookURL,
		telegramBotToken:   cfg.TelegramBotToken,
		telegramChatID:     cfg.TelegramChatID,
		telegramAPIBaseURL: cfg.TelegramAPIBaseURL,
		emailSMTPHost:      cfg.EmailSMTPHost,
		emailSMTPPort:      cfg.EmailSMTPPort,
		emailUsername:      cfg.EmailUsername,
		emailPassword:      cfg.EmailPassword,
		emailFrom:          cfg.EmailFrom,
		emailTo:            cfg.EmailTo,
		circuitBreaker:  newCircuitBreaker(circuitBreakerThreshold, circuitBreakerResetTimeout),
		cycleTimeout:    defaultCycleTimeout,
		shutdownTimeout: defaultShutdownTimeout,
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
	return c.circuitBreaker.state
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

	// Each correlation cycle is bounded by a context timeout so a slow or hung
	// database query cannot block the worker indefinitely.
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

	candidates, err := loadAdjacentCandidates(ctx, tx, event)
	if err != nil {
		return false, false, err
	}

	candidate, _, matched := selectCandidate(event, candidates)
	now := c.now()
	incidentID, reason, created, mutationErr := c.applyIncidentMutation(ctx, tx, event, candidate, matched, now)
	if mutationErr != nil {
		return false, false, mutationErr
	}

	_, execErr := tx.ExecContext(ctx, `INSERT INTO incident_events (incident_id,event_id,correlation_reason,policy_version,attached_at) VALUES ($1,$2,$3,$4,$5)`, incidentID, event.ID, reason, correlationVersion, now)
	if execErr != nil {
		return false, false, execErr
	}
	key := investigationRequestKey(incidentID, event.ID)
	outboxReason := reason
	if outboxReason == "resolution_of_open_incident" {
		outboxReason = "incident_resolved"
	} else {
		outboxReason = "incident_created"
	}
	_, execErr = tx.ExecContext(ctx, `INSERT INTO investigation_outbox (id,incident_id,contract_version,request_key_hash,status,attempts,next_attempt_at,reason,created_at,updated_at) VALUES ($1,$2,$3,$4,'pending',0,$5,$6,$7,$7,$7) ON CONFLICT (request_key_hash) DO NOTHING`, outboxID(key), incidentID, correlationVersion, key, now, outboxReason, now)
	if execErr != nil {
		return false, false, execErr
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
	row := tx.QueryRowContext(ctx, `SELECT e.id, e.source, COALESCE(e.source_event_id, ''), e.type, e.occurred_at, COALESCE(e.service, ''), e.environment, e.severity, e.title, e.status, e.labels FROM events e WHERE NOT EXISTS (SELECT 1 FROM incident_events ie WHERE ie.event_id = e.id AND ie.policy_version = $1) ORDER BY e.occurred_at, e.id FOR UPDATE SKIP LOCKED LIMIT 1`, correlationVersion)
	if err := row.Scan(&event.ID, &event.Source, &event.SourceEventID, &event.Type, &event.Timestamp, &event.Service, &event.Environment, &event.Severity, &event.Title, &event.Status, &labels); err != nil {
		return event, err
	}
	if err := json.Unmarshal(labels, &event.Labels); err != nil {
		return event, err
	}
	// Validate the event before it is correlated. Malformed events are rejected
	// early to prevent them from creating bad incidents or blocking the worker.
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

// loadAdjacentCandidates queries open/investigating incidents in the same
// environment that fall within the time-correlation window of the event.
func loadAdjacentCandidates(ctx context.Context, tx *sql.Tx, event models.Event) ([]incidentCandidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT i.id, i.status, i.started_at, i.last_event_at, i.resolved_at, i.severity, i.environment, i.affected_services, i.created_at, COALESCE((SELECT e2.service FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.source FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.source_event_id FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.labels || CASE WHEN e2.source='grafana' AND NULLIF(e2.raw_payload->'alerts'->0->>'fingerprint','') IS NOT NULL THEN jsonb_build_object('grafana_fingerprint', e2.raw_payload->'alerts'->0->>'fingerprint') ELSE '{}'::jsonb END FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), '{}'::jsonb) FROM incidents i WHERE i.environment=$1 AND i.status IN ('open','investigating') AND i.last_event_at >= $2 AND i.last_event_at <= $3 AND i.started_at >= $4 ORDER BY i.last_event_at DESC, i.created_at, i.id FOR UPDATE OF i SKIP LOCKED`, event.Environment, event.Timestamp.Add(-15*time.Minute), event.Timestamp.Add(5*time.Minute), event.Timestamp.Add(-6*time.Hour))
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close rows")
		}
	}()

	var candidates []incidentCandidate
	for rows.Next() {
		x, scanErr := scanCandidate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, x)
	}
	return candidates, rows.Err()
}

// scanCandidate extracts a single incidentCandidate from a rows iterator.
func scanCandidate(rows *sql.Rows) (incidentCandidate, error) {
	var x incidentCandidate
	var services pq.StringArray
	var lb []byte
	if err := rows.Scan(&x.ID, &x.Status, &x.StartedAt, &x.LastEventAt, new(sql.NullTime), &x.Severity, &x.Environment, &services, &x.CreatedAt, &x.Service, &x.Source, &x.SourceEventID, &lb); err != nil {
		return x, err
	}
	x.Labels = map[string]string{}
	if err := json.Unmarshal(lb, &x.Labels); err != nil {
		return x, err
	}
	return x, nil
}

// applyIncidentMutation decides whether to INSERT a new incident or UPDATE an
// existing matched candidate, then executes the appropriate statement.
// Returns the resolved incident ID, correlation reason, whether a new incident
// was created, and any execution error.
func (c *Correlator) applyIncidentMutation(ctx context.Context, tx *sql.Tx, event models.Event, candidate incidentCandidate, matched bool, now time.Time) (string, string, bool, error) {
	reduction := event.Status == "resolved" && matched && acceptedResolution(event, candidate)
	reason := "new_incident"
	if matched {
		reason = "same_service_time_window"
		if reduction {
			reason = "resolution_of_open_incident"
		}
	}
	var created bool
	var incidentID string
	if !matched {
		incidentID = c.newIncidentID(event.Timestamp)
		_, err := tx.ExecContext(ctx, `INSERT INTO incidents (id,title,status,started_at,last_event_at,resolved_at,severity,environment,affected_services,correlation_policy,correlation_version,created_at,updated_at) VALUES ($1,$2,'open',$3,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, incidentID, event.Title, event.Timestamp, nil, event.Severity, event.Environment, pq.Array(nonEmpty(event.Service)), correlationPolicy, correlationVersion, now, now)
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
		_, err := tx.ExecContext(ctx, `UPDATE incidents SET title=$1,status=$2,last_event_at=GREATEST(last_event_at,$3),resolved_at=CASE WHEN $2='resolved' THEN GREATEST(COALESCE(resolved_at,$3),$3) ELSE resolved_at END,severity=$4,affected_services=$5,updated_at=$6 WHERE id=$7`, event.Title, status, event.Timestamp, event.Severity, pq.Array(nonEmpty(event.Service)), now, incidentID)
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

		tx, err := c.db.BeginTx(c.ctx, nil)
		if err != nil {
			return err
		}

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
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("failed to rollback transaction after query error")
			}
			return err
		}

		items, err := scanOutboxRows(rows)
		if closeErr := rows.Close(); closeErr != nil {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("failed to rollback transaction after rows close error")
			}
			if err != nil {
				return err
			}
			return closeErr
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
				log.Error().Err(rbErr).Msg("failed to rollback transaction after scan error")
			}
			return err
		}

		if len(items) == 0 {
			if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}

		// Lease all items in this transaction, then commit promptly so row locks
		// are not held during the downstream HTTP call.
		for _, item := range items {
			if err := c.leaseOutboxItem(tx, item); err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
					log.Error().Err(rbErr).Msg("failed to rollback transaction after lease error")
				}
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		// Process each item outside the transaction. A single slow or failed
		// investigator call must not block the rest of the batch.
		for _, item := range items {
			if err := c.processOutboxItem(item); err != nil {
				log.Error().Err(err).Str("outbox_id", item.id).Str("incident_id", item.incidentID).Msg("Outbox item processing failed")
			}
		}
	}
	return nil
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
// Items with reason "incident_resolved" (high/critical only) send an immediate
// user-facing alert-solved notification via Discord/Telegram/Email instead of
// calling the investigator.
func (c *Correlator) processOutboxItem(item outboxItem) error {
	if item.reason == "incident_resolved" {
		return c.sendIncidentResolvedNotification(item.id, item.incidentID)
	}

	// If the circuit breaker is open, skip the HTTP call and retry later.
	if !c.circuitBreaker.allow() {
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

	req, err := http.NewRequestWithContext(c.ctx, "POST", c.investigatorURL+"/api/v1/investigations", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.serviceToken != "" {
		req.Header.Set("X-Service-Token", c.serviceToken)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.circuitBreaker.recordFailure()
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

	return c.applyOutboxResponseStatusStandalone(item.id, resp)
}

// sendIncidentResolvedNotification fetches the incident and sends an immediate
// "alert solved" notification via Discord/Telegram/Email for high/critical
// severity alerts only. Low severity alerts produce no user-facing notification.
func (c *Correlator) sendIncidentResolvedNotification(outboxID, incidentID string) error {
	var (
		title       string
		severity    string
		environment string
		resolvedAt  time.Time
		startedAt   time.Time
	)
	row := c.db.QueryRowContext(c.ctx, `SELECT title, severity, environment, resolved_at, started_at FROM incidents WHERE id = $1`, incidentID)
	if err := row.Scan(&title, &severity, &environment, &resolvedAt, &startedAt); err != nil {
		if err == sql.ErrNoRows {
			log.Warn().Str("incident_id", incidentID).Msg("Incident not found for resolve notification, marking delivered")
			return nil
		}
		return err
	}

	// High/critical only: skip low-level alerts.
	if severity != "high" && severity != "critical" {
		log.Info().Str("incident_id", incidentID).Str("severity", severity).Msg("Resolve notification skipped: not high/critical severity")
		return nil
	}

	msg := formatResolveNotification(incidentID, title, severity, environment, resolvedAt, startedAt)

	// Send via all configured channels (best effort; failures are logged, not retried
	// here — the outbox item will be retried by the worker on the next cycle if marked retryable).
	var firstErr error

	if c.discordWebhookURL != "" {
		if err := c.sendDiscord(c.ctx, msg); err != nil {
			log.Error().Err(err).Str("incident_id", incidentID).Msg("Discord resolve notification failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if c.telegramBotToken != "" && c.telegramChatID != "" {
		if err := c.sendTelegram(c.ctx, msg); err != nil {
			log.Error().Err(err).Str("incident_id", incidentID).Msg("Telegram resolve notification failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if c.emailSMTPHost != "" && c.emailUsername != "" && c.emailPassword != "" {
		if err := c.sendEmail(c.ctx, msg, title); err != nil {
			log.Error().Err(err).Str("incident_id", incidentID).Msg("Email resolve notification failed")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Mark the outbox item delivered even if some channels failed — this is a
	// best-effort user notification, not a blocking downstream dependency.
	if err := c.markOutboxDelivered(outboxID); err != nil {
		return err
	}
	return firstErr
}

// markOutboxDelivered transitions an outbox item to 'delivered'.
func (c *Correlator) markOutboxDelivered(id string) error {
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
		SET status = 'delivered', updated_at = now()
		WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}

// sendDiscord sends an alert-resolved message via Discord webhook.
func (c *Correlator) sendDiscord(ctx context.Context, content string) error {
	payload := map[string]interface{}{
		"content":  content,
		"username": "Pamawas Alert",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.discordWebhookURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close Discord response body")
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discord webhook returned status %d", resp.StatusCode)
	}
	return nil
}

// sendTelegram sends an alert-resolved message via Telegram Bot API.
func (c *Correlator) sendTelegram(ctx context.Context, content string) error {
	telegramBase := c.telegramAPIBaseURL
	if telegramBase == "" {
		telegramBase = "https://api.telegram.org"
	}
	url := fmt.Sprintf("%s/bot%s/sendMessage", telegramBase, c.telegramBotToken)
	payload := map[string]interface{}{
		"chat_id": c.telegramChatID,
		"text":    content,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close Telegram response body")
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram api returned status %d", resp.StatusCode)
	}
	return nil
}

// sendEmail sends an alert-resolved message via SMTP.
func (c *Correlator) sendEmail(ctx context.Context, body, subject string) error {
	if c.emailSMTPHost == "" || c.emailUsername == "" || c.emailPassword == "" {
		return nil
	}
	client, err := mail.NewClient(c.emailSMTPHost,
		mail.WithPort(c.emailSMTPPort),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(c.emailUsername),
		mail.WithPassword(c.emailPassword),
		mail.WithTLSPortPolicy(mail.TLSMandatory),
	)
	if err != nil {
		return fmt.Errorf("failed to create SMTP client: %w", err)
	}
	msg := mail.NewMsg()
	if err := msg.From(c.emailFrom); err != nil {
		return fmt.Errorf("failed to set from address: %w", err)
	}
	if err := msg.To(c.emailTo); err != nil {
		return fmt.Errorf("failed to set to address: %w", err)
	}
	msg.Subject(subject)
	msg.SetBodyString(mail.TypeTextPlain, body)
	emailCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.DialAndSendWithContext(emailCtx, msg); err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}
	log.Info().Str("smtp_host", c.emailSMTPHost).Str("to", c.emailTo).Msg("Resolve notification email sent successfully")
	return nil
}

// formatResolveNotification builds a human-readable alert-solved message.
func formatResolveNotification(incidentID, title, severity, environment string, resolvedAt, startedAt time.Time) string {
	var sb strings.Builder
	sb.WriteString("✅ *Alert Resolved*\n")
	sb.WriteString(fmt.Sprintf("Incident: %s (%s)\n", title, incidentID))
	sb.WriteString(fmt.Sprintf("Severity: %s\n", severity))
	sb.WriteString(fmt.Sprintf("Environment: %s\n", environment))
	if !startedAt.IsZero() {
		sb.WriteString(fmt.Sprintf("Started: %s\n", startedAt.UTC().Format(time.RFC3339)))
	}
	if !resolvedAt.IsZero() {
		sb.WriteString(fmt.Sprintf("Resolved: %s\n", resolvedAt.UTC().Format(time.RFC3339)))
	}
	sb.WriteString("The alert has recovered and is no longer firing.")
	return sb.String()
}

// leaseOutboxItem transitions a single outbox item to the 'leased' state.
func (c *Correlator) leaseOutboxItem(tx *sql.Tx, item outboxItem) error {
	_, err := tx.ExecContext(c.ctx, `
		UPDATE investigation_outbox
		SET status = 'leased', lease_expires_at = now() + interval '5 minutes', attempts = attempts + 1, updated_at = now()
		WHERE id = $1
	`, item.id)
	return err
}

// markOutboxRetryable transitions a single outbox item to the 'retryable' state
// after a transient error, using exponential backoff based on the attempt count.
func (c *Correlator) markOutboxRetryable(id string, attempts int, circuitOpen bool) error {
	backoff := time.Duration(1<<uint(attempts)) * time.Minute
	if backoff > time.Hour {
		backoff = time.Hour
	}
	if backoff < time.Minute {
		backoff = time.Minute
	}
	if circuitOpen {
		backoff = circuitBreakerResetTimeout
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
		SET status = 'retryable', next_attempt_at = now() + $1, updated_at = now()
		WHERE id = $2
	`, backoff, id)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}

// applyOutboxResponseStatusStandalone maps an investigator HTTP response status
// code to the corresponding outbox status update, managed in its own transaction.
func (c *Correlator) applyOutboxResponseStatusStandalone(itemID string, resp *http.Response) error {
	var status string
	var safeErrorCode string
	recordFailure := false

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		status = "delivered"
		c.circuitBreaker.recordSuccess()
		if c.metrics != nil {
			c.metrics.OutboxDeliveredTotal.WithLabelValues("success").Inc()
		}
	case resp.StatusCode == 409:
		status = "delivered"
		safeErrorCode = "'INVESTIGATOR_CONFLICT'"
		c.circuitBreaker.recordSuccess()
		if c.metrics != nil {
			c.metrics.OutboxDeliveredTotal.WithLabelValues("conflict").Inc()
		}
	case resp.StatusCode >= 500:
		status = "retryable"
		recordFailure = true
		if c.metrics != nil {
			c.metrics.OutboxFailedTotal.WithLabelValues("server_error").Inc()
		}
	default:
		status = "failed_terminal"
		safeErrorCode = "'INVESTIGATOR_ERROR'"
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

	query := `UPDATE investigation_outbox SET status = $1, updated_at = now() WHERE id = $2`
	args := []interface{}{status, itemID}
	if safeErrorCode != "" {
		query = `UPDATE investigation_outbox SET status = $1, safe_error_code = $2, updated_at = now() WHERE id = $3`
		args = []interface{}{status, safeErrorCode, itemID}
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
	_, err = tx.ExecContext(c.ctx, `
		DELETE FROM investigation_outbox
		WHERE status = 'delivered' AND updated_at < now() - $1
	`, outboxRetention)
	if err != nil {
		return err
	}
	rollback = false
	return tx.Commit()
}
