package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
)

const (
	correlationPolicy  = "pamawas-correlation-v1"
	correlationVersion = 1
)

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

var identityLabelScores = map[string]int{"alert_rule": 40, "namespace": 20, "workload": 20, "instance": 20}

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
	httpClient      *http.Client
	outboxWg        sync.WaitGroup
}

func NewCorrelator(db *sql.DB, timeWindow, interval time.Duration, mode string, m *metrics.Metrics, investigatorURL string) *Correlator {
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
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Correlator) MuLock()              { c.mu.Lock() }
func (c *Correlator) MuUnlock()            { c.mu.Unlock() }
func (c *Correlator) LastRun() time.Time   { c.mu.Lock(); defer c.mu.Unlock(); return c.lastRun }
func (c *Correlator) Running() bool        { c.mu.Lock(); defer c.mu.Unlock(); return c.running }
func (c *Correlator) StartTime() time.Time { return c.startTime }

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
	start := c.now()
	processed := 0
	created := 0
	for {
		didProcess, didCreate, err := c.processNextEvent(c.ctx)
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
			_ = tx.Rollback()
		}
	}()
	var event models.Event
	var labels []byte
	row := tx.QueryRowContext(ctx, `SELECT e.id, e.source, e.source_event_id, e.type, e.occurred_at, e.service, e.environment, e.severity, e.title, e.status, e.labels FROM events e LEFT JOIN incident_events ie ON ie.event_id=e.id AND ie.policy_version=$1 WHERE ie.event_id IS NULL ORDER BY e.occurred_at, e.id FOR UPDATE SKIP LOCKED LIMIT 1`, correlationVersion)
	scanErr := row.Scan(&event.ID, &event.Source, &event.SourceEventID, &event.Type, &event.Timestamp, &event.Service, &event.Environment, &event.Severity, &event.Title, &event.Status, &labels)
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
	unmarshalErr := json.Unmarshal(labels, &event.Labels)
	if unmarshalErr != nil {
		return false, false, unmarshalErr
	}
	rows, queryErr := tx.QueryContext(ctx, `SELECT i.id, i.status, i.started_at, i.last_event_at, i.resolved_at, i.severity, i.environment, i.affected_services, i.created_at, COALESCE((SELECT e2.service FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.source FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.source_event_id FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), ''), COALESCE((SELECT e2.labels FROM incident_events ie2 JOIN events e2 ON e2.id=ie2.event_id WHERE ie2.incident_id=i.id ORDER BY e2.occurred_at DESC LIMIT 1), '{}'::jsonb) FROM incidents i WHERE i.environment=$1 AND i.status IN ('open','investigating') AND i.last_event_at >= $2 AND i.last_event_at <= $3 AND i.started_at >= $4 ORDER BY i.last_event_at DESC, i.created_at, i.id FOR UPDATE OF i SKIP LOCKED`, event.Environment, event.Timestamp.Add(-15*time.Minute), event.Timestamp.Add(5*time.Minute), event.Timestamp.Add(-6*time.Hour))
	if queryErr != nil {
		return false, false, queryErr
	}
	var candidates []incidentCandidate
	for rows.Next() {
		var x incidentCandidate
		var services pq.StringArray
		var lb []byte
		scanErr := rows.Scan(&x.ID, &x.Status, &x.StartedAt, &x.LastEventAt, new(sql.NullTime), &x.Severity, &x.Environment, &services, &x.CreatedAt, &x.Service, &x.Source, &x.SourceEventID, &lb)
		if scanErr != nil {
			closeErr := rows.Close()
			if closeErr != nil {
				return false, false, closeErr
			}
			return false, false, scanErr
		}
		x.Labels = map[string]string{}
		unmarshalErr := json.Unmarshal(lb, &x.Labels)
		if unmarshalErr != nil {
			closeErr := rows.Close()
			if closeErr != nil {
				return false, false, closeErr
			}
			return false, false, unmarshalErr
		}
		candidates = append(candidates, x)
	}
	closeErr := rows.Close()
	if closeErr != nil {
		return false, false, closeErr
	}
	rowsErr := rows.Err()
	if rowsErr != nil {
		return false, false, rowsErr
	}
	candidate, _, matched := selectCandidate(event, candidates)
	incidentID := candidate.ID
	now := c.now()
	resolution := event.Status == "resolved" && matched && acceptedResolution(event, candidate)
	reason := "new_incident"
	if matched {
		reason = "same_service_time_window"
		if resolution {
			reason = "resolution_of_open_incident"
		}
	}
	var execErr error
	if !matched {
		incidentID = c.newIncidentID(event.Timestamp)
		_, execErr = tx.ExecContext(ctx, `INSERT INTO incidents (id,title,status,started_at,last_event_at,resolved_at,severity,environment,affected_services,correlation_policy,correlation_version,created_at,updated_at) VALUES ($1,$2,'open',$3,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, incidentID, event.Title, event.Timestamp, nil, event.Severity, event.Environment, pq.Array(nonEmpty(event.Service)), correlationPolicy, correlationVersion, now, now)
		created = true
	} else {
		status := candidate.Status
		if resolution {
			status = "resolved"
		}
		_, execErr = tx.ExecContext(ctx, `UPDATE incidents SET title=$1,status=$2,last_event_at=GREATEST(last_event_at,$3),resolved_at=CASE WHEN $2='resolved' THEN GREATEST(COALESCE(resolved_at,$3),$3) ELSE resolved_at END,severity=$4,affected_services=$5,updated_at=$6 WHERE id=$7`, event.Title, status, event.Timestamp, event.Severity, pq.Array(nonEmpty(event.Service)), now, incidentID)
	}
	if execErr != nil {
		return false, false, execErr
	}
	_, execErr = tx.ExecContext(ctx, `INSERT INTO incident_events (incident_id,event_id,correlation_reason,policy_version,attached_at) VALUES ($1,$2,$3,$4,$5)`, incidentID, event.ID, reason, correlationVersion, now)
	if execErr != nil {
		return false, false, execErr
	}
	key := investigationRequestKey(incidentID, event.ID)
	_, execErr = tx.ExecContext(ctx, `INSERT INTO investigation_outbox (id,incident_id,contract_version,request_key_hash,status,attempts,next_attempt_at,created_at,updated_at) VALUES ($1,$2,$3,$4,'pending',0,$5,$5,$5) ON CONFLICT (request_key_hash) DO NOTHING`, outboxID(key), incidentID, correlationVersion, key, now)
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

func (c *Correlator) StartOutboxWorker() {
	if c.investigatorURL == "" {
		log.Info().Msg("Investigator URL not configured, skipping outbox worker")
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.processOutbox(); err != nil {
				log.Error().Err(err).Msg("Outbox processing error")
			}
		}
	}
}

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

func (c *Correlator) processOutbox() error {
	const batchSize = 10
	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(c.ctx, `
		SELECT id, incident_id, contract_version, request_key_hash
		FROM investigation_outbox
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	type outboxItem struct {
		id              string
		incidentID      string
		contractVersion int
		requestKeyHash  string
	}

	var items []outboxItem
	for rows.Next() {
		var item outboxItem
		if err := rows.Scan(&item.id, &item.incidentID, &item.contractVersion, &item.requestKeyHash); err != nil {
			return err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(items) == 0 {
		if err := tx.Commit(); err != nil {
			return err
		}
		rollback = false
		return nil
	}

	for _, item := range items {
		// Lease the outbox item
		_, err := tx.ExecContext(c.ctx, `
			UPDATE investigation_outbox
			SET status = 'leased', lease_expires_at = now() + interval '5 minutes', attempts = attempts + 1
			WHERE id = $1
		`, item.id)
		if err != nil {
			return err
		}

		// Call investigator API
		reqBody := map[string]interface{}{
			"contract_version":    item.contractVersion,
			"incident_id":         item.incidentID,
			"reason":              "incident_created",
			"correlation_version": item.contractVersion,
		}
		jsonBody, _ := json.Marshal(reqBody)

		req, err := http.NewRequestWithContext(c.ctx, "POST", c.investigatorURL+"/v1/investigations", bytes.NewReader(jsonBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Mark as retryable
			_, _ = tx.ExecContext(c.ctx, `
				UPDATE investigation_outbox
				SET status = 'retryable', next_attempt_at = now() + interval '1 minute'
				WHERE id = $1
			`, item.id)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Success - mark delivered
			_, err = tx.ExecContext(c.ctx, `
				UPDATE investigation_outbox
				SET status = 'delivered', updated_at = now()
				WHERE id = $1
			`, item.id)
			if err != nil {
				return err
			}
		} else if resp.StatusCode == 409 {
			// Conflict - already processed, mark delivered
			_, err = tx.ExecContext(c.ctx, `
				UPDATE investigation_outbox
				SET status = 'delivered', updated_at = now()
				WHERE id = $1
			`, item.id)
			if err != nil {
				return err
			}
		} else if resp.StatusCode >= 500 {
			// Server error - retryable
			_, err = tx.ExecContext(c.ctx, `
				UPDATE investigation_outbox
				SET status = 'retryable', next_attempt_at = now() + interval '1 minute'
				WHERE id = $1
			`, item.id)
			if err != nil {
				return err
			}
		} else {
			// Client error - terminal failure
			_, err = tx.ExecContext(c.ctx, `
				UPDATE investigation_outbox
				SET status = 'failed_terminal', safe_error_code = 'INVESTIGATOR_ERROR', updated_at = now()
				WHERE id = $1
			`, item.id)
			if err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	rollback = false
	return nil
}

func (c *Correlator) Stop() {
	c.cancelFunc()
	c.wg.Wait()
}
