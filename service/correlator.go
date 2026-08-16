package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
)

// Correlator holds the database connection and correlation logic
type Correlator struct {
	db         *sql.DB
	timeWindow time.Duration
	interval   time.Duration
	mode       string
	mu         sync.Mutex
	lastRun    time.Time
	running    bool
	wg         sync.WaitGroup
	ctx        context.Context
	cancelFunc context.CancelFunc
	startTime  time.Time
	metrics    *metrics.Metrics
}

// NewCorrelator creates a new correlator instance
func NewCorrelator(db *sql.DB, timeWindow, interval time.Duration, mode string, m *metrics.Metrics) *Correlator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Correlator{
		db:         db,
		timeWindow: timeWindow,
		interval:   interval,
		mode:       mode,
		ctx:        ctx,
		cancelFunc: cancel,
		startTime:  time.Now(),
		metrics:    m,
	}
}

// MuLock locks the correlator mutex
func (c *Correlator) MuLock() {
	c.mu.Lock()
}

// MuUnlock unlocks the correlator mutex
func (c *Correlator) MuUnlock() {
	c.mu.Unlock()
}

// LastRun returns the last run time
func (c *Correlator) LastRun() time.Time {
	return c.lastRun
}

// Running returns whether the correlator is running
func (c *Correlator) Running() bool {
	return c.running
}

// StartTime returns the start time
func (c *Correlator) StartTime() time.Time {
	return c.startTime
}

// Correlate performs one correlation cycle
func (c *Correlator) Correlate() error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil // already running
	}
	c.running = true
	c.metrics.CorrelatorRunning.Set(1)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.metrics.CorrelatorRunning.Set(0)
		c.mu.Unlock()
	}()

	c.wg.Add(1)
	defer c.wg.Done()

	startTime := time.Now()
	log.Info().Msg("Starting correlation cycle")

	// Get uncorrelated events from the database
	events, err := c.getUncorrelatedEvents()
	if err != nil {
		c.metrics.CorrelationCyclesTotal.WithLabelValues("error").Inc()
		return err
	}
	if len(events) == 0 {
		log.Info().Msg("No uncorrelated events found")
		c.metrics.CorrelationCyclesTotal.WithLabelValues("success").Inc()
		return nil
	}

	log.Info().Int("count", len(events)).Msg("Found uncorrelated events")
	c.metrics.EventsProcessedTotal.Add(float64(len(events)))

	// Group events into incidents
	incidents := c.groupEventsIntoIncidents(events)
	log.Info().Int("count", len(incidents)).Msg("Correlated into incidents")
	c.metrics.IncidentsCreatedTotal.Add(float64(len(incidents)))

	// Save incidents to database
	if err := c.saveIncidents(incidents); err != nil {
		c.metrics.CorrelationCyclesTotal.WithLabelValues("error").Inc()
		return err
	}

	// Mark events as correlated
	c.markEventsCorrelated(events)

	c.mu.Lock()
	c.lastRun = time.Now()
	c.metrics.LastRunTimestamp.Set(float64(time.Now().Unix()))
	c.mu.Unlock()

	c.metrics.CycleDuration.Observe(time.Since(startTime).Seconds())
	c.metrics.CorrelationCyclesTotal.WithLabelValues("success").Inc()

	log.Info().Dur("duration", time.Since(startTime)).Msg("Correlation cycle completed")
	return nil
}

// getUncorrelatedEvents fetches events that haven't been correlated yet
func (c *Correlator) getUncorrelatedEvents() ([]models.Event, error) {
	query := `
		SELECT e.id, e.source, e.type, e.timestamp, e.service, e.environment, 
		       e.severity, e.title, e.status, e.labels
		FROM events e
		LEFT JOIN incident_events ie ON e.id = ie.event_id
		WHERE ie.event_id IS NULL
		ORDER BY e.timestamp ASC
	`

	rows, err := c.db.QueryContext(c.ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close rows")
		}
	}()

	var events []models.Event
	for rows.Next() {
		var e models.Event
		var labelsJSON []byte
		if err := rows.Scan(
			&e.ID, &e.Source, &e.Type, &e.Timestamp, &e.Service, &e.Environment,
			&e.Severity, &e.Title, &e.Status, &labelsJSON,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(labelsJSON, &e.Labels); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// groupEventsIntoIncidents groups events using time window and service/label overlap
func (c *Correlator) groupEventsIntoIncidents(events []models.Event) []models.Incident {
	if len(events) == 0 {
		return []models.Incident{}
	}

	// Sort events by timestamp (should already be sorted from query)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	var incidents []models.Incident
	var currentGroup []models.Event

	for _, event := range events {
		if len(currentGroup) == 0 {
			currentGroup = append(currentGroup, event)
			continue
		}

		// Check if event belongs to current group
		lastEvent := currentGroup[len(currentGroup)-1]
		if c.eventsBelongTogether(lastEvent, event) {
			currentGroup = append(currentGroup, event)
		} else {
			// Finalize current group and start new one
			if len(currentGroup) > 0 {
				incident := c.createIncidentFromGroup(currentGroup)
				incidents = append(incidents, incident)
			}
			currentGroup = []models.Event{event}
		}
	}

	// Don't forget the last group
	if len(currentGroup) > 0 {
		incident := c.createIncidentFromGroup(currentGroup)
		incidents = append(incidents, incident)
	}

	return incidents
}

// eventsBelongTogether checks if two events should be in the same incident
func (c *Correlator) eventsBelongTogether(e1, e2 models.Event) bool {
	// Check time window
	if e2.Timestamp.Sub(e1.Timestamp) > c.timeWindow {
		return false
	}

	// Check service overlap (if both have service)
	if e1.Service != "" && e2.Service != "" && e1.Service == e2.Service {
		return true
	}

	// Check label overlap
	for k, v := range e1.Labels {
		if v2, exists := e2.Labels[k]; exists && v == v2 {
			return true
		}
	}

	return false
}

// createIncidentFromGroup creates an Incident from a group of events
func (c *Correlator) createIncidentFromGroup(group []models.Event) models.Incident {
	if len(group) == 0 {
		return models.Incident{}
	}

	// Determine incident properties from the group
	var services []string
	serviceSet := make(map[string]bool)
	var titles []string
	var severities []string
	var statuses []string
	var minTime, maxTime time.Time

	for i, event := range group {
		if i == 0 {
			minTime = event.Timestamp
			maxTime = event.Timestamp
		} else {
			if event.Timestamp.Before(minTime) {
				minTime = event.Timestamp
			}
			if event.Timestamp.After(maxTime) {
				maxTime = event.Timestamp
			}
		}

		if event.Service != "" && !serviceSet[event.Service] {
			serviceSet[event.Service] = true
			services = append(services, event.Service)
		}
		if event.Title != "" {
			titles = append(titles, event.Title)
		}
		if event.Severity != "" {
			severities = append(severities, event.Severity)
		}
		if event.Status != "" {
			statuses = append(statuses, event.Status)
		}
	}

	// Determine incident title (most common or first)
	title := "Infrastructure Incident"
	if len(titles) > 0 {
		title = titles[0] // simple: use first title
	}

	// Determine severity (highest)
	severity := "info"
	severityOrder := map[string]int{"critical": 5, "high": 4, "warning": 3, "info": 2, "debug": 1}
	maxSeverity := 0
	for _, s := range severities {
		if val, ok := severityOrder[s]; ok && val > maxSeverity {
			maxSeverity = val
			severity = s
		}
	}

	// Determine status (most severe or latest)
	status := "firing"
	statusOrder := map[string]int{"resolved": 0, "firing": 1}
	maxStatus := 0
	for _, s := range statuses {
		if val, ok := statusOrder[s]; ok && val > maxStatus {
			maxStatus = val
			status = s
		}
	}

	return models.Incident{
		ID:           uuid.NewString(),
		Title:        title,
		Status:       status,
		StartedAt:    minTime,
		ResolvedAt:   maxTime,
		Severity:     severity,
		AffectedServices: services,
		EventIDs:     c.extractEventIDs(group),
	}
}

// extractEventIDs extracts IDs from a group of events
func (c *Correlator) extractEventIDs(group []models.Event) []string {
	ids := make([]string, len(group))
	for i, event := range group {
		ids[i] = event.ID
	}
	return ids
}

// saveIncidents inserts incidents into the database
func (c *Correlator) saveIncidents(incidents []models.Incident) error {
	if len(incidents) == 0 {
		return nil
	}

	tx, err := c.db.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error().Err(rbErr).Msg("Failed to rollback transaction")
		}
	}()

	for _, incident := range incidents {
		_, err := tx.ExecContext(c.ctx,
			"INSERT INTO incidents (id, title, status, started_at, resolved_at, severity, affected_services) VALUES ($1,$2,$3,$4,$5,$6,$7)",
			incident.ID, incident.Title, incident.Status, incident.StartedAt, incident.ResolvedAt, incident.Severity, incident.AffectedServices,
		)
		if err != nil {
			return err
		}

		// Link events to incident
		for _, eventID := range incident.EventIDs {
			_, err := tx.ExecContext(c.ctx,
				"INSERT INTO incident_events (incident_id, event_id) VALUES ($1,$2)",
				incident.ID, eventID,
			)
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// markEventsCorrelated marks events as correlated by ensuring they appear in incident_events
func (c *Correlator) markEventsCorrelated(events []models.Event) {
	if len(events) == 0 {
		return
	}

	// The saveIncidents function already creates the incident_events links
	// This is a simplified approach - in reality we'd need to track which events belong to which incidents
	// But since saveIncidents already does the linking, we can skip this step
}

// StartWorker starts the background correlation worker
func (c *Correlator) StartWorker() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Info().Msg("Correlation worker stopped")
			return
		case <-ticker.C:
			if err := c.Correlate(); err != nil {
				log.Error().Err(err).Msg("Correlation error")
			}
		}
	}
}

// Stop stops the correlator gracefully
func (c *Correlator) Stop() {
	c.cancelFunc()
	c.wg.Wait()
}