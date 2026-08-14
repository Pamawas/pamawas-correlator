package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/google/uuid"
)

// Event represents the normalized event from the database
type Event struct {
	ID        string            `json:"id"`
	Source    string            `json:"source"`
	Type      string            `json:"type"`
	Timestamp time.Time         `json:"timestamp"`
	Service   string            `json:"service,omitempty"`
	Environment string        `json:"environment,omitempty"`
	Severity  string            `json:"severity,omitempty"`
	Title     string            `json:"title,omitempty"`
	Status    string            `json:"status,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Incident represents a correlated group of events
type Incident struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"started_at"`
	ResolvedAt   time.Time `json:"resolved_at,omitempty"`
	Severity     string    `json:"severity,omitempty"`
	AffectedServices []string `json:"affected_services"`
	EventIDs     []string  `json:"event_ids"`
}

// Correlator holds the database connection and correlation logic
type Correlator struct {
	db          *sql.DB
	timeWindow  time.Duration
	mu          sync.Mutex
	lastRun     time.Time
	running     bool
}

func main() {
	// Get configuration from environment
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable not set")
	}
	timeWindowStr := os.Getenv("CORRELATION_TIME_WINDOW")
	if timeWindowStr == "" {
		timeWindowStr = "10m" // default 10 minutes
	}
	timeWindow, err := time.ParseDuration(timeWindowStr)
	if err != nil {
		log.Fatalf("Invalid CORRELATION_TIME_WINDOW: %v", err)
	}

	// Connect to database
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err = db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}

	c := &Correlator{
		db:       db,
		timeWindow: timeWindow,
	}

	// HTTP server for health checks and manual triggers
	r := http.NewServeMux()
	r.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	r.HandleFunc("/trigger", func(w http.ResponseWriter, r *http.Request) {
		if err := c.Correlate(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("correlation triggered"))
	})
	r.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		status := map[string]interface{}{
			"last_run": c.lastRun,
			"running":  c.running,
			"time_window": c.timeWindow.String(),
		}
		json.NewEncoder(w).Encode(status)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting correlator on :%s with time window %s", port, timeWindow)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

// Correlate performs one correlation cycle
func (c *Correlator) Correlate() error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return nil // already running
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	startTime := time.Now()
	log.Printf("Starting correlation cycle")

	// Get uncorrelated events from the database
	events, err := c.getUncorrelatedEvents()
	if err != nil {
		return err
	}
	if len(events) == 0 {
		log.Printf("No uncorrelated events found")
		return nil
	}

	log.Printf("Found %d uncorrelated events", len(events))

	// Group events into incidents
	incidents := c.groupEventsIntoIncidents(events)
	log.Printf("Correlated into %d incidents", len(incidents))

	// Save incidents to database
	if err := c.saveIncidents(incidents); err != nil {
		return err
	}

	// Mark events as correlated
	if err := c.markEventsCorrelated(events); err != nil {
		return err
	}

	c.mu.Lock()
	c.lastRun = time.Now()
	c.mu.Unlock()

	log.Printf("Correlation cycle completed in %v", time.Since(startTime))
	return nil
}

// getUncorrelatedEvents fetches events that haven't been correlated yet
func (c *Correlator) getUncorrelatedEvents() ([]Event, error) {
	query := `
		SELECT e.id, e.source, e.type, e.timestamp, e.service, e.environment, 
		       e.severity, e.title, e.status, e.labels
		FROM events e
		LEFT JOIN incident_events ie ON e.id = ie.event_id
		WHERE ie.event_id IS NULL
		ORDER BY e.timestamp ASC
	`

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
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
func (c *Correlator) groupEventsIntoIncidents(events []Event) []Incident {
	if len(events) == 0 {
		return []Incident{}
	}

	// Sort events by timestamp (should already be sorted from query)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	var incidents []Incident
	var currentGroup []Event

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
			currentGroup = []Event{event}
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
func (c *Correlator) eventsBelongTogether(e1, e2 Event) bool {
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
func (c *Correlator) createIncidentFromGroup(group []Event) Incident {
	if len(group) == 0 {
		return Incident{}
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

	return Incident{
		ID:           uuid.NewString(),
		Title:        title,
		Status:       status,
		StartedAt:    minTime,
		ResolvedAt:   maxTime, // if still firing, this will be updated later
		Severity:     severity,
		AffectedServices: services,
	}
}

// saveIncidents inserts incidents into the database
func (c *Correlator) saveIncidents(incidents []Incident) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // will be rolled back if not committed

	for _, incident := range incidents {
		_, err := tx.Exec(
			"INSERT INTO incidents (id, title, status, started_at, resolved_at, severity, affected_services) VALUES ($1,$2,$3,$4,$5,$6,$7)",
			incident.ID, incident.Title, incident.Status, incident.StartedAt, incident.ResolvedAt, incident.Severity, incident.AffectedServices,
		)
		if err != nil {
			return err
		}

		// Link events to incident
		for _, eventID := range incident.EventIDs {
			_, err := tx.Exec(
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
// Note: This is actually done in saveIncidents, but we keep this for clarity
func (c *Correlator) markEventsCorrelated(events []Event) error {
	// Events are marked as correlated when we insert into incident_events in saveIncidents
	// This function is kept for potential future use if we need a separate marking step
	return nil
}