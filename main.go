package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	wg          sync.WaitGroup
	ctx         context.Context
	cancelFunc  context.CancelFunc
}

// HealthStatus represents the health of the correlator
type HealthStatus struct {
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
	LastRun     time.Time `json:"last_run"`
	Running     bool      `json:"running"`
	TimeWindow  string    `json:"time_window"`
	Uptime      string    `json:"uptime,omitempty"`
	Version     string    `json:"version,omitempty"`
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

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Correlator{
		db:       db,
		timeWindow: timeWindow,
		ctx:      ctx,
	}

	// HTTP server for health checks and manual triggers
	r := http.NewServeMux()
	r.HandleFunc("/healthz", c.healthHandler)
	r.HandleFunc("/ready", c.readyHandler)
	r.HandleFunc("/trigger", c.triggerHandler)
	r.HandleFunc("/status", c.statusHandler)
	r.HandleFunc("/metrics", c.metricsHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Starting correlator on :%s with time window %s", port, timeWindow)

	// Start background correlation worker if enabled
	if os.Getenv("CORRELATOR_MODE") != "manual" {
		go c.correlationWorker()
	}

	// Start HTTP server
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Listen for shutdown signals
	go func() {
		<-ctx.Done()
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("Server forced to shutdown: %v", err)
		}
	}()

	log.Fatal(srv.ListenAndServe())
}

// correlationWorker runs the correlation process periodically
func (c *Correlator) correlationWorker() {
	intervalStr := os.Getenv("CORRELATION_INTERVAL")
	if intervalStr == "" {
		intervalStr = "1m" // default 1 minute
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		log.Fatalf("Invalid CORRELATION_INTERVAL: %v", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Correlation worker stopped")
			return
		case <-ticker.C:
			if err := c.Correlate(); err != nil {
				log.Printf("Correlation error: %v", err)
				// Continue despite errors - don't want to stop the worker
			}
		}
	}
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

	c.wg.Add(1)
	defer c.wg.Done()

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
		EventIDs:     c.extractEventIDs(group),
	}
}

// extractEventIDs extracts IDs from a group of events
func (c *Correlator) extractEventIDs(group []Event) []string {
	ids := make([]string, len(group))
	for i, event := range group {
		ids[i] = event.ID
	}
	return ids
}

// saveIncidents inserts incidents into the database
func (c *Correlator) saveIncidents(incidents []Incident) error {
	if len(incidents) == 0 {
		return nil
	}

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
func (c *Correlator) markEventsCorrelated(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Build a placeholder query - in practice, we'd use the IDs from the incidents we just created
	// For now, we'll rely on the fact that saveIncients already created the incident_events links
	// This is a simplified approach - in reality we'd need to track which events belong to which incidents
	// But since saveIncidents already does the linking, we can skip this step
	return nil
}

// Health check handlers
func (c *Correlator) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check database connectivity
	if err := c.db.PingContext(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "unhealthy",
			"error":  fmt.Sprintf("Database connection failed: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthStatus{
		Status:      "healthy",
		Timestamp:   time.Now().UTC(),
		LastRun:     c.lastRun,
		Running:     c.running,
		TimeWindow:  c.timeWindow.String(),
		Version:     "1.0.0",
	})
}

func (c *Correlator) readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple readiness check - if we can connect to db, we're ready
	if err := c.db.PingContext(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"error":  fmt.Sprintf("Database not ready: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (c *Correlator) triggerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := c.Correlate(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "correlation triggered successfully",
	})
}

func (c *Correlator) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"last_run": c.lastRun,
		"running":  c.running,
		"time_window": c.timeWindow.String(),
		"version": "1.0.0",
	})
}

func (c *Correlator) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple metrics - in production you'd use prometheus client library
	fmt.Fprintf(w, `# HELP pamawas_correlator_last_run_timestamp_seconds Timestamp of last correlation run
# TYPE pamawas_correlator_last_run_timestamp_seconds gauge
pamawas_correlator_last_run_timestamp_seconds %d
`,
		c.lastRun.Unix())

	fmt.Fprintf(w, `# HELP pamawas_correlator_running Whether the correlator is currently running
# TYPE pamawas_correlator_running gauge
pamawas_correlator_running %d
`,
		boolToInt(c.running))

	fmt.Fprintf(w, `# HELP pamawas_correlator_time_window_seconds Correlation time window in seconds
# TYPE pamawas_correlator_time_window_seconds gauge
pamawas_correlator_time_window_seconds %.2f
`,
		c.timeWindow.Seconds())
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
