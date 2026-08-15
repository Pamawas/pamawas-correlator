package models

import (
	"time"
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

// HealthResponse represents the health check response
type HealthResponse struct {
	Status      string    `json:"status"`
	Timestamp   time.Time `json:"timestamp"`
	LastRun     time.Time `json:"last_run"`
	Running     bool      `json:"running"`
	TimeWindow  string    `json:"time_window"`
	Uptime      string    `json:"uptime,omitempty"`
	Version     string    `json:"version,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// StatusResponse represents the status response
type StatusResponse struct {
	LastRun     time.Time `json:"last_run"`
	Running     bool      `json:"running"`
	Uptime      string    `json:"uptime"`
	Version     string    `json:"version"`
	TimeWindow  string    `json:"time_window"`
	Interval    string    `json:"interval"`
	Mode        string    `json:"mode"`
}

// TriggerResponse represents the manual trigger response
type TriggerResponse struct {
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
}