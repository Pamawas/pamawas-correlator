package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-correlator/config"
	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
	"github.com/Pamawas/pamawas-correlator/service"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
	correlator    *service.Correlator
	cfg           config.Config
	metrics       *metrics.Metrics
	db            *sql.DB
	investigatorURL string
}

// NewHandler creates a new handler with dependencies
func NewHandler(db *sql.DB, cfg config.Config, m *metrics.Metrics, investigatorURL string) *Handler {
	// Get investigator URL from environment
	serviceToken := os.Getenv("PAMAWAS_SERVICE_TOKEN")
	correlator := service.NewCorrelator(db, cfg.TimeWindow, cfg.Interval, cfg.Mode, m, investigatorURL, serviceToken)

	return &Handler{
		correlator:    correlator,
		cfg:           cfg,
		metrics:       m,
		db:            db,
		investigatorURL: investigatorURL,
	}
}

// HealthHandler handles health check requests
func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := "healthy"
	var errors []string

	if err := h.db.PingContext(r.Context()); err != nil {
		h.metrics.DBConnectionErrors.Inc()
		log.Error().Err(err).Msg("Health check failed: database connection")
		status = "unhealthy"
		errors = append(errors, fmt.Sprintf("database: %v", err))
	}

	h.correlator.MuLock()
	lastRun := h.correlator.LastRun()
	running := h.correlator.Running()
	circuitState := h.correlator.CircuitBreakerState()
	h.correlator.MuUnlock()

	if h.investigatorURL != "" {
		if err := h.pingInvestigator(); err != nil {
			// Investigator is a separate service with its own k8s probe.
			// The correlator can still correlate events; the outbox worker
			// handles delivery retries via its circuit breaker. Log a warning
			// but don't mark the correlator itself unhealthy.
			log.Warn().Err(err).Msg("Investigator unreachable; correlator health degraded")
			errors = append(errors, fmt.Sprintf("investigator: %v", err))
		}
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(models.HealthResponse{
		Status:             status,
		Timestamp:          time.Now().UTC(),
		LastRun:            lastRun,
		Running:            running,
		TimeWindow:         h.cfg.TimeWindow.String(),
		Version:            "1.0.0",
		CircuitBreakerState: circuitState,
		Errors:             errors,
	}); err != nil {
		log.Error().Err(err).Msg("Failed to encode health response")
	}
}

// pingInvestigator checks investigator connectivity with a short timeout.
// Uses its own context (not the request's) so a slow investigator cannot
// cause cascading timeouts on the correlator's own health probe.
func (h *Handler) pingInvestigator() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", h.investigatorURL+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close response body")
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("investigator returned status %d", resp.StatusCode)
	}
	return nil
}

// ReadyHandler handles readiness check requests
func (h *Handler) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.PingContext(r.Context()); err != nil {
		log.Error().Err(err).Msg("Readiness check failed: database not ready")
		w.WriteHeader(http.StatusServiceUnavailable)
		if encodeErr := json.NewEncoder(w).Encode(models.HealthResponse{
			Status: "not ready",
			Error:  fmt.Sprintf("Database not ready: %v", err),
		}); encodeErr != nil {
			log.Error().Err(encodeErr).Msg("Failed to encode health response")
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(models.HealthResponse{Status: "ready"}); err != nil {
		log.Error().Err(err).Msg("Failed to encode health response")
	}
}

// TriggerHandler handles manual correlation trigger
func (h *Handler) TriggerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.correlator.Correlate(); err != nil {
		http.Error(w, fmt.Sprintf("Correlation failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(models.TriggerResponse{
		Message: "Correlation triggered successfully",
	}); err != nil {
		log.Error().Err(err).Msg("Failed to encode trigger response")
	}
}

// StatusHandler returns the current status of the correlator
func (h *Handler) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.correlator.MuLock()
	defer h.correlator.MuUnlock()

	if err := json.NewEncoder(w).Encode(models.StatusResponse{
		LastRun:    h.correlator.LastRun(),
		Running:    h.correlator.Running(),
		Uptime:     time.Since(h.correlator.StartTime()).String(),
		Version:    "1.0.0",
		TimeWindow: h.cfg.TimeWindow.String(),
		Interval:   h.cfg.Interval.String(),
		Mode:       h.cfg.Mode,
	}); err != nil {
		log.Error().Err(err).Msg("Failed to encode status response")
	}
}

// MetricsHandler returns the Prometheus metrics handler
func (h *Handler) MetricsHandler() http.Handler {
	return promhttp.Handler()
}
