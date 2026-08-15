package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
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
	correlator *service.Correlator
	cfg        config.Config
	metrics    *metrics.Metrics
	db         *sql.DB
}

// NewHandler creates a new handler with dependencies
func NewHandler(db *sql.DB, cfg config.Config, m *metrics.Metrics) *Handler {
	correlator := service.NewCorrelator(db, cfg.TimeWindow, cfg.Interval, cfg.Mode, m)

	return &Handler{
		correlator: correlator,
		cfg:        cfg,
		metrics:    m,
		db:         db,
	}
}

// HealthHandler handles health check requests
func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.PingContext(r.Context()); err != nil {
		h.metrics.DBConnectionErrors.Inc()
		log.Error().Err(err).Msg("Health check failed: database connection")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(models.HealthResponse{
			Status: "unhealthy",
			Error:  fmt.Sprintf("Database connection failed: %v", err),
		})
		return
	}

	h.correlator.MuLock()
	lastRun := h.correlator.LastRun()
	running := h.correlator.Running()
	h.correlator.MuUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.HealthResponse{
		Status:      "healthy",
		Timestamp:   time.Now().UTC(),
		LastRun:     lastRun,
		Running:     running,
		TimeWindow:  h.cfg.TimeWindow.String(),
		Version:     "1.0.0",
	})
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
		json.NewEncoder(w).Encode(models.HealthResponse{
			Status: "not ready",
			Error:  fmt.Sprintf("Database not ready: %v", err),
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.HealthResponse{Status: "ready"})
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
	json.NewEncoder(w).Encode(models.TriggerResponse{
		Message: "Correlation triggered successfully",
	})
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

	json.NewEncoder(w).Encode(models.StatusResponse{
		LastRun:    h.correlator.LastRun(),
		Running:    h.correlator.Running(),
		Uptime:     time.Since(h.correlator.StartTime()).String(),
		Version:    "1.0.0",
		TimeWindow: h.cfg.TimeWindow.String(),
		Interval:   h.cfg.Interval.String(),
		Mode:       h.cfg.Mode,
	})
}

// MetricsHandler returns the Prometheus metrics handler
func (h *Handler) MetricsHandler() http.Handler {
	return promhttp.Handler()
}