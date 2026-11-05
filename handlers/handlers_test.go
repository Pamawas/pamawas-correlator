package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Pamawas/pamawas-correlator/config"
	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/models"
)

func testMetrics() *metrics.Metrics {
	return &metrics.Metrics{
		DBConnectionErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_db_errors_total"}),
		CorrelatorRunning:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_running"}),
		LastRunTimestamp:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_last_run"}),
	}
}

func newTestHandler(t *testing.T) (*Handler, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	cfg := config.Config{
		TimeWindow: 10 * time.Minute,
		Interval:   1 * time.Minute,
		Mode:       "auto",
	}
	return NewHandler(db, cfg, testMetrics()), mock
}

func TestHealthHandler_Healthy(t *testing.T) {
	h, mock := newTestHandler(t)
	mock.ExpectPing()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	h.HealthHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var resp models.HealthResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected status 'healthy', got %q", resp.Status)
	}
}

func TestStatusHandler_Healthy(t *testing.T) {
	h, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rr := httptest.NewRecorder()

	h.StatusHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var resp models.StatusResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Mode != "auto" {
		t.Errorf("expected mode 'auto', got %q", resp.Mode)
	}
}

func TestHandlers_MethodNotAllowed(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		path    string
	}{
		{"healthz", h.HealthHandler, "/healthz"},
		{"ready", h.ReadyHandler, "/ready"},
		{"status", h.StatusHandler, "/status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			rr := httptest.NewRecorder()
			tc.handler(rr, req)
			if rr.Code != http.StatusMethodNotAllowed {
				t.Errorf("expected 405 MethodNotAllowed, got %d", rr.Code)
			}
		})
	}
}

