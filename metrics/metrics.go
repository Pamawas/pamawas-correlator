package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the correlator service
type Metrics struct {
	CorrelationCyclesTotal *prometheus.CounterVec
	EventsProcessedTotal   prometheus.Counter
	IncidentsCreatedTotal  prometheus.Counter
	CycleDuration          prometheus.Histogram
	DBConnectionErrors     prometheus.Counter
	LastRunTimestamp       prometheus.Gauge
	CorrelatorRunning      prometheus.Gauge
}

// NewMetrics creates and registers all metrics
func NewMetrics() *Metrics {
	return &Metrics{
		CorrelationCyclesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "correlator_cycles_total",
				Help: "Total number of correlation cycles",
			},
			[]string{"status"},
		),
		EventsProcessedTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "correlator_events_processed_total",
				Help: "Total number of events processed by correlator",
			},
		),
		IncidentsCreatedTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "correlator_incidents_created_total",
				Help: "Total number of incidents created by correlator",
			},
		),
		CycleDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "correlator_cycle_duration_seconds",
				Help:    "Correlation cycle duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
		),
		DBConnectionErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "correlator_db_connection_errors_total",
				Help: "Total number of database connection errors",
			},
		),
		LastRunTimestamp: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "correlator_last_run_timestamp_seconds",
				Help: "Timestamp of last correlation run",
			},
		),
		CorrelatorRunning: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "correlator_running",
				Help: "Whether correlator is currently running (1) or not (0)",
			},
		),
	}
}