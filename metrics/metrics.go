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

	// Outbox metrics
	OutboxDeliveredTotal   *prometheus.CounterVec
	OutboxFailedTotal      *prometheus.CounterVec
	OutboxRetryableTotal   prometheus.Counter
	OutboxPendingGauge     prometheus.Gauge
	OutboxDeliveryDuration prometheus.Histogram
}

// NewMetrics creates and registers all metrics.
//
// Names follow the pamawas_<subsystem>_<metric> convention (design review
// P3): namespace `pamawas`, subsystem `correlator` or `correlator_outbox`.
func NewMetrics() *Metrics {
	return &Metrics{
		CorrelationCyclesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "cycles_total",
				Help:      "Total number of correlation cycles",
			},
			[]string{"status"},
		),
		EventsProcessedTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "events_processed_total",
				Help:      "Total number of events processed by correlator",
			},
		),
		IncidentsCreatedTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "incidents_created_total",
				Help:      "Total number of incidents created by correlator",
			},
		),
		CycleDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "cycle_duration_seconds",
				Help:      "Correlation cycle duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
		),
		DBConnectionErrors: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "db_connection_errors_total",
				Help:      "Total number of database connection errors",
			},
		),
		LastRunTimestamp: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "last_run_timestamp_seconds",
				Help:      "Timestamp of last correlation run",
			},
		),
		CorrelatorRunning: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "correlator",
				Name:      "running",
				Help:      "Whether correlator is currently running (1) or not (0)",
			},
		),

		// Outbox metrics
		OutboxDeliveredTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator_outbox",
				Name:      "delivered_total",
				Help:      "Total number of outbox items delivered successfully",
			},
			[]string{"status"},
		),
		OutboxFailedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator_outbox",
				Name:      "failed_total",
				Help:      "Total number of outbox items that failed",
			},
			[]string{"status"},
		),
		OutboxRetryableTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pamawas",
				Subsystem: "correlator_outbox",
				Name:      "retryable_total",
				Help:      "Total number of outbox items marked retryable",
			},
		),
		OutboxPendingGauge: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pamawas",
				Subsystem: "correlator_outbox",
				Name:      "pending",
				Help:      "Number of pending outbox items",
			},
		),
		OutboxDeliveryDuration: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "pamawas",
				Subsystem: "correlator_outbox",
				Name:      "delivery_duration_seconds",
				Help:      "Duration of outbox delivery calls to investigator",
				Buckets:   prometheus.DefBuckets,
			},
		),
	}
}
