package main

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Pamawas/pamawas-correlator/config"
	"github.com/Pamawas/pamawas-correlator/handlers"
	"github.com/Pamawas/pamawas-correlator/metrics"
	"github.com/Pamawas/pamawas-correlator/middleware"
	"github.com/Pamawas/pamawas-correlator/otel"
	"github.com/Pamawas/pamawas-correlator/service"
)

func main() {
	cfg := config.Load()
	initLogger(cfg)
	printStartupBanner(loadVersion(), cfg.Port)

	// Initialize OpenTelemetry tracing
	otelShutdown, err := otel.InitTracer(otel.Config{
		ServiceName:  "pamawas-correlator",
		OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Insecure:     true,
		SampleRatio:  1.0,
		Enabled:      os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize OpenTelemetry")
	}
	defer func() {
		if shutdownErr := otelShutdown(context.Background()); shutdownErr != nil {
			log.Error().Err(shutdownErr).Msg("Error shutting down OpenTelemetry")
		}
	}()

	log.Info().
		Str("port", cfg.Port).
		Str("environment", cfg.Environment).
		Str("log_level", cfg.LogLevel).
		Str("time_window", cfg.TimeWindow.String()).
		Str("interval", cfg.Interval.String()).
		Str("flapping_cooldown_window", cfg.FlappingCooldownWindow.String()).
		Str("mode", cfg.Mode).
		Msg("Starting pamawas-correlator")

	// Connect to database with retries
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatal().Err(err).Msg("Error opening database")
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close database connection")
		}
	}()

	// Tune the connection pool so the correlator cannot exhaust Postgres connections
	// under load or during bursty outbox processing.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 30; i++ {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		log.Printf("Waiting for database... (%d/30)", i+1)
		time.Sleep(1 * time.Second)
	}

	if err := db.PingContext(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database after retries")
	}

	log.Info().Msg("Connected to database")

	// Initialize metrics
	m := metrics.NewMetrics()

	// Initialize correlator service
	investigatorURL := os.Getenv("INVESTIGATOR_URL")
	serviceToken := os.Getenv("PAMAWAS_SERVICE_TOKEN")
	correlator := service.NewCorrelator(db, service.CorrelatorConfig{
		TimeWindow:              cfg.TimeWindow,
		Interval:                cfg.Interval,
		Mode:                    cfg.Mode,
		Metrics:                 m,
		InvestigatorURL:         investigatorURL,
		ServiceToken:            serviceToken,
		CircuitBreakerThreshold: cfg.CircuitBreakerThreshold,
		CircuitBreakerReset:     cfg.CircuitBreakerResetTimeout,
		OutboxMaxAttempts:       cfg.OutboxMaxAttempts,
		FlappingCooldownWindow:  cfg.FlappingCooldownWindow,
	})

	// Initialize handlers
	h := handlers.NewHandler(db, cfg, m, investigatorURL)

	// Start background worker if not in manual mode
	if cfg.Mode != "manual" {
		go correlator.StartWorker()
		go correlator.StartOutboxWorker()
	}

	// Create router
	r := http.NewServeMux()

	// Health endpoints at root (for k8s probes)
	r.HandleFunc("/healthz", h.HealthHandler)
	r.HandleFunc("/ready", h.ReadyHandler)

	// V1 API endpoints under /api/v1
	r.HandleFunc("/api/v1/trigger", h.TriggerHandler)
	r.HandleFunc("/api/v1/status", h.StatusHandler)
	registerMetricsRoutes(r, h.MetricsHandler())

	// Wrap router with middleware
	var handler http.Handler = r
	handler = middleware.RateLimitMiddleware("pamawas-correlator", map[string]middleware.RouteLimit{
		"/api/v1/trigger": {RPS: cfg.RateLimitRPS, Burst: cfg.RateLimitBurst},
		"/api/v1/status":  {RPS: cfg.RateLimitRPS, Burst: cfg.RateLimitBurst},
	}, handler)
	handler = middleware.LoggingMiddleware("pamawas-correlator", handler)
	handler = middleware.ErrorLoggingMiddleware("pamawas-correlator", handler)
	// Tracing is outermost so every request gets a span, and so the logging
	// middleware below it can log the real trace and span IDs.
	handler = middleware.TracingMiddleware("pamawas-correlator", handler)
	// Service-to-service auth. Applied outermost of the loggers so a rejected
	// request is still logged. /api/v1/trigger runs a correlation cycle on
	// demand, so an unauthenticated caller could force that work at will.
	handler = middleware.ServiceTokenAuth(cfg.ServiceToken, middleware.IsProbeOrMetricsPath)(handler)

	// Create server
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Info().Msg("Shutdown signal received, stopping server...")
		if err := correlator.Stop(); err != nil {
			log.Error().Err(err).Msg("Correlator stop error")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Error().Err(err).Msg("Server forced to shutdown")
		}
	}()

	log.Info().Str("port", cfg.Port).Msg("Starting server")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("Server failed")
	}

	log.Info().Msg("Server stopped gracefully")
}

func registerMetricsRoutes(r *http.ServeMux, metricsHandler http.Handler) {
	r.Handle("/metrics", metricsHandler)
}

func initLogger(cfg config.Config) {
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	if cfg.Environment == "development" {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	} else {
		log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}
}
