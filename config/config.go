package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	ServiceToken string
	DatabaseURL  string
	Port         string
	LogLevel     string
	Environment  string
	TimeWindow   time.Duration
	Interval     time.Duration
	Mode         string

	// Centralized circuit-breaker + retry-budget configuration (design review P2).
	CircuitBreakerThreshold    int
	CircuitBreakerResetTimeout time.Duration
	OutboxMaxAttempts          int

	// Inbound HTTP rate limiting (design review P5). Applies per route;
	// /healthz, /ready and /metrics are exempt.
	RateLimitRPS   float64
	RateLimitBurst int
}

func Load() Config {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("/etc/pamawas/")
	v.SetEnvPrefix("PAMAWAS_CORRELATOR")
	// Don't use AutomaticEnv() - Kubernetes injects PAMAWAS_CORRELATOR_PORT=tcp://IP:PORT
	// which would override our config. Explicitly bind only what we need.
	if err := v.BindEnv("service_token", "PAMAWAS_SERVICE_TOKEN"); err != nil {
		panic(fmt.Sprintf("failed to bind PAMAWAS_SERVICE_TOKEN env: %v", err))
	}
	if err := v.BindEnv("database_url", "DATABASE_URL", "PAMAWAS_CORRELATOR_DATABASE_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind DATABASE_URL env: %v", err))
	}
	if err := v.BindEnv("port", "PORT", "PAMAWAS_CORRELATOR_PORT"); err != nil {
		panic(fmt.Sprintf("failed to bind PORT env: %v", err))
	}
	if err := v.BindEnv("log_level", "LOG_LEVEL", "PAMAWAS_CORRELATOR_LOG_LEVEL"); err != nil {
		panic(fmt.Sprintf("failed to bind LOG_LEVEL env: %v", err))
	}
	if err := v.BindEnv("environment", "ENVIRONMENT", "PAMAWAS_CORRELATOR_ENVIRONMENT"); err != nil {
		panic(fmt.Sprintf("failed to bind ENVIRONMENT env: %v", err))
	}
	if err := v.BindEnv("time_window", "CORRELATION_TIME_WINDOW", "PAMAWAS_CORRELATOR_CORRELATION_TIME_WINDOW"); err != nil {
		panic(fmt.Sprintf("failed to bind CORRELATION_TIME_WINDOW env: %v", err))
	}
	if err := v.BindEnv("interval", "CORRELATION_INTERVAL", "PAMAWAS_CORRELATOR_CORRELATION_INTERVAL"); err != nil {
		panic(fmt.Sprintf("failed to bind CORRELATION_INTERVAL env: %v", err))
	}
	if err := v.BindEnv("mode", "CORRELATOR_MODE", "PAMAWAS_CORRELATOR_MODE"); err != nil {
		panic(fmt.Sprintf("failed to bind CORRELATOR_MODE env: %v", err))
	}
	if err := v.BindEnv("circuit_breaker_threshold", "CIRCUIT_BREAKER_THRESHOLD"); err != nil {
		panic(fmt.Sprintf("failed to bind CIRCUIT_BREAKER_THRESHOLD env: %v", err))
	}
	if err := v.BindEnv("circuit_breaker_reset_timeout", "CIRCUIT_BREAKER_RESET_TIMEOUT"); err != nil {
		panic(fmt.Sprintf("failed to bind CIRCUIT_BREAKER_RESET_TIMEOUT env: %v", err))
	}
	if err := v.BindEnv("outbox_max_attempts", "OUTBOX_MAX_ATTEMPTS"); err != nil {
		panic(fmt.Sprintf("failed to bind OUTBOX_MAX_ATTEMPTS env: %v", err))
	}
	if err := v.BindEnv("rate_limit_rps", "RATE_LIMIT_RPS"); err != nil {
		panic(fmt.Sprintf("failed to bind RATE_LIMIT_RPS env: %v", err))
	}
	if err := v.BindEnv("rate_limit_burst", "RATE_LIMIT_BURST"); err != nil {
		panic(fmt.Sprintf("failed to bind RATE_LIMIT_BURST env: %v", err))
	}

	// Defaults
	v.SetDefault("port", "8080")
	v.SetDefault("log_level", "info")
	v.SetDefault("environment", "development")
	v.SetDefault("time_window", "10m")
	v.SetDefault("interval", "1m")
	v.SetDefault("mode", "auto")
	v.SetDefault("circuit_breaker_threshold", 5)
	v.SetDefault("circuit_breaker_reset_timeout", "30s")
	v.SetDefault("outbox_max_attempts", 5)
	v.SetDefault("rate_limit_rps", 10)
	v.SetDefault("rate_limit_burst", 20)

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(fmt.Sprintf("failed to read config: %v", err))
		}
	}

	timeWindow, err := time.ParseDuration(v.GetString("time_window"))
	if err != nil {
		panic(fmt.Sprintf("invalid time_window: %v", err))
	}
	interval, err := time.ParseDuration(v.GetString("interval"))
	if err != nil {
		panic(fmt.Sprintf("invalid interval: %v", err))
	}
	cbResetTimeout, err := time.ParseDuration(v.GetString("circuit_breaker_reset_timeout"))
	if err != nil {
		panic(fmt.Sprintf("invalid circuit_breaker_reset_timeout: %v", err))
	}

	cfg := Config{
		ServiceToken:               v.GetString("service_token"),
		DatabaseURL:                v.GetString("database_url"),
		Port:                       v.GetString("port"),
		LogLevel:                   v.GetString("log_level"),
		Environment:                v.GetString("environment"),
		TimeWindow:                 timeWindow,
		Interval:                   interval,
		Mode:                       v.GetString("mode"),
		CircuitBreakerThreshold:    v.GetInt("circuit_breaker_threshold"),
		CircuitBreakerResetTimeout: cbResetTimeout,
		OutboxMaxAttempts:          v.GetInt("outbox_max_attempts"),
		RateLimitRPS:               v.GetFloat64("rate_limit_rps"),
		RateLimitBurst:             v.GetInt("rate_limit_burst"),
	}

	if err := cfg.Validate(); err != nil {
		panic(fmt.Sprintf("invalid configuration: %v", err))
	}
	return cfg
}

func (c Config) Validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("database_url is required")
	}
	if c.Port == "" {
		return fmt.Errorf("port is required")
	}
	if c.TimeWindow <= 0 {
		return fmt.Errorf("time_window must be positive")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if c.CircuitBreakerThreshold < 1 {
		return fmt.Errorf("circuit_breaker_threshold must be >= 1")
	}
	if c.CircuitBreakerResetTimeout <= 0 {
		return fmt.Errorf("circuit_breaker_reset_timeout must be positive")
	}
	if c.OutboxMaxAttempts < 1 {
		return fmt.Errorf("outbox_max_attempts must be >= 1")
	}
	if c.RateLimitRPS <= 0 {
		return fmt.Errorf("rate_limit_rps must be positive")
	}
	if c.RateLimitBurst < 1 {
		return fmt.Errorf("rate_limit_burst must be >= 1")
	}
	return nil
}
