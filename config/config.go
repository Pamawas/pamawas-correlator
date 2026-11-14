package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DatabaseURL string
	Port        string
	LogLevel    string
	Environment string
	TimeWindow  time.Duration
	Interval    time.Duration
	Mode        string

	// Centralized circuit-breaker + retry-budget configuration (design review P2).
	CircuitBreakerThreshold    int
	CircuitBreakerResetTimeout time.Duration
	OutboxMaxAttempts          int
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
	}

	if cfg.DatabaseURL == "" {
		panic("DATABASE_URL not set (config file or PAMAWAS_CORRELATOR_DATABASE_URL / DATABASE_URL env var)")
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
	return nil
}
