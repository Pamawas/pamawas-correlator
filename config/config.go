package config

import (
	"os"
	"time"
)

type Config struct {
	DatabaseURL  string
	Port         string
	LogLevel     string
	Environment  string
	TimeWindow   time.Duration
	Interval     time.Duration
	Mode         string
}

func Load() Config {
	timeWindowStr := getEnv("CORRELATION_TIME_WINDOW", "10m")
	timeWindow, _ := time.ParseDuration(timeWindowStr)

	intervalStr := getEnv("CORRELATION_INTERVAL", "1m")
	interval, _ := time.ParseDuration(intervalStr)

	cfg := Config{
		DatabaseURL: getEnv("DATABASE_URL", ""),
		Port:        getEnv("PORT", "8080"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		Environment: getEnv("ENVIRONMENT", "development"),
		TimeWindow:  timeWindow,
		Interval:    interval,
		Mode:        getEnv("CORRELATOR_MODE", "auto"),
	}

	if cfg.DatabaseURL == "" {
		panic("DATABASE_URL environment variable not set")
	}
	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}