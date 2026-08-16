package config

import (
	"fmt"
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
	timeWindow, err := time.ParseDuration(timeWindowStr)
	if err != nil {
		panic(fmt.Sprintf("invalid CORRELATION_TIME_WINDOW: %v", err))
	}

	intervalStr := getEnv("CORRELATION_INTERVAL", "1m")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		panic(fmt.Sprintf("invalid CORRELATION_INTERVAL: %v", err))
	}

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