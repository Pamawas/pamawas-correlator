package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
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
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("/etc/pamawas/")
	v.SetEnvPrefix("PAMAWAS_CORRELATOR")
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("port", "8080")
	v.SetDefault("log_level", "info")
	v.SetDefault("environment", "development")
	v.SetDefault("time_window", "10m")
	v.SetDefault("interval", "1m")
	v.SetDefault("mode", "auto")

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

	cfg := Config{
		DatabaseURL: v.GetString("database_url"),
		Port:        v.GetString("port"),
		LogLevel:    v.GetString("log_level"),
		Environment: v.GetString("environment"),
		TimeWindow:  timeWindow,
		Interval:    interval,
		Mode:        v.GetString("mode"),
	}

	if cfg.DatabaseURL == "" {
		panic("DATABASE_URL not set (config file or PAMAWAS_CORRELATOR_DATABASE_URL env var)")
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
	return nil
}