package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	DatabaseURL        string
	Port               string
	LogLevel           string
	Environment        string
	TimeWindow         time.Duration
	Interval           time.Duration
	Mode               string
	DiscordWebhookURL  string
	TelegramBotToken   string
	TelegramChatID     string
	TelegramAPIBaseURL string
	EmailSMTPHost      string
	EmailSMTPPort      int
	EmailUsername      string
	EmailPassword      string
	EmailFrom          string
	EmailTo            string
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
	if err := v.BindEnv("discord_webhook_url", "DISCORD_WEBHOOK_URL", "PAMAWAS_CORRELATOR_DISCORD_WEBHOOK_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind DISCORD_WEBHOOK_URL env: %v", err))
	}
	if err := v.BindEnv("telegram_bot_token", "TELEGRAM_BOT_TOKEN", "PAMAWAS_CORRELATOR_TELEGRAM_BOT_TOKEN"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_BOT_TOKEN env: %v", err))
	}
	if err := v.BindEnv("telegram_chat_id", "TELEGRAM_CHAT_ID", "PAMAWAS_CORRELATOR_TELEGRAM_CHAT_ID"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_CHAT_ID env: %v", err))
	}
	if err := v.BindEnv("telegram_api_base_url", "TELEGRAM_API_BASE_URL", "PAMAWAS_CORRELATOR_TELEGRAM_API_BASE_URL"); err != nil {
		panic(fmt.Sprintf("failed to bind TELEGRAM_API_BASE_URL env: %v", err))
	}
	if err := v.BindEnv("email_smtp_host", "EMAIL_SMTP_HOST", "PAMAWAS_CORRELATOR_EMAIL_SMTP_HOST"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_SMTP_HOST env: %v", err))
	}
	if err := v.BindEnv("email_smtp_port", "EMAIL_SMTP_PORT", "PAMAWAS_CORRELATOR_EMAIL_SMTP_PORT"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_SMTP_PORT env: %v", err))
	}
	if err := v.BindEnv("email_username", "EMAIL_USERNAME", "PAMAWAS_CORRELATOR_EMAIL_USERNAME"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_USERNAME env: %v", err))
	}
	if err := v.BindEnv("email_password", "EMAIL_PASSWORD", "PAMAWAS_CORRELATOR_EMAIL_PASSWORD"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_PASSWORD env: %v", err))
	}
	if err := v.BindEnv("email_from", "EMAIL_FROM", "PAMAWAS_CORRELATOR_EMAIL_FROM"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_FROM env: %v", err))
	}
	if err := v.BindEnv("email_to", "EMAIL_TO", "PAMAWAS_CORRELATOR_EMAIL_TO"); err != nil {
		panic(fmt.Sprintf("failed to bind EMAIL_TO env: %v", err))
	}

	// Defaults
	v.SetDefault("port", "8080")
	v.SetDefault("log_level", "info")
	v.SetDefault("environment", "development")
	v.SetDefault("time_window", "10m")
	v.SetDefault("interval", "1m")
	v.SetDefault("mode", "auto")
	v.SetDefault("email_smtp_port", 587)
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
		DatabaseURL:        v.GetString("database_url"),
		Port:               v.GetString("port"),
		LogLevel:           v.GetString("log_level"),
		Environment:        v.GetString("environment"),
		TimeWindow:         timeWindow,
		Interval:           interval,
		Mode:               v.GetString("mode"),
		DiscordWebhookURL:  v.GetString("discord_webhook_url"),
		TelegramBotToken:   v.GetString("telegram_bot_token"),
		TelegramChatID:     v.GetString("telegram_chat_id"),
		TelegramAPIBaseURL: v.GetString("telegram_api_base_url"),
		EmailSMTPHost:      v.GetString("email_smtp_host"),
		EmailSMTPPort:      v.GetInt("email_smtp_port"),
		EmailUsername:      v.GetString("email_username"),
		EmailPassword:      v.GetString("email_password"),
		EmailFrom:          v.GetString("email_from"),
		EmailTo:            v.GetString("email_to"),
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
	return nil
}
