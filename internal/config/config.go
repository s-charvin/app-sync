package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL       string
	JWTSecret         string
	SupabaseURL       string
	ListenAddr        string
	LogLevel          string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ShutdownTimeout   time.Duration
	DBMaxConns        int
	DBMinConns        int
	DBMaxConnLifetime time.Duration
	DBMaxConnIdleTime time.Duration
	AppName           string
}

func (c *Config) JWKSURL() string {
	return strings.TrimRight(c.SupabaseURL, "/") + "/auth/v1/.well-known/jwks.json"
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		JWTSecret:          os.Getenv("JWT_SECRET"),
		SupabaseURL:        os.Getenv("SUPABASE_URL"),
		ListenAddr:         getEnv("LISTEN_ADDR", ":8080"),
		LogLevel:           getEnv("LOG_LEVEL", "info"),
		ReadTimeout:        getDuration("READ_TIMEOUT", 120*time.Second),
		WriteTimeout:       getDuration("WRITE_TIMEOUT", 120*time.Second),
		IdleTimeout:        getDuration("IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout:    getDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		DBMaxConns:         getInt("DB_MAX_CONNS", 25),
		DBMinConns:         getInt("DB_MIN_CONNS", 5),
		DBMaxConnLifetime:  getDuration("DB_MAX_CONN_LIFETIME", 30*time.Minute),
		DBMaxConnIdleTime:  getDuration("DB_MAX_CONN_IDLE_TIME", 5*time.Minute),
		AppName:            getEnv("APP_NAME", "app-sync"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.SupabaseURL == "" {
		return nil, fmt.Errorf("SUPABASE_URL is required for JWT verification via JWKS")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
