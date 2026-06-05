package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
	os.Setenv("SUPABASE_URL", "https://test.supabase.co")
	defer os.Unsetenv("DATABASE_URL")
	defer os.Unsetenv("SUPABASE_URL")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "app-sync", cfg.AppName)
	assert.Equal(t, ":8080", cfg.ListenAddr)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 120*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 25, cfg.DBMaxConns)
	assert.Equal(t, 5, cfg.DBMinConns)
	assert.Equal(t, "https://test.supabase.co/auth/v1/.well-known/jwks.json", cfg.JWKSURL())
}

func TestLoad_Overrides(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
	os.Setenv("SUPABASE_URL", "https://test.supabase.co")
	os.Setenv("LISTEN_ADDR", ":9090")
	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("DB_MAX_CONNS", "50")
	os.Setenv("DB_MIN_CONNS", "10")
	defer os.Unsetenv("DATABASE_URL")
	defer os.Unsetenv("SUPABASE_URL")
	defer os.Unsetenv("LISTEN_ADDR")
	defer os.Unsetenv("LOG_LEVEL")
	defer os.Unsetenv("DB_MAX_CONNS")
	defer os.Unsetenv("DB_MIN_CONNS")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, ":9090", cfg.ListenAddr)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, 50, cfg.DBMaxConns)
	assert.Equal(t, 10, cfg.DBMinConns)
}

func TestLoad_MissingDatabaseURL(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Setenv("SUPABASE_URL", "https://test.supabase.co")
	defer os.Unsetenv("SUPABASE_URL")

	_, err := Load()
	assert.ErrorContains(t, err, "DATABASE_URL is required")
}

func TestLoad_MissingSupabaseURL(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
	os.Unsetenv("SUPABASE_URL")

	_, err := Load()
	assert.ErrorContains(t, err, "SUPABASE_URL is required")
}

func TestGetDuration(t *testing.T) {
	t.Run("returns parsed duration", func(t *testing.T) {
		os.Setenv("TEST_DURATION", "30s")
		defer os.Unsetenv("TEST_DURATION")
		assert.Equal(t, 30*time.Second, getDuration("TEST_DURATION", 10*time.Second))
	})

	t.Run("returns default on invalid value", func(t *testing.T) {
		os.Setenv("TEST_DURATION", "not-a-duration")
		defer os.Unsetenv("TEST_DURATION")
		assert.Equal(t, 10*time.Second, getDuration("TEST_DURATION", 10*time.Second))
	})

	t.Run("returns default when env unset", func(t *testing.T) {
		os.Unsetenv("TEST_DURATION")
		assert.Equal(t, 10*time.Second, getDuration("TEST_DURATION", 10*time.Second))
	})
}

func TestGetInt(t *testing.T) {
	t.Run("returns parsed int", func(t *testing.T) {
		os.Setenv("TEST_INT", "42")
		defer os.Unsetenv("TEST_INT")
		assert.Equal(t, 42, getInt("TEST_INT", 10))
	})

	t.Run("returns default on invalid value", func(t *testing.T) {
		os.Setenv("TEST_INT", "not-an-int")
		defer os.Unsetenv("TEST_INT")
		assert.Equal(t, 10, getInt("TEST_INT", 10))
	})

	t.Run("returns default when env unset", func(t *testing.T) {
		os.Unsetenv("TEST_INT")
		assert.Equal(t, 10, getInt("TEST_INT", 10))
	})
}
