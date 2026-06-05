package oversync

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// ensureBusinessTables creates the business tables used by seedSystemData
// (IF NOT EXISTS) so the test can run against any test DB that has the sync
// schema but not necessarily the app business schema.
func ensureBusinessTables(ctx context.Context, pool *pgxpool.Pool) error {
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS public.thing_attribute_type (
			id TEXT PRIMARY KEY NOT NULL,
			user_id TEXT,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			value_type TEXT NOT NULL DEFAULT '',
			is_system INTEGER NOT NULL DEFAULT 0,
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_attribute (
			id TEXT PRIMARY KEY NOT NULL,
			user_id TEXT NOT NULL,
			attribute_type_id TEXT NOT NULL DEFAULT '',
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			status TEXT NOT NULL DEFAULT 'active',
			is_system INTEGER NOT NULL DEFAULT 0,
			last_used_at TEXT,
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_tag (
			id TEXT PRIMARY KEY NOT NULL,
			user_id TEXT,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			icon TEXT,
			color TEXT,
			is_system INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'active',
			last_used_at TEXT,
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_tag_attribute_binding (
			id TEXT PRIMARY KEY NOT NULL,
			tag_id TEXT NOT NULL DEFAULT '',
			attribute_id TEXT NOT NULL DEFAULT '',
			required INTEGER NOT NULL DEFAULT 0,
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_scenario (
			id TEXT PRIMARY KEY NOT NULL,
			user_id TEXT,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			icon TEXT,
			status TEXT NOT NULL DEFAULT 'active',
			priority INTEGER NOT NULL DEFAULT 0,
			last_used_at TEXT,
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_scenario_tag_binding (
			id TEXT PRIMARY KEY NOT NULL,
			scenario_id TEXT NOT NULL DEFAULT '',
			tag_id TEXT NOT NULL DEFAULT '',
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS public.thing_scenario_condition (
			id TEXT PRIMARY KEY NOT NULL,
			scenario_id TEXT NOT NULL DEFAULT '',
			expression TEXT NOT NULL DEFAULT '',
			source TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			deleted_at TEXT
		)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("ensure business table: %w", err)
		}
	}
	return nil
}

// cleanupSeedUser removes all rows owned by userID from the business tables
// that seedSystemData populates.
func cleanupSeedUser(ctx context.Context, pool *pgxpool.Pool, userID string) {
	for _, stmt := range []string{
		`DELETE FROM public.thing_scenario_condition
		 WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)`,
		`DELETE FROM public.thing_scenario_tag_binding
		 WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)`,
		`DELETE FROM public.thing_tag_attribute_binding
		 WHERE tag_id IN (SELECT id FROM public.thing_tag WHERE user_id = $1)`,
		`DELETE FROM public.thing_attribute WHERE user_id = $1`,
		`DELETE FROM public.thing_tag WHERE user_id = $1`,
		`DELETE FROM public.thing_scenario WHERE user_id = $1`,
	} {
		_, _ = pool.Exec(ctx, stmt, userID)
	}
}

// insertTemplateData populates the template rows that seedSystemData copies
// from.  These rows have user_id IS NULL (global templates).
func insertTemplateData(ctx context.Context, pool *pgxpool.Pool) error {
	// thing_attribute_type: 10 template rows
	attrTypes := []struct{ code, name, desc, valueType string }{
		{"expiry_date", "Expiry Date", "Expiration date", "date"},
		{"purchase_date", "Purchase Date", "Date of purchase", "date"},
		{"quantity", "Quantity", "Current quantity", "number"},
		{"capacity", "Capacity", "Max capacity", "number"},
		{"season", "Season", "Applicable season", "string"},
		{"base_value", "Base Value", "Purchase/base value", "number"},
		{"current_value", "Current Value", "Estimated current value", "number"},
		{"lend_date", "Lend Date", "Date item was lent", "date"},
		{"borrow_date", "Borrow Date", "Date item was borrowed", "date"},
		{"location", "Location", "Storage location", "string"},
	}
	if err := deleteTemplateData(ctx, pool); err != nil {
		return err
	}
	for _, at := range attrTypes {
		_, err := pool.Exec(ctx, `
			INSERT INTO public.thing_attribute_type (id, code, name, description, value_type, is_system, source, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, 1, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		`, uuid.NewString(), at.code, at.name, at.desc, at.valueType)
		if err != nil {
			return fmt.Errorf("insert template attribute type %s: %w", at.code, err)
		}
	}

	// thing_tag: 9 system tags
	tags := []struct{ code, name, icon, color string }{
		{"perishable", "Perishable", "clock", "#FF6B6B"},
		{"consumable", "Consumable", "package", "#4ECDC4"},
		{"seasonal", "Seasonal", "sun", "#FFE66D"},
		{"collectible", "Collectible", "star", "#A66CFF"},
		{"valuable", "Valuable", "diamond", "#FFD700"},
		{"lendable_out", "Lent Out", "arrow-up-right", "#45B7D1"},
		{"lendable_in", "Borrowed In", "arrow-down-left", "#96CEB4"},
		{"lostable", "Lostable", "search", "#DDA0DD"},
		{"maintainable", "Maintainable", "wrench", "#98D8C8"},
	}
	for _, t := range tags {
		_, err := pool.Exec(ctx, `
			INSERT INTO public.thing_tag (id, user_id, code, name, description, icon, color, is_system, status, source, created_at, updated_at)
			VALUES ($1, NULL, $2, $3, '', $4, $5, 1, 'active', 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		`, uuid.NewString(), t.code, t.name, t.icon, t.color)
		if err != nil {
			return fmt.Errorf("insert template tag %s: %w", t.code, err)
		}
	}

	// thing_scenario: 7 system scenarios
	scenarios := []struct{ code, name, icon string }{
		{"expiry_reminder", "Expiry Reminder", "alert-triangle"},
		{"warranty_expiry", "Warranty Expiry", "shield"},
		{"low_stock_alert", "Low Stock Alert", "shopping-cart"},
		{"lend_due_reminder", "Lend Due Reminder", "calendar"},
		{"borrow_due_reminder", "Borrow Due Reminder", "calendar-plus"},
		{"season_start_reminder", "Season Start Reminder", "sunrise"},
		{"season_end_pack", "Season End Pack", "sunset"},
	}
	for _, s := range scenarios {
		_, err := pool.Exec(ctx, `
			INSERT INTO public.thing_scenario (id, user_id, code, name, description, icon, status, priority, source, created_at, updated_at)
			VALUES ($1, NULL, $2, $3, '', $4, 'active', 0, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		`, uuid.NewString(), s.code, s.name, s.icon)
		if err != nil {
			return fmt.Errorf("insert template scenario %s: %w", s.code, err)
		}
	}

	return nil
}

func deleteTemplateData(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range []string{
		`DELETE FROM public.thing_attribute_type WHERE is_system = 1`,
		`DELETE FROM public.thing_tag WHERE is_system = 1 AND user_id IS NULL`,
		`DELETE FROM public.thing_scenario WHERE user_id IS NULL`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("delete template data: %w", err)
		}
	}
	return nil
}

func newSeedTestService(pool *pgxpool.Pool) *SyncService {
	return &SyncService{
		pool:   pool,
		logger: integrationTestLogger(slog.LevelWarn),
	}
}

func TestSeedSystemData_RowCounts(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)

	require.NoError(t, ensureBusinessTables(ctx, pool))
	require.NoError(t, insertTemplateData(ctx, pool))

	userID := "seed-rc-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() { cleanupSeedUser(context.Background(), pool, userID) })

	svc := newSeedTestService(pool)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	err = svc.seedSystemData(ctx, tx, userID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Assert per-user row counts.
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_attribute WHERE user_id = $1", userID, 10)
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_tag WHERE user_id = $1", userID, 9)
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_scenario WHERE user_id = $1", userID, 7)
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_tag_attribute_binding WHERE tag_id IN (SELECT id FROM public.thing_tag WHERE user_id = $1)", userID, 13)
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_scenario_tag_binding WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)", userID, 7)
	assertRowCount(t, ctx, pool,
		"SELECT COUNT(*) FROM public.thing_scenario_condition WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)", userID, 7)
}

func TestSeedSystemData_Idempotent(t *testing.T) {
	ctx := context.Background()
	pool := newIntegrationTestPool(t, ctx)

	require.NoError(t, ensureBusinessTables(ctx, pool))
	require.NoError(t, insertTemplateData(ctx, pool))

	userID := "seed-idem-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() { cleanupSeedUser(context.Background(), pool, userID) })

	svc := newSeedTestService(pool)

	// First call: seed data and commit.
	conn1, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn1.Release()

	tx1, err := conn1.Begin(ctx)
	require.NoError(t, err)
	err = svc.seedSystemData(ctx, tx1, userID)
	require.NoError(t, err)
	require.NoError(t, tx1.Commit(ctx))

	// Capture row counts after first seed.
	counts := make(map[string]int)
	for table, query := range map[string]string{
		"thing_attribute":              "SELECT COUNT(*) FROM public.thing_attribute WHERE user_id = $1",
		"thing_tag":                    "SELECT COUNT(*) FROM public.thing_tag WHERE user_id = $1",
		"thing_scenario":               "SELECT COUNT(*) FROM public.thing_scenario WHERE user_id = $1",
		"tag_attribute_binding":        "SELECT COUNT(*) FROM public.thing_tag_attribute_binding WHERE tag_id IN (SELECT id FROM public.thing_tag WHERE user_id = $1)",
		"scenario_tag_binding":         "SELECT COUNT(*) FROM public.thing_scenario_tag_binding WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)",
		"scenario_condition":           "SELECT COUNT(*) FROM public.thing_scenario_condition WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)",
	} {
		var c int
		require.NoError(t, pool.QueryRow(ctx, query, userID).Scan(&c))
		counts[table] = c
	}

	// Second call: should be idempotent (NOT EXISTS guards skip the insert).
	conn2, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn2.Release()

	tx2, err := conn2.Begin(ctx)
	require.NoError(t, err)
	err = svc.seedSystemData(ctx, tx2, userID)
	require.NoError(t, err)
	require.NoError(t, tx2.Commit(ctx))

	// Assert row counts unchanged.
	for table, query := range map[string]string{
		"thing_attribute":              "SELECT COUNT(*) FROM public.thing_attribute WHERE user_id = $1",
		"thing_tag":                    "SELECT COUNT(*) FROM public.thing_tag WHERE user_id = $1",
		"thing_scenario":               "SELECT COUNT(*) FROM public.thing_scenario WHERE user_id = $1",
		"tag_attribute_binding":        "SELECT COUNT(*) FROM public.thing_tag_attribute_binding WHERE tag_id IN (SELECT id FROM public.thing_tag WHERE user_id = $1)",
		"scenario_tag_binding":         "SELECT COUNT(*) FROM public.thing_scenario_tag_binding WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)",
		"scenario_condition":           "SELECT COUNT(*) FROM public.thing_scenario_condition WHERE scenario_id IN (SELECT id FROM public.thing_scenario WHERE user_id = $1)",
	} {
		var c int
		require.NoError(t, pool.QueryRow(ctx, query, userID).Scan(&c))
		require.Equal(t, counts[table], c, "table %s: expected %d rows after second seed, got %d", table, counts[table], c)
	}
}

func assertRowCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, userID string, expected int) {
	t.Helper()
	var count int
	require.NoError(t, pool.QueryRow(ctx, query, userID).Scan(&count))
	require.Equal(t, expected, count, "query: %s", query)
}
