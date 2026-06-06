package seed

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

const sentinelScope = "00000000-0000-0000-0000-000000000000"

func SystemData(ctx context.Context, pool *pgxpool.Pool, userID string, logger *slog.Logger) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var userPK int64
	if err := tx.QueryRow(ctx, `SELECT user_pk FROM sync.user_state WHERE user_id = $1`, userID).Scan(&userPK); err != nil {
		return fmt.Errorf("lookup user_pk: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('oversync.bundle_user_id', $1, true)", userID); err != nil {
		return fmt.Errorf("set bundle_user_id: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SELECT set_config('oversync.bundle_user_pk', '%d', true)", userPK)); err != nil {
		return fmt.Errorf("set bundle_user_pk: %w", err)
	}

	// Entity tables — have user_id, filter templates by user_id IS NULL
	entityInserts := []struct {
		table string
		query string
	}{
		{"thing_attribute", `INSERT INTO public.thing_attribute (id, user_id, attribute_type_id, code, description, is_system, status, name, created_at, updated_at) SELECT gen_random_uuid(), $1, template.attribute_type_id, template.code, template.description, template.is_system, template.status, template.name, now(), now() FROM public.thing_attribute template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_attribute u WHERE u.user_id = $1 AND u.code = template.code)`},
		{"thing_tag", `INSERT INTO public.thing_tag (id, user_id, code, name, description, icon, color, is_system, status, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.color, template.is_system, template.status, now(), now() FROM public.thing_tag template WHERE template.user_id IS NULL AND template.is_system = true AND NOT EXISTS (SELECT 1 FROM public.thing_tag u WHERE u.user_id = $1 AND u.code = template.code)`},
		{"thing_scenario", `INSERT INTO public.thing_scenario (id, user_id, code, name, description, icon, status, priority, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.status, template.priority, now(), now() FROM public.thing_scenario template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario u WHERE u.user_id = $1 AND u.code = template.code)`},
	}
	for _, e := range entityInserts {
		tag, err := tx.Exec(ctx, e.query, userID)
		if err != nil {
			return fmt.Errorf("seed entity %s: %w", e.table, err)
		}
		logger.Info("seed: copied entity rows", "table", e.table, "user_id", userID, "rows", tag.RowsAffected())
	}

	// Binding tables — no user_id, filter templates by _sync_scope_id = sentinel
	bindingInserts := []struct {
		table string
		query string
	}{
		{"thing_tag_attribute_binding", `INSERT INTO public.thing_tag_attribute_binding (id, tag_id, attribute_id, created_at, updated_at) SELECT gen_random_uuid(), pt.id::text, pa.id::text, now(), now() FROM public.thing_tag_attribute_binding tb JOIN public.thing_tag tt ON tt.id::text = tb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id::text = $1 JOIN public.thing_attribute ta ON ta.id::text = tb.attribute_id AND ta.user_id IS NULL JOIN public.thing_attribute pa ON pa.code = ta.code AND pa.user_id::text = $1 WHERE tb._sync_scope_id = $2 AND NOT EXISTS (SELECT 1 FROM public.thing_tag_attribute_binding ub WHERE ub._sync_scope_id = $1 AND ub.tag_id = pt.id::text AND ub.attribute_id = pa.id::text)`},
		{"thing_scenario_tag_binding", `INSERT INTO public.thing_scenario_tag_binding (id, scenario_id, tag_id, created_at, updated_at) SELECT gen_random_uuid(), ps.id::text, pt.id::text, now(), now() FROM public.thing_scenario_tag_binding sb JOIN public.thing_scenario ts ON ts.id::text = sb.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id::text = $1 JOIN public.thing_tag tt ON tt.id::text = sb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id::text = $1 WHERE sb._sync_scope_id = $2 AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_tag_binding ub WHERE ub._sync_scope_id = $1 AND ub.scenario_id = ps.id::text AND ub.tag_id = pt.id::text)`},
		{"thing_scenario_condition", `INSERT INTO public.thing_scenario_condition (id, scenario_id, expression, created_at, updated_at) SELECT gen_random_uuid(), ps.id::text, tc.expression, now(), now() FROM public.thing_scenario_condition tc JOIN public.thing_scenario ts ON ts.id::text = tc.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id::text = $1 WHERE tc._sync_scope_id = $2 AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_condition uc WHERE uc._sync_scope_id = $1 AND uc.scenario_id = ps.id::text)`},
	}
	for _, b := range bindingInserts {
		tag, err := tx.Exec(ctx, b.query, userID, sentinelScope)
		if err != nil {
			return fmt.Errorf("seed binding %s: %w", b.table, err)
		}
		logger.Info("seed: copied binding rows", "table", b.table, "user_id", userID, "rows", tag.RowsAffected())
	}

	tables := []string{"thing_attribute", "thing_tag", "thing_scenario",
		"thing_tag_attribute_binding", "thing_scenario_tag_binding", "thing_scenario_condition"}
	bindingTables := map[string]bool{
		"thing_tag_attribute_binding": true, "thing_scenario_tag_binding": true, "thing_scenario_condition": true,
	}
	for _, table := range tables {
		var rsQuery string
		if bindingTables[table] {
			rsQuery = fmt.Sprintf(`INSERT INTO sync.row_state (user_pk, table_id, key_bytes, bundle_seq, deleted) SELECT $1, tc.table_id, uuid_send(t.id), 0, FALSE FROM public.%s t JOIN sync.table_catalog tc ON tc.schema_name = 'public' AND tc.table_name = $2 WHERE t._sync_scope_id = $3 ON CONFLICT DO NOTHING`, table)
		} else {
			rsQuery = fmt.Sprintf(`INSERT INTO sync.row_state (user_pk, table_id, key_bytes, bundle_seq, deleted) SELECT $1, tc.table_id, uuid_send(t.id), 0, FALSE FROM public.%s t JOIN sync.table_catalog tc ON tc.schema_name = 'public' AND tc.table_name = $2 WHERE t.user_id = $3 ON CONFLICT DO NOTHING`, table)
		}
		if _, err := tx.Exec(ctx, rsQuery, userPK, table, userID); err != nil {
			return fmt.Errorf("row_state %s: %w", table, err)
		}
	}

	var bundleSeq int64
	if err := tx.QueryRow(ctx, `UPDATE sync.user_state SET next_bundle_seq = next_bundle_seq + 1 WHERE user_pk = $1 RETURNING next_bundle_seq - 1`, userPK).Scan(&bundleSeq); err != nil {
		return fmt.Errorf("alloc bundle_seq: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO sync.bundle_log (user_pk, bundle_seq, source_id, source_bundle_id, row_count, byte_count, bundle_hash, committed_at) VALUES ($1, $2, 'system-seed', 1, 0, 0, ''::bytea, now())`, userPK, bundleSeq); err != nil {
		return fmt.Errorf("bundle_log init: %w", err)
	}

	var totalRows int64
	ord := int64(1)
	for _, table := range tables {
		var brQuery string
		if bindingTables[table] {
			brQuery = fmt.Sprintf(`INSERT INTO sync.bundle_rows (user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire) SELECT $1, $2, $3 + row_number() OVER () - 1, tc.table_id, uuid_send(t.id), 1, to_jsonb(t.*) - ARRAY['_sync_scope_id'] FROM public.%s t JOIN sync.table_catalog tc ON tc.table_name = $4 WHERE t._sync_scope_id = $5`, table)
		} else {
			brQuery = fmt.Sprintf(`INSERT INTO sync.bundle_rows (user_pk, bundle_seq, row_ordinal, table_id, key_bytes, op_code, payload_wire) SELECT $1, $2, $3 + row_number() OVER () - 1, tc.table_id, uuid_send(t.id), 1, to_jsonb(t.*) - ARRAY['_sync_scope_id'] FROM public.%s t JOIN sync.table_catalog tc ON tc.table_name = $4 WHERE t.user_id = $5`, table)
		}
		tag, err := tx.Exec(ctx, brQuery, userPK, bundleSeq, ord, table, userID)
		if err != nil {
			return fmt.Errorf("bundle_rows %s: %w", table, err)
		}
		ord += tag.RowsAffected()
		totalRows += tag.RowsAffected()
	}

	if _, err := tx.Exec(ctx, `UPDATE sync.bundle_log SET row_count = (SELECT COUNT(*) FROM sync.bundle_rows WHERE user_pk = $1 AND bundle_seq = $2), byte_count = (SELECT COALESCE(SUM(pg_column_size(payload_wire)), 0) FROM sync.bundle_rows WHERE user_pk = $1 AND bundle_seq = $2) WHERE user_pk = $1 AND bundle_seq = $2`, userPK, bundleSeq); err != nil {
		return fmt.Errorf("bundle_log update: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE sync.row_state SET bundle_seq = $2 WHERE user_pk = $1 AND bundle_seq = 0`, userPK, bundleSeq); err != nil {
		return fmt.Errorf("row_state update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	logger.Info("seed: system data seeded successfully",
		"user_id", userID,
		"bundle_seq", bundleSeq,
		"total_bundle_rows", totalRows,
	)
	return nil
}
