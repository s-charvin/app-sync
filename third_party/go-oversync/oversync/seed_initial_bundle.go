// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
)

// hasExistingData checks whether any registered business table contains rows
// owned by the given user. It short-circuits on the first table that has data.
func (s *SyncService) hasExistingData(ctx context.Context, tx pgx.Tx, userID string) (bool, error) {
	tableInfos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		tableInfos = append(tableInfos, info)
	}
	sort.Slice(tableInfos, func(i, j int) bool {
		return tableInfos[i].tableID < tableInfos[j].tableID
	})

	for _, info := range tableInfos {
		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()
		query := fmt.Sprintf(`
			SELECT EXISTS(
				SELECT 1 FROM %s WHERE %s = $1 LIMIT 1
			)
		`, tableIdent, ownerColumnIdent)

		var exists bool
		if err := tx.QueryRow(ctx, query, userID).Scan(&exists); err != nil {
			s.logger.Warn("hasExistingData query failed, skipping table",
				"schema", info.schemaName, "table", info.tableName, "error", err)
			continue
		}
		if exists {
			s.logger.Debug("hasExistingData found data in table",
				"schema", info.schemaName, "table", info.tableName)
			return true, nil
		}
	}
	return false, nil
}

// seedInitialBundle reads existing business data for the user and creates a
// seed bundle so the client can pull it on the next sync.
// Returns true if a seed bundle was created, false if no data was found.
func (s *SyncService) seedInitialBundle(ctx context.Context, tx pgx.Tx, actor Actor, userPK int64, userID string) (bool, error) {
	var maxRows int64
	if s.config != nil && s.config.MaxRowsPerInitialSeed > 0 {
		maxRows = s.config.MaxRowsPerInitialSeed
	}

	tableInfos := make([]registeredTableRuntimeInfo, 0, len(s.registeredTableByID))
	for _, info := range s.registeredTableByID {
		tableInfos = append(tableInfos, info)
	}
	sort.Slice(tableInfos, func(i, j int) bool {
		return tableInfos[i].tableID < tableInfos[j].tableID
	})

	storageRows := make([]committedBundleStorageRow, 0)
	bundleRows := make([]BundleRow, 0)
	var rowCount int64

	for _, info := range tableInfos {
		if maxRows > 0 && rowCount >= maxRows {
			s.logger.Warn("seedInitialBundle row limit reached, skipping remaining tables",
				"max_rows", maxRows, "processed", rowCount)
			break
		}

		tableIdent := pgx.Identifier{info.schemaName, info.tableName}.Sanitize()
		keyColumnIdent := pgx.Identifier{info.syncKeyColumn}.Sanitize()
		ownerColumnIdent := pgx.Identifier{syncScopeColumnName}.Sanitize()

		query := fmt.Sprintf(`
			SELECT CAST(src.%s AS text), to_jsonb(src) - '%s'
			FROM %s AS src
			WHERE src.%s = $1
			ORDER BY CAST(src.%s AS text)
		`, keyColumnIdent, syncScopeColumnName, tableIdent, ownerColumnIdent, keyColumnIdent)

		limit := maxRows - rowCount
		if maxRows > 0 && limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", limit)
		}

		liveRows, err := tx.Query(ctx, query, userID)
		if err != nil {
			return false, fmt.Errorf("query live rows for %s.%s: %w", info.schemaName, info.tableName, err)
		}

		for liveRows.Next() {
			var keyText string
			var payloadDB []byte
			if err := liveRows.Scan(&keyText, &payloadDB); err != nil {
				liveRows.Close()
				return false, fmt.Errorf("scan live row for %s.%s: %w", info.schemaName, info.tableName, err)
			}

			keyBytes, _, err := encodeKeyBytes(info.syncKeyType, keyText)
			if err != nil {
				liveRows.Close()
				return false, fmt.Errorf("encode key for %s.%s: %w", info.schemaName, info.tableName, err)
			}

			payloadWire, err := s.canonicalizeWirePayload(info.schemaName, info.tableName, payloadDB)
			if err != nil {
				liveRows.Close()
				return false, fmt.Errorf("canonicalize payload for %s.%s: %w", info.schemaName, info.tableName, err)
			}

			rowCount++

			storageRows = append(storageRows, committedBundleStorageRow{
				tableID:     info.tableID,
				keyBytes:    append([]byte(nil), keyBytes...),
				opCode:      opCodeInsert,
				payloadWire: append([]byte(nil), payloadWire...),
			})

			key, err := wireSyncKeyFromBytes(info, keyBytes)
			if err != nil {
				liveRows.Close()
				return false, fmt.Errorf("decode key for bundle row %s.%s: %w", info.schemaName, info.tableName, err)
			}

			bundleRows = append(bundleRows, BundleRow{
				Schema:     info.schemaName,
				Table:      info.tableName,
				Key:        key,
				Op:         OpInsert,
				RowVersion: 0,
				Payload:    payloadWire,
			})
		}
		if err := liveRows.Err(); err != nil {
			liveRows.Close()
			return false, fmt.Errorf("iterate live rows for %s.%s: %w", info.schemaName, info.tableName, err)
		}
		liveRows.Close()
	}

	if rowCount == 0 {
		return false, nil
	}

	bundleSeq, err := reserveUserBundleSeq(ctx, tx, userPK)
	if err != nil {
		return false, fmt.Errorf("reserve bundle seq: %w", err)
	}

	// Assign bundle_seq as row_version for consistency with existing bundles
	for i := range bundleRows {
		bundleRows[i].RowVersion = bundleSeq
	}

	bundleHash, byteCount, err := computeCommittedBundleHash(bundleRows)
	if err != nil {
		return false, fmt.Errorf("compute bundle hash: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO sync.bundle_log (
			user_pk, bundle_seq, source_id, source_bundle_id, row_count, byte_count, bundle_hash, committed_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
	`, userPK, bundleSeq, "auto-seed", int64(1), rowCount, byteCount, bundleHash); err != nil {
		return false, fmt.Errorf("insert bundle_log: %w", err)
	}

	if err := persistCommittedBundleRows(ctx, tx, userPK, bundleSeq, storageRows); err != nil {
		return false, fmt.Errorf("persist bundle rows: %w", err)
	}

	return true, nil
}

// seedSystemData copies system data (attribute types, system tags, system
// scenarios, and their bindings/conditions) into per-user tables during the
// first connect flow.  Each section guards with NOT EXISTS so repeated calls
// against the same user are idempotent.
func (s *SyncService) seedSystemData(ctx context.Context, tx pgx.Tx, userID string) error {
	tables := []struct {
		name  string
		query string
	}{
		// Copy entities from template rows (user_id IS NULL)
		{name: "thing_attribute", query: `INSERT INTO public.thing_attribute (id, user_id, attribute_type_id, code, description, type_code, is_system, status, name, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.attribute_type_id, template.code, template.description, template.type_code, template.is_system, template.status, template.name, $1, now(), now() FROM public.thing_attribute template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_attribute u WHERE u.user_id = $1 AND u.code = template.code)`},
		{name: "thing_tag", query: `INSERT INTO public.thing_tag (id, user_id, code, name, description, icon, color, is_system, status, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.color, template.is_system, template.status, $1, now(), now() FROM public.thing_tag template WHERE template.user_id IS NULL AND template.is_system = true AND NOT EXISTS (SELECT 1 FROM public.thing_tag u WHERE u.user_id = $1 AND u.code = template.code)`},
		{name: "thing_scenario", query: `INSERT INTO public.thing_scenario (id, user_id, code, name, description, icon, status, priority, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, template.code, template.name, template.description, template.icon, template.status, template.priority, $1, now(), now() FROM public.thing_scenario template WHERE template.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario u WHERE u.user_id = $1 AND u.code = template.code)`},
		// Copy bindings — JOIN template rows to per-user copies by code
		{name: "thing_tag_attribute_binding", query: `INSERT INTO public.thing_tag_attribute_binding (id, user_id, tag_id, attribute_id, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, pt.id, pa.id, $1, now(), now() FROM public.thing_tag_attribute_binding tb JOIN public.thing_tag tt ON tt.id = tb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id = $1 JOIN public.thing_attribute ta ON ta.id = tb.attribute_id AND ta.user_id IS NULL JOIN public.thing_attribute pa ON pa.code = ta.code AND pa.user_id = $1 WHERE tb.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_tag_attribute_binding ub WHERE ub.user_id = $1 AND ub.tag_id = pt.id AND ub.attribute_id = pa.id)`},
		{name: "thing_scenario_tag_binding", query: `INSERT INTO public.thing_scenario_tag_binding (id, user_id, scenario_id, tag_id, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, ps.id, pt.id, $1, now(), now() FROM public.thing_scenario_tag_binding sb JOIN public.thing_scenario ts ON ts.id = sb.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id = $1 JOIN public.thing_tag tt ON tt.id = sb.tag_id AND tt.user_id IS NULL JOIN public.thing_tag pt ON pt.code = tt.code AND pt.user_id = $1 WHERE sb.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_tag_binding ub WHERE ub.user_id = $1 AND ub.scenario_id = ps.id AND ub.tag_id = pt.id)`},
		{name: "thing_scenario_condition", query: `INSERT INTO public.thing_scenario_condition (id, user_id, scenario_id, expression_ast, _sync_scope_id, created_at, updated_at) SELECT gen_random_uuid(), $1, ps.id, tc.expression_ast, $1, now(), now() FROM public.thing_scenario_condition tc JOIN public.thing_scenario ts ON ts.id = tc.scenario_id AND ts.user_id IS NULL JOIN public.thing_scenario ps ON ps.code = ts.code AND ps.user_id = $1 WHERE tc.user_id IS NULL AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_condition uc WHERE uc.user_id = $1 AND uc.scenario_id = ps.id)`},
	}

	for _, t := range tables {
		tag, err := tx.Exec(ctx, t.query, userID)
		if err != nil {
			return fmt.Errorf("seed %s: %w", t.name, err)
		}
		s.logger.Debug("seeded system data", "table", t.name, "rows", tag.RowsAffected())
	}

	s.logger.Info("seeded system data for user", "user_id", userID)
	return nil
}

