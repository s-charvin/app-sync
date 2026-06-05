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
	// 1. Copy thing_attribute_type templates → per-user thing_attribute
	_, err := tx.Exec(ctx, `
		INSERT INTO public.thing_attribute (id, user_id, attribute_type_id, code, name, description, status, is_system, source, created_at, updated_at)
		SELECT gen_random_uuid(), $1, att.id, att.code, att.name, att.description, 'active', true, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_attribute_type att
		WHERE att.user_id IS NULL
		  AND NOT EXISTS (SELECT 1 FROM public.thing_attribute a2 WHERE a2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed thing_attribute: %w", err)
	}

	// 2. Copy system thing_tag → per-user copies
	_, err = tx.Exec(ctx, `
		INSERT INTO public.thing_tag (id, user_id, code, name, description, icon, color, is_system, status, source, created_at, updated_at)
		SELECT gen_random_uuid(), $1, code, name, description, icon, color, is_system, 'active', 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_tag
		WHERE is_system = true AND user_id IS NULL
		  AND NOT EXISTS (SELECT 1 FROM public.thing_tag t2 WHERE t2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed thing_tag: %w", err)
	}

	// 3. Copy system thing_scenario → per-user copies
	_, err = tx.Exec(ctx, `
		INSERT INTO public.thing_scenario (id, user_id, code, name, description, icon, status, priority, source, created_at, updated_at)
		SELECT gen_random_uuid(), $1, code, name, description, icon, 'active', 0, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_scenario
		WHERE user_id IS NULL
		  AND NOT EXISTS (SELECT 1 FROM public.thing_scenario s2 WHERE s2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed thing_scenario: %w", err)
	}

	// 4. tag-attribute bindings (using UUID FK to per-user copies)
	_, err = tx.Exec(ctx, `
		INSERT INTO public.thing_tag_attribute_binding (id, tag_id, attribute_id, required, source, created_at, updated_at)
		SELECT gen_random_uuid(), tag.id, attr.id, true, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_attribute attr
		JOIN public.thing_tag tag ON tag.user_id = $1 AND attr.user_id = $1
		WHERE (tag.code, attr.code) IN (
			('perishable', 'expiry_date'),
			('consumable', 'quantity'), ('consumable', 'capacity'),
			('seasonal', 'season'),
			('collectible', 'base_value'), ('collectible', 'current_value'),
			('valuable', 'base_value'), ('valuable', 'current_value'),
			('lendable_out', 'lend_date'),
			('lendable_in', 'borrow_date'),
			('lostable', 'location'),
			('maintainable', 'maintenance_cycle'), ('maintainable', 'last_maintenance_date')
		)
		AND NOT EXISTS (SELECT 1 FROM public.thing_tag_attribute_binding b2
			JOIN public.thing_tag t2 ON t2.id = b2.tag_id
			WHERE t2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed tag_attribute_binding: %w", err)
	}

	// 5. scenario-tag bindings (using UUID FK to per-user copies)
	_, err = tx.Exec(ctx, `
		INSERT INTO public.thing_scenario_tag_binding (id, scenario_id, tag_id, source, created_at, updated_at)
		SELECT gen_random_uuid(), s.id, t.id, 'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_scenario s
		JOIN public.thing_tag t ON t.user_id = $1
		WHERE s.user_id = $1
		AND (s.code, t.code) IN (
			('expiry_reminder', 'perishable'),
			('warranty_expiry', 'valuable'),
			('low_stock_alert', 'consumable'),
			('lend_due_reminder', 'lendable_out'),
			('borrow_due_reminder', 'lendable_in'),
			('season_start_reminder', 'seasonal'),
			('season_end_pack', 'seasonal')
		)
		AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_tag_binding b2
			JOIN public.thing_scenario s2 ON s2.id = b2.scenario_id
			WHERE s2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed scenario_tag_binding: %w", err)
	}

	// 6. scenario conditions
	_, err = tx.Exec(ctx, `
		INSERT INTO public.thing_scenario_condition (id, scenario_id, expression, source, created_at, updated_at)
		SELECT gen_random_uuid(), s.id,
			CASE s.code
				WHEN 'expiry_reminder'       THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"expiry_date","operator":"DAYS_UNTIL_LESS_THAN_OR_EQUAL","value":"3","valueType":"DATE","valueSource":"STATIC"}]}'
				WHEN 'warranty_expiry'       THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"purchase_date","operator":"DAYS_SINCE_GREATER_THAN_OR_EQUAL","value":"365","valueType":"DATE","valueSource":"STATIC"}]}'
				WHEN 'low_stock_alert'       THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"quantity","operator":"LESS_THAN_PERCENTAGE","value":"capacity","valueType":"NUMBER","valueSource":"ATTRIBUTE","operatorParam":"20"}]}'
				WHEN 'lend_due_reminder'     THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"lend_date","operator":"DAYS_SINCE_GREATER_THAN_OR_EQUAL","value":"30","valueType":"DATE","valueSource":"STATIC"}]}'
				WHEN 'borrow_due_reminder'   THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"borrow_date","operator":"DAYS_SINCE_GREATER_THAN_OR_EQUAL","value":"30","valueType":"DATE","valueSource":"STATIC"}]}'
				WHEN 'season_start_reminder' THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"season","operator":"EQUALS","value":"spring","valueType":"STRING","valueSource":"STATIC"}]}'
				WHEN 'season_end_pack'       THEN '{"type":"logic","operator":"AND","children":[{"type":"condition","field":"season","operator":"EQUALS","value":"winter","valueType":"STRING","valueSource":"STATIC"}]}'
			END,
			'system', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		FROM public.thing_scenario s
		WHERE s.user_id = $1
		  AND s.code IN ('expiry_reminder','warranty_expiry','low_stock_alert','lend_due_reminder','borrow_due_reminder','season_start_reminder','season_end_pack')
		  AND NOT EXISTS (SELECT 1 FROM public.thing_scenario_condition c2
			JOIN public.thing_scenario s2 ON s2.id = c2.scenario_id
			WHERE s2.user_id = $1 LIMIT 1)
	`, userID)
	if err != nil {
		return fmt.Errorf("seed scenario_condition: %w", err)
	}

	s.logger.Info("seeded system data for user", "user_id", userID)
	return nil
}
