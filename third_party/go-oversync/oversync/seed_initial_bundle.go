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


