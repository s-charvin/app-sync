// Copyright 2025 Toly Pochkin
// SPDX-License-Identifier: Apache-2.0

package oversync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *SyncService) ProcessPull(
	ctx context.Context,
	actor Actor,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (resp *PullResponse, err error) {
	done, err := s.beginOperation()
	if err != nil {
		return nil, err
	}
	defer done()
	totalStart := s.stageStart()
	defer func() {
		s.observeStageErr(ctx, "pull", "total", totalStart, maxBundles, 0, err)
	}()
	if err := actor.validate(false); err != nil {
		return nil, err
	}
	if maxBundles <= 0 {
		maxBundles = defaultPullBundlesPerRequest
	}
	if maxBundles > defaultMaxBundlesPerPull {
		maxBundles = defaultMaxBundlesPerPull
	}
	if afterBundleSeq < 0 {
		return nil, fmt.Errorf("after_bundle_seq must be >= 0")
	}
	if targetBundleSeq < 0 {
		return nil, fmt.Errorf("target_bundle_seq must be >= 0")
	}

	// Auto-initialize uninitialized scope before pull.
	initStart := time.Now()
	if err := s.ensureScopeInitializedForPull(ctx, actor); err != nil {
		return nil, err
	}
	if d := time.Since(initStart); d > 10*time.Millisecond {
		s.logger.Warn("pull: slow scope init", "duration", d, "user_id", actor.UserID)
	}

	// Detach from HTTP request context so client disconnect doesn't cancel
	// in-flight database work. The read-only transaction is harmless to let
	// complete, and completing it means the next client retry finds data ready.
	detachedCtx := context.WithoutCancel(ctx)
	var txResp *PullResponse
	txStart := s.stageStart()
	err = pgx.BeginTxFunc(detachedCtx, s.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var txErr error
		txResp, txErr = s.processPullQuerier(detachedCtx, tx, actor.UserID, afterBundleSeq, maxBundles, targetBundleSeq)
		return txErr
	})
	if d := time.Since(txStart); d > 100*time.Millisecond {
		s.logger.Warn("pull: slow transaction", "duration", d, "user_id", actor.UserID)
	}
	s.observeStageErr(ctx, "pull", "transaction", txStart, maxBundles, 0, err)
	if err != nil {
		var prunedErr *HistoryPrunedError
		if errors.As(err, &prunedErr) {
			if recordErr := s.recordHistoryPrunedError(ctx); recordErr != nil {
				s.logger.Warn("Failed to record history_pruned event", "error", recordErr, "user_id", actor.UserID, "source_id", actor.SourceID)
			}
		}
		return nil, err
	}
	return txResp, nil
}

func (s *SyncService) processPullQuerier(
	ctx context.Context,
	tx pgx.Tx,
	userID string,
	afterBundleSeq int64,
	maxBundles int,
	targetBundleSeq int64,
) (*PullResponse, error) {
	if err := requireScopeInitializedQuerier(ctx, tx, userID); err != nil {
		return nil, err
	}
	retainedState, err := loadRetainedHistoryStateByUserID(ctx, tx, userID)
	if err != nil {
		return nil, err
	}
	if retainedState == nil {
		return &PullResponse{
			StableBundleSeq: 0,
			Bundles:         []Bundle{},
			HasMore:         false,
		}, nil
	}
	stageStart := s.stageStart()
	if err := enforceRetainedBundleFloor(userID, afterBundleSeq, retainedState.RetainedFloor); err != nil {
		s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
		return nil, err
	}
	if targetBundleSeq > 0 {
		if err := enforceRetainedBundleFloor(userID, targetBundleSeq, retainedState.RetainedFloor); err != nil {
			s.observeStageErr(ctx, "pull", "retained_floor_check", stageStart, 1, 0, err)
			return nil, err
		}
	}
	s.observeStage(ctx, "pull", "retained_floor_check", stageStart, 1, 0, false)

	stableBundleSeq := targetBundleSeq
	if stableBundleSeq <= 0 {
		stageStart = s.stageStart()
		stableBundleSeq = retainedState.highestBundleSeq()
		s.observeStage(ctx, "pull", "resolve_stable_bundle_seq", stageStart, 1, 0, false)
	} else {
		s.observeStage(ctx, "pull", "resolve_stable_bundle_seq", s.stageStart(), 1, 0, false)
	}
	if stableBundleSeq > 0 {
		if err := enforceRetainedBundleFloor(userID, stableBundleSeq, retainedState.RetainedFloor); err != nil {
			return nil, err
		}
	}
	if afterBundleSeq >= stableBundleSeq {
		return &PullResponse{
			StableBundleSeq: stableBundleSeq,
			Bundles:         []Bundle{},
			HasMore:         false,
		}, nil
	}

	stageStart = s.stageStart()
	rows, err := tx.Query(ctx, `
		SELECT bundle_seq
		FROM sync.bundle_log
		WHERE user_pk = $1
		  AND bundle_seq > $2
		  AND bundle_seq > $3
		  AND bundle_seq <= $4
		ORDER BY bundle_seq
		LIMIT $5
	`, retainedState.UserPK, afterBundleSeq, retainedState.RetainedFloor, stableBundleSeq, maxBundles+1)
	if err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, maxBundles, 0, err)
		return nil, fmt.Errorf("query bundle pull page: %w", err)
	}
	defer rows.Close()

	bundleSeqs := make([]int64, 0, maxBundles+1)
	for rows.Next() {
		var bundleSeq int64
		if err := rows.Scan(&bundleSeq); err != nil {
			s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
			return nil, fmt.Errorf("scan pulled bundle sequence: %w", err)
		}
		bundleSeqs = append(bundleSeqs, bundleSeq)
	}
	if err := rows.Err(); err != nil {
		s.observeStageErr(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, err)
		return nil, fmt.Errorf("iterate pulled bundle sequences: %w", err)
	}
	s.observeStage(ctx, "pull", "list_bundle_seqs", stageStart, len(bundleSeqs), 0, false)

	hasMore := false
	if len(bundleSeqs) > maxBundles {
		hasMore = true
		bundleSeqs = bundleSeqs[:maxBundles]
	}

	firstLoadStart := time.Now()
	bundles := make([]Bundle, 0, len(bundleSeqs))
	stageStart = s.stageStart()
	for _, bundleSeq := range bundleSeqs {
		bundle, err := s.loadCommittedBundle(ctx, tx, userID, bundleSeq)
		if err != nil {
			s.observeStageErr(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, err)
			return nil, err
		}
		bundles = append(bundles, *bundle)
	}
	if d := time.Since(firstLoadStart); d > 10*time.Millisecond {
		s.logger.Warn("pull: slow load bundles", "count", len(bundles), "duration", d, "user_id", userID)
	}
	s.observeStage(ctx, "pull", "load_bundles", stageStart, len(bundles), 0, false)

	return &PullResponse{
		StableBundleSeq: stableBundleSeq,
		Bundles:         bundles,
		HasMore:         hasMore,
	}, nil
}

func userHighestBundleSeqQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string) (int64, error) {
	var maxSeq int64
	err := q.QueryRow(ctx, `
		SELECT next_bundle_seq - 1
		FROM sync.user_state
		WHERE user_id = @user_id
	`, pgx.NamedArgs{"user_id": userID}).Scan(&maxSeq)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("query user highest bundle seq: %w", err)
	}
	return maxSeq, nil
}

func enforceRetainedBundleFloorQuerier(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, userID string, providedSeq int64) error {
	state, err := loadRetainedHistoryStateByUserID(ctx, q, userID)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	return enforceRetainedBundleFloor(userID, providedSeq, state.RetainedFloor)
}

func (s *SyncService) recordHistoryPrunedError(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `SELECT nextval('sync.history_pruned_error_seq')`)
	if err != nil {
		return fmt.Errorf("record history_pruned event: %w", err)
	}
	return nil
}

// ensureScopeInitializedForPull checks the scope state and auto-initializes it
// if UNINITIALIZED. Uses a separate context with a longer timeout so the seed
// won't be canceled by the HTTP request timeout.
func (s *SyncService) ensureScopeInitializedForPull(ctx context.Context, actor Actor) error {
	initCtx, initCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer initCancel()

	return pgx.BeginFunc(initCtx, s.pool, func(tx pgx.Tx) error {
		if err := ensureScopeStateExistsWithExec(initCtx, tx, actor.UserID); err != nil {
			return err
		}
		state, err := loadScopeStateForUpdate(initCtx, tx, actor.UserID)
		if err != nil {
			return err
		}
		state, err = expireInitializationLeaseIfNeeded(initCtx, tx, state)
		if err != nil {
			return err
		}
		if state.State != scopeStateUninitialized {
			return nil
		}

		if s.config != nil && s.config.AutoSeedInitialBundle {
			seeded, seedErr := s.seedInitialBundle(initCtx, tx, actor, state.UserPK, actor.UserID)
			if seedErr != nil {
				s.logger.Warn("auto-seed initial bundle failed during pull init",
					"user_id", actor.UserID, "error", seedErr)
			} else if seeded {
				s.logger.Info("auto-seeded initial bundle from existing data during pull init",
					"user_id", actor.UserID)
			}
		}

		return transitionScopeToInitialized(initCtx, tx, actor.UserID, actor.SourceID)
	})
}
