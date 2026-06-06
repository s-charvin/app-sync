package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"

	"github.com/s-charvin/app-sync/internal/config"
	"github.com/s-charvin/app-sync/internal/seed"
	"github.com/s-charvin/app-sync/internal/table"
)

func New(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*http.Server, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database pool: %w", err)
	}

	svcCfg := &oversync.ServiceConfig{
		AppName:                  cfg.AppName,
		MaxSupportedSchemaVersion: 1,
		RegisteredTables:         table.RegisteredTables(),
	}

	svc, err := oversync.NewRuntimeService(pool, svcCfg, logger)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("sync service: %w", err)
	}

	if err := svc.Bootstrap(ctx); err != nil {
		if strings.Contains(err.Error(), "must be owner of relation") || strings.Contains(err.Error(), "SQLSTATE 42501") {
			logger.Warn("bootstrap: skipping trigger re-install (insufficient table ownership), triggers already exist from prior deploy",
				"detail", err.Error())
		} else {
			pool.Close()
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	sh := oversync.NewHTTPSyncHandlers(svc, logger)

	actorMW := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: UserIDFromContext,
	})

	syncMux := newSyncMux(sh, pool, logger)
	protectedHandler := actorMW(syncMux)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger(logger))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "app": cfg.AppName})
	})

	r.GET("/syncx/health", gin.WrapH(http.HandlerFunc(sh.HandleHealth)))
	r.GET("/syncx/status", gin.WrapH(http.HandlerFunc(sh.HandleStatus)))

	auth := r.Group("")
	auth.Use(jwtAuthMiddleware([]byte(cfg.JWTSecret), cfg.SupabaseURL))

	auth.POST("/syncx/seed", func(c *gin.Context) {
		userID, _ := c.Request.Context().Value(userIDCtxKey).(string)
		if userID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing user_id"})
			return
		}
		if err := seed.SystemData(c.Request.Context(), pool, userID, logger); err != nil {
			logger.Error("seed system data failed", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "seeded"})
	})

	auth.Any("/sync/*path", gin.WrapH(protectedHandler))

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return srv, nil
}

func newSyncMux(sh *oversync.HTTPSyncHandlers, pool *pgxpool.Pool, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sync/connect", autoSeedConnect(sh.HandleConnect, pool, logger))
	mux.HandleFunc("POST /sync/push-sessions", sh.HandleCreatePushSession)
	mux.HandleFunc("POST /sync/push-sessions/{push_id}/chunks", sh.HandlePushSessionChunk)
	mux.HandleFunc("POST /sync/push-sessions/{push_id}/commit", sh.HandleCommitPushSession)
	mux.HandleFunc("DELETE /sync/push-sessions/{push_id}", sh.HandleDeletePushSession)
	mux.HandleFunc("GET /sync/committed-bundles/{bundle_seq}/rows", sh.HandleGetCommittedBundleRows)
	mux.HandleFunc("GET /sync/pull", sh.HandlePull)
	mux.HandleFunc("POST /sync/snapshot-sessions", sh.HandleCreateSnapshotSession)
	mux.HandleFunc("GET /sync/snapshot-sessions/{snapshot_id}", sh.HandleGetSnapshotChunk)
	mux.HandleFunc("DELETE /sync/snapshot-sessions/{snapshot_id}", sh.HandleDeleteSnapshotSession)
	mux.HandleFunc("GET /sync/capabilities", sh.HandleCapabilities)
	return mux
}

type responseBuffer struct {
	header     http.Header
	body       bytes.Buffer
	statusCode int
}

func (r *responseBuffer) Header() http.Header { return r.header }
func (r *responseBuffer) Write(b []byte) (int, error) {
	return r.body.Write(b)
}
func (r *responseBuffer) WriteHeader(statusCode int) { r.statusCode = statusCode }

func autoSeedConnect(original http.HandlerFunc, pool *pgxpool.Pool, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &responseBuffer{header: make(http.Header), statusCode: http.StatusOK}
		original(rec, r)

		userID, _ := r.Context().Value(userIDCtxKey).(string)
		if userID == "" {
			logger.Warn("auto-seed: no user_id in connect context")
		} else if rec.statusCode != http.StatusOK {
			logger.Warn("auto-seed: connect returned non-OK status, skipping seed check",
				"user_id", userID, "connect_status", rec.statusCode)
		} else {
			var resp struct{ Resolution string }
			if err := json.Unmarshal(rec.body.Bytes(), &resp); err != nil {
				logger.Error("auto-seed: failed to parse connect response",
					"user_id", userID, "error", err)
			} else {
				logger.Info("auto-seed: connect completed",
					"user_id", userID, "resolution", resp.Resolution)

				if resp.Resolution == "initialize_empty" || resp.Resolution == "remote_authoritative" {
					nextSeq, hasBundles := userBundleState(r.Context(), pool, userID)
					if !hasBundles {
						logger.Info("auto-seed: user has no bundles, seeding system data",
							"user_id", userID, "next_bundle_seq", nextSeq, "resolution", resp.Resolution)
						if err := seed.SystemData(r.Context(), pool, userID, logger); err != nil {
							logger.Error("auto-seed: seed failed",
								"user_id", userID, "error", err)
						}
					} else {
						logger.Info("auto-seed: user already has bundles, skipping",
							"user_id", userID, "next_bundle_seq", nextSeq)
					}
				} else {
					logger.Info("auto-seed: resolution does not require seed check",
						"user_id", userID, "resolution", resp.Resolution)
				}
			}
		}

		for k, v := range rec.header {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.statusCode)
		io.Copy(w, &rec.body)
	}
}

func userBundleState(ctx context.Context, pool *pgxpool.Pool, userID string) (nextBundleSeq int64, hasNoBundles bool) {
	err := pool.QueryRow(ctx,
		`SELECT next_bundle_seq FROM sync.user_state WHERE user_id = $1`, userID,
	).Scan(&nextBundleSeq)
	if err != nil {
		return 0, false
	}
	return nextBundleSeq, nextBundleSeq <= 1
}

func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		logger.Info("request",
			"method", c.Request.Method,
			"path", path,
			"status", c.Writer.Status(),
			"latency", time.Since(start).String(),
			"client_ip", c.ClientIP(),
		)
	}
}
