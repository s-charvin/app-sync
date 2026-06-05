package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"

	"github.com/s-charvin/app-sync/internal/config"
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
		AutoSeedInitialBundle:    true,
		MaxRowsPerInitialSeed:    100000,
	}

	svc, err := oversync.NewRuntimeService(pool, svcCfg, logger)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("sync service: %w", err)
	}

	if err := svc.Bootstrap(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	sh := oversync.NewHTTPSyncHandlers(svc, logger)

	actorMW := oversync.ActorMiddleware(oversync.ActorMiddlewareConfig{
		UserIDFromContext: UserIDFromContext,
	})

	syncMux := newSyncMux(sh)
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
	validator := NewJWTValidator(cfg.JWKSURL())
	if err := validator.FetchKeys(ctx); err != nil {
		return nil, fmt.Errorf("jwt validator: %w", err)
	}
	auth.Use(jwtAuthMiddleware(validator))
	auth.Use(jwtAuthMiddleware([]byte(cfg.JWTSecret), cfg.SupabaseURL))
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

func newSyncMux(sh *oversync.HTTPSyncHandlers) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /sync/connect", sh.HandleConnect)
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
