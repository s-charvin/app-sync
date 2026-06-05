package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserIDFromContext(t *testing.T) {
	t.Run("returns user ID when present", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), userIDCtxKey, "user-123")
		uid, err := UserIDFromContext(ctx)
		require.NoError(t, err)
		assert.Equal(t, "user-123", uid)
	})

	t.Run("returns error when user ID missing", func(t *testing.T) {
		ctx := context.Background()
		_, err := UserIDFromContext(ctx)
		assert.ErrorContains(t, err, "user_id not found")
	})

	t.Run("returns error when user ID is empty", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), userIDCtxKey, "")
		_, err := UserIDFromContext(ctx)
		assert.ErrorContains(t, err, "user_id not found")
	})

	t.Run("returns error when context key type mismatch", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), userIDCtxKey, 12345)
		_, err := UserIDFromContext(ctx)
		assert.ErrorContains(t, err, "user_id not found")
	})
}

func TestJWTAuthMiddleware(t *testing.T) {
	t.Run("rejects missing authorization header", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/sync/pull", nil)

		middleware := jwtAuthMiddleware([]byte("secret"), "https://test.supabase.co")
		middleware(c)

		assert.Equal(t, 401, w.Code)
		assert.Contains(t, w.Body.String(), "missing or malformed")
	})

	t.Run("rejects malformed authorization header", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/sync/pull", nil)
		c.Request.Header.Set("Authorization", "Basic token123")

		middleware := jwtAuthMiddleware([]byte("secret"), "https://test.supabase.co")
		middleware(c)

		assert.Equal(t, 401, w.Code)
		assert.Contains(t, w.Body.String(), "missing or malformed")
	})

	t.Run("rejects random token string", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/sync/pull", nil)
		c.Request.Header.Set("Authorization", "Bearer this.is.not.a.valid.jwt")

		middleware := jwtAuthMiddleware([]byte("secret"), "https://test.supabase.co")
		middleware(c)

		assert.Equal(t, 401, w.Code)
	})

	t.Run("rejects valid HS256 JWT with wrong signature", func(t *testing.T) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/sync/pull", nil)
		token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLTEyMyJ9.wrongsignature"
		c.Request.Header.Set("Authorization", "Bearer "+token)

		middleware := jwtAuthMiddleware([]byte("real-secret"), "https://test.supabase.co")
		middleware(c)

		assert.Equal(t, 401, w.Code)
	})
}

func TestNewServerHTTPEndpoints(t *testing.T) {
	t.Run("health endpoint returns 200", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.GET("/health", func(c *gin.Context) {
			c.JSON(200, gin.H{"status": "ok", "app": "app-sync-test"})
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/health", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, 200, w.Code)
		assert.Contains(t, w.Body.String(), "ok")
		assert.Contains(t, w.Body.String(), "app-sync-test")
	})

	t.Run("unregistered route returns 404", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		r.GET("/health", func(c *gin.Context) {
			c.JSON(200, gin.H{"status": "ok"})
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/nonexistent", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, 404, w.Code)
	})

	t.Run("sync routes require auth", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		r := gin.New()
		auth := r.Group("")
		auth.Use(jwtAuthMiddleware([]byte("secret"), "https://test.supabase.co"))
		auth.Any("/sync/*path", func(c *gin.Context) {
			c.JSON(200, gin.H{"ok": true})
		})

		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/sync/pull", nil)
		r.ServeHTTP(w, req)

		assert.Equal(t, 401, w.Code)
	})
}
