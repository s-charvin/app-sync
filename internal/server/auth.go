package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDCtxKey contextKey = "user_id"

// UserIDFromContext extracts the authenticated user's ID from the context.
// Used by oversync.ActorMiddleware to map JWT identity to sync scope.
func UserIDFromContext(ctx context.Context) (string, error) {
	uid, ok := ctx.Value(userIDCtxKey).(string)
	if !ok || uid == "" {
		return "", fmt.Errorf("user_id not found in request context")
	}
	return uid, nil
}

// jwtAuthMiddleware validates the JWT Bearer token and injects user_id into the request context.
// Supports both HS256 (anon/service_role keys with JWT secret) and ES256 (user tokens via JWKS).
func jwtAuthMiddleware(jwtSecret []byte, supabaseURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing or malformed authorization header"})
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")

		// Parse without verification first to check the signing method
		unverified, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
		if err != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		var claims jwt.MapClaims
		switch unverified.Method.Alg() {
		case "HS256":
			token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return jwtSecret, nil
			})
			if err != nil {
				c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
				return
			}
			claims = token.Claims.(jwt.MapClaims)

		case "ES256":
			kid, _ := unverified.Header["kid"].(string)
			if kid == "" {
				c.AbortWithStatusJSON(401, gin.H{"error": "token missing kid header"})
				return
			}
			publicKey, err := getSupabasePublicKey(supabaseURL, kid)
			if err != nil {
				c.AbortWithStatusJSON(401, gin.H{"error": "unable to verify token"})
				return
			}
			token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return publicKey, nil
			})
			if err != nil {
				c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
				return
			}
			claims = token.Claims.(jwt.MapClaims)

		default:
			c.AbortWithStatusJSON(401, gin.H{"error": fmt.Sprintf("unsupported signing method: %s", unverified.Method.Alg())})
			return
		}

		userID, _ := claims["sub"].(string)
		if userID == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "token missing sub claim"})
			return
		}

		ctx := context.WithValue(c.Request.Context(), userIDCtxKey, userID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// --- JWKS verification for Supabase ES256 tokens ---

type jwksCacheEntry struct {
	key       *ecdsa.PublicKey
	fetchedAt time.Time
}

var (
	jwksCacheMu sync.RWMutex
	jwksCache   map[string]*jwksCacheEntry
	httpClient  = &http.Client{Timeout: 10 * time.Second}
)

const jwksCacheTTL = 15 * time.Minute

type jwksResponse struct {
	Keys []jwksKeyData `json:"keys"`
}

type jwksKeyData struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func getSupabasePublicKey(supabaseURL, kid string) (*ecdsa.PublicKey, error) {
	// Check cache first
	jwksCacheMu.RLock()
	if entry, ok := jwksCache[kid]; ok && time.Since(entry.fetchedAt) < jwksCacheTTL {
		jwksCacheMu.RUnlock()
		return entry.key, nil
	}
	jwksCacheMu.RUnlock()

	// Fetch JWKS
	jwksURL := strings.TrimRight(supabaseURL, "/") + "/auth/v1/.well-known/jwks.json"
	resp, err := httpClient.Get(jwksURL)
	if err != nil {
		return nil, fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()

	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("decode jwks: %w", err)
	}

	// Cache all keys, return the one matching kid
	jwksCacheMu.Lock()
	if jwksCache == nil {
		jwksCache = make(map[string]*jwksCacheEntry)
	}
	now := time.Now()
	for _, keyData := range jwks.Keys {
		if keyData.Kty != "EC" || keyData.Crv != "P-256" {
			continue
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(keyData.X)
		if err != nil {
			continue
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(keyData.Y)
		if err != nil {
			continue
		}
		pubKey := &ecdsa.PublicKey{
			Curve: elliptic.P256(),
			X:     new(big.Int).SetBytes(xBytes),
			Y:     new(big.Int).SetBytes(yBytes),
		}
		entry := &jwksCacheEntry{key: pubKey, fetchedAt: now}
		jwksCache[keyData.Kid] = entry
	}
	entry, ok := jwksCache[kid]
	jwksCacheMu.Unlock()

	if !ok {
		return nil, fmt.Errorf("no matching JWKS key found for kid: %s", kid)
	}
	return entry.key, nil
}
