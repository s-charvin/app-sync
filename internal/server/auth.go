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

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDCtxKey contextKey = "user_id"

// JWK represents a single JSON Web Key from a JWKS response.
type JWK struct {
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// JWKS represents a JWKS key set response.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWTValidator validates Supabase JWTs using the project's JWKS endpoint.
type JWTValidator struct {
	mu      sync.RWMutex
	pubKey  *ecdsa.PublicKey
	jwksURL string
}

func NewJWTValidator(jwksURL string) *JWTValidator {
	return &JWTValidator{jwksURL: jwksURL}
}

// FetchKeys fetches and caches the ECDSA public key from the JWKS endpoint.
func (v *JWTValidator) FetchKeys(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return fmt.Errorf("jwks request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("jwks fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks fetch: unexpected status %d", resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("jwks decode: %w", err)
	}

	if len(jwks.Keys) == 0 {
		return fmt.Errorf("jwks: no keys found")
	}

	pubKey, err := jwks.Keys[0].PublicKey()
	if err != nil {
		return fmt.Errorf("jwks parse key: %w", err)
	}

	v.mu.Lock()
	v.pubKey = pubKey
	v.mu.Unlock()
	return nil
}

func (k JWK) PublicKey() (*ecdsa.PublicKey, error) {
	xBytes, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, fmt.Errorf("decode x: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, fmt.Errorf("decode y: %w", err)
	}

	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}

// ValidateToken validates a JWT token string against the cached JWKS public key.
func (v *JWTValidator) ValidateToken(tokenString string) (*jwt.MapClaims, error) {
	v.mu.RLock()
	pubKey := v.pubKey
	v.mu.RUnlock()

	if pubKey == nil {
		return nil, fmt.Errorf("jwks not initialized: call FetchKeys first")
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return pubKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("token validation: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	return &claims, nil
}

// UserIDFromContext extracts the authenticated user's ID from the context.
func UserIDFromContext(ctx context.Context) (string, error) {
	uid, ok := ctx.Value(userIDCtxKey).(string)
	if !ok || uid == "" {
		return "", fmt.Errorf("user_id not found in request context")
	}
	return uid, nil
}

// jwtAuthMiddleware validates the JWT Bearer token via JWKS and injects user_id.
func jwtAuthMiddleware(validator *JWTValidator) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing or malformed authorization header"})
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := validator.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		userID, _ := (*claims)["sub"].(string)
		if userID == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "token missing sub claim"})
			return
		}

		ctx := context.WithValue(c.Request.Context(), userIDCtxKey, userID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
