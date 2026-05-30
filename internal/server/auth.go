package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDCtxKey contextKey = "user_id"

// jwtAuthMiddleware validates the JWT Bearer token and injects user_id into the request context.
func jwtAuthMiddleware(secret []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing or malformed authorization header"})
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return secret, nil
		})
		if err != nil {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok || !token.Valid {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token claims"})
			return
		}

		userID, _ := claims["sub"].(string)
		if userID == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "token missing user_id claim"})
			return
		}

		ctx := context.WithValue(c.Request.Context(), userIDCtxKey, userID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// UserIDFromContext extracts the authenticated user's ID from the context.
// Used by oversync.ActorMiddleware to map JWT identity to sync scope.
func UserIDFromContext(ctx context.Context) (string, error) {
	uid, ok := ctx.Value(userIDCtxKey).(string)
	if !ok || uid == "" {
		return "", fmt.Errorf("user_id not found in request context")
	}
	return uid, nil
}
