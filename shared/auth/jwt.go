package auth

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// Middleware validates Bearer JWT against JWKS_URL and sets actor_id/roles in context.
// When JWKS_URL is unset (dev mode), passes through without validation.
func Middleware() gin.HandlerFunc {
	jwksURL := os.Getenv("JWKS_URL")
	if jwksURL == "" {
		return func(c *gin.Context) { c.Next() }
	}

	cache := jwk.NewCache(context.Background())
	_ = cache.Register(jwksURL, jwk.WithMinRefreshInterval(15*time.Minute))

	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}
		tokenStr := strings.TrimPrefix(header, "Bearer ")

		ks, err := cache.Get(c.Request.Context(), jwksURL)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "cannot fetch JWKS"})
			return
		}

		tok, err := jwt.Parse([]byte(tokenStr), jwt.WithKeySet(ks))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		roles, _ := tok.Get("roles")
		c.Set("actor_id", tok.Subject())
		c.Set("roles", roles)
		c.Next()
	}
}
