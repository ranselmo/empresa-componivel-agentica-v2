package middleware

import (
	"net"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type ipLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

func (il *ipLimiter) get(ip string) *rate.Limiter {
	il.mu.Lock()
	defer il.mu.Unlock()
	if l, ok := il.limiters[ip]; ok {
		return l
	}
	l := rate.NewLimiter(100, 50)
	il.limiters[ip] = l
	return l
}

// RateLimit returns a Gin middleware that enforces 100 req/s burst 50 per IP.
func RateLimit() gin.HandlerFunc {
	il := &ipLimiter{limiters: make(map[string]*rate.Limiter)}
	return func(c *gin.Context) {
		ip, _, err := net.SplitHostPort(c.Request.RemoteAddr)
		if err != nil {
			ip = c.Request.RemoteAddr
		}
		if !il.get(ip).Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}
