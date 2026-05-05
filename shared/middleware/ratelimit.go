package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

type ipEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type ipLimiter struct {
	mu       sync.Mutex
	limiters map[string]*ipEntry
}

func (il *ipLimiter) get(ip string) *rate.Limiter {
	il.mu.Lock()
	defer il.mu.Unlock()
	if e, ok := il.limiters[ip]; ok {
		e.lastSeen = time.Now()
		return e.limiter
	}
	l := rate.NewLimiter(100, 50)
	il.limiters[ip] = &ipEntry{limiter: l, lastSeen: time.Now()}
	return l
}

func (il *ipLimiter) cleanup() {
	il.mu.Lock()
	defer il.mu.Unlock()
	cutoff := time.Now().Add(-5 * time.Minute)
	for ip, e := range il.limiters {
		if e.lastSeen.Before(cutoff) {
			delete(il.limiters, ip)
		}
	}
}

// RateLimit returns a Gin middleware that enforces 100 req/s burst 50 per IP.
// Idle IP entries are evicted after 5 minutes to prevent unbounded map growth.
func RateLimit() gin.HandlerFunc {
	il := &ipLimiter{limiters: make(map[string]*ipEntry)}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			il.cleanup()
		}
	}()
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
