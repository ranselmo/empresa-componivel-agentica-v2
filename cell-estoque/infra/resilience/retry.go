package resilience

import (
	"context"
	"math/rand/v2"
	"time"
)

func Retry(ctx context.Context, maxAttempts int, base time.Duration, fn func() error) error {
	delay := base
	var lastErr error
	for i := range maxAttempts {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if i == maxAttempts-1 {
			break
		}
		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay + jitter):
		}
		delay = min(delay*2, 5*time.Second)
	}
	return lastErr
}
