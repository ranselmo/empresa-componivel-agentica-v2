package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/ranselmo/poc-eci/shared/resilience"
)

var errTest = errors.New("test error")

func TestRetry_SuccedeNaPrimeira(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetry_SuccedeNaTerceira(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errTest
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_EsgotaTentativas(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return errTest
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_RespeitaCtxCancelado(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	// cancel immediately
	cancel()
	err := Retry(ctx, 5, time.Millisecond, func() error {
		calls++
		return errTest
	})
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	// first call always runs; subsequent ones should be stopped by ctx
	if calls > 2 {
		t.Fatalf("expected ≤2 calls with cancelled ctx, got %d", calls)
	}
}
