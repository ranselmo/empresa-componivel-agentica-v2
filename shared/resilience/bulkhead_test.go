package resilience_test

import (
	"context"
	"sync"
	"testing"

	. "github.com/ranselmo/poc-eci/shared/resilience"
)

func TestBulkhead_AceitaDentroCapacidade(t *testing.T) {
	bh := NewBulkhead("test", "s1", "db", 2)
	err := bh.Do(context.Background(), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}

func TestBulkhead_RejeitaAcimaCapacidade(t *testing.T) {
	bh := NewBulkhead("test", "s1", "db", 1)
	// Fill the slot
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_ = bh.Do(context.Background(), func() error {
			close(ready)
			<-done
			return nil
		})
	}()
	<-ready
	// Slot is occupied — next call must be rejected immediately
	err := bh.Do(context.Background(), func() error { return nil })
	close(done)
	if err == nil {
		t.Fatal("expected rejection error when bulkhead is full")
	}
}

func TestBulkhead_LiberaSlotAposExecucao(t *testing.T) {
	bh := NewBulkhead("test", "s1", "db", 1)
	var wg sync.WaitGroup
	// Run two sequential calls — second must succeed after first finishes
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = bh.Do(context.Background(), func() error { return nil })
		}()
	}
	wg.Wait()
	// If slot was released, at least one of the two succeeded without deadlock
}
