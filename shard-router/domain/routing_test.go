package domain_test

import (
	"fmt"
	"testing"

	. "github.com/ranselmo/poc-eci/shard-router/domain"
)

func TestRoute_Determinismo(t *testing.T) {
	key := "cliente-abc-123"
	first := Route(key)
	for i := range 100 {
		if got := Route(key); got != first {
			t.Fatalf("iteration %d: expected %s, got %s", i, first, got)
		}
	}
}

func TestRoute_Formato(t *testing.T) {
	valid := map[string]bool{"shard-1": true, "shard-2": true, "shard-3": true}
	for i := range 50 {
		key := fmt.Sprintf("key-%d", i)
		shard := Route(key)
		if !valid[shard] {
			t.Fatalf("unexpected shard %q for key %q", shard, key)
		}
	}
}

func TestRoute_Distribuicao(t *testing.T) {
	counts := map[string]int{}
	n := 300
	for i := range n {
		counts[Route(fmt.Sprintf("cliente-%d", i))]++
	}
	if len(counts) != TotalShards {
		t.Fatalf("expected %d distinct shards, got %d: %v", TotalShards, len(counts), counts)
	}
	// each shard should get at least 5% of keys (very loose bound)
	minExpected := n / (TotalShards * 10)
	for shard, c := range counts {
		if c < minExpected {
			t.Fatalf("shard %s received only %d/%d keys — poor distribution", shard, c, n)
		}
	}
}
