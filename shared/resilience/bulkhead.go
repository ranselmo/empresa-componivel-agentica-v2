package resilience

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var bulkheadRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bulkhead_rejected_total",
}, []string{"component", "shard", "name"})

type Bulkhead struct {
	sem       chan struct{}
	component string
	shard     string
	name      string
}

func NewBulkhead(component, shard, name string, max int) *Bulkhead {
	return &Bulkhead{sem: make(chan struct{}, max),
		component: component, shard: shard, name: name}
}

func (b *Bulkhead) Do(ctx context.Context, fn func() error) error {
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
		return fn()
	case <-ctx.Done():
		return ctx.Err()
	default:
		bulkheadRejected.WithLabelValues(b.component, b.shard, b.name).Inc()
		return fmt.Errorf("bulkhead %s/%s: capacity exceeded", b.component, b.name)
	}
}
