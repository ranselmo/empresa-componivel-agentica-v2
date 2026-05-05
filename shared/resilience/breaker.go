package resilience

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker"
)

var breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "circuit_breaker_state", Help: "0=closed 1=half-open 2=open",
}, []string{"component", "shard", "breaker"})

type Breaker struct{ cb *gobreaker.CircuitBreaker }

func NewBreaker(component, shardID, name string) *Breaker {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 3
		},
		OnStateChange: func(n string, from, to gobreaker.State) {
			slog.Warn("circuit breaker state change",
				"breaker", n, "from", from, "to", to,
				"component", component, "shard", shardID)
			breakerState.WithLabelValues(component, shardID, n).Set(float64(to))
		},
	})
	return &Breaker{cb: cb}
}

func (b *Breaker) Execute(fn func() error) error {
	_, err := b.cb.Execute(func() (interface{}, error) { return nil, fn() })
	return err
}
