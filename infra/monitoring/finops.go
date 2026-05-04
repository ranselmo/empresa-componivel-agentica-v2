package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TxCost = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cell_transaction_cost_cents",
		Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
	}, []string{"component", "shard", "operation"})

	DBQueries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cell_db_queries_total",
	}, []string{"component", "shard", "operation"})

	KafkaMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cell_kafka_messages_total",
	}, []string{"component", "shard", "topic", "direction"})
)
