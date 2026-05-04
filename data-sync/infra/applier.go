package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ranselmo/poc-eci/data-sync/infra/resilience"
)

var (
	applied = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "data_sync_applied_total"},
		[]string{"shard", "pbc", "op"})
	syncErr = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "data_sync_errors_total"},
		[]string{"shard", "pbc"})
)

type debeziumMsg struct {
	Payload struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Op     string         `json:"op"`
		Source struct {
			Table string `json:"table"`
		} `json:"source"`
	} `json:"payload"`
}

type Applier struct {
	c        *kafka.Consumer
	pools    map[string]*pgxpool.Pool   // "shard-1:pedidos" → pool da passiva
	breakers map[string]*resilience.Breaker // mesma chave
}

func NewApplier(passiveDSNs map[string]string) (*Applier, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "data-sync-cdc-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		return nil, err
	}

	prefix := os.Getenv("CDC_TOPIC_PREFIX")
	if prefix == "" {
		prefix = "cdc"
	}
	var topics []string
	shards := []string{"shard-1", "shard-2", "shard-3"}
	pbcs := map[string][]string{
		"pedidos":      {"pedidos"},
		"estoque":      {"produtos"},
		"notificacoes": {"notificacoes"},
	}
	for _, shard := range shards {
		for pbc, tables := range pbcs {
			for _, table := range tables {
				topics = append(topics, fmt.Sprintf("%s.%s.%s.%s",
					prefix, shard, pbc, table))
			}
		}
	}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}

	pools := make(map[string]*pgxpool.Pool)
	breakers := make(map[string]*resilience.Breaker)
	for key, dsn := range passiveDSNs {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			slog.Warn("passive pool failed", "key", key, "err", err)
			continue
		}
		pools[key] = pool
		breakers[key] = resilience.NewBreaker("data-sync", key, "db-passive")
	}
	return &Applier{c: c, pools: pools, breakers: breakers}, nil
}

func (a *Applier) Run(ctx context.Context) {
	slog.Info("data-sync started", "pools", len(a.pools))
	for {
		select {
		case <-ctx.Done():
			a.c.Close()
			return
		default:
			msg, err := a.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(strings.ToLower(err.Error()), "timed out") {
					slog.Error("read", "err", err)
				}
				continue
			}
			a.apply(ctx, msg)
		}
	}
}

func (a *Applier) apply(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic
	parts := strings.SplitN(topic, ".", 4)
	if len(parts) < 4 {
		return
	}
	shard, pbc := parts[1], parts[2]
	key := shard + ":" + pbc

	pool, ok := a.pools[key]
	if !ok {
		slog.Debug("no passive pool", "key", key)
		return
	}
	breaker := a.breakers[key]

	var dm debeziumMsg
	if err := json.Unmarshal(msg.Value, &dm); err != nil {
		syncErr.WithLabelValues(shard, pbc).Inc()
		slog.Error("unmarshal debezium", "err", err)
		return
	}

	table := dm.Payload.Source.Table
	var err error
	switch dm.Payload.Op {
	case "c", "r":
		err = applyWithResilience(ctx, breaker, func() error {
			return applyInsert(ctx, pool, table, dm.Payload.After)
		})
	case "u":
		err = applyWithResilience(ctx, breaker, func() error {
			return applyUpdate(ctx, pool, table, dm.Payload.Before, dm.Payload.After)
		})
	case "d":
		err = applyWithResilience(ctx, breaker, func() error {
			return applyDelete(ctx, pool, table, dm.Payload.Before)
		})
	}

	if err != nil {
		syncErr.WithLabelValues(shard, pbc).Inc()
		slog.Error("apply", "shard", shard, "pbc", pbc, "table", table, "err", err)
		return
	}
	applied.WithLabelValues(shard, pbc, dm.Payload.Op).Inc()
}

func applyWithResilience(ctx context.Context, breaker *resilience.Breaker, fn func() error) error {
	return resilience.Retry(ctx, 3, 100*time.Millisecond, func() error {
		return breaker.Execute(fn)
	})
}

func applyInsert(ctx context.Context, pool *pgxpool.Pool, table string, row map[string]any) error {
	if len(row) == 0 {
		return nil
	}
	cols, vals, placeholders := insertParts(row)
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO NOTHING",
		table, cols, placeholders)
	_, err := pool.Exec(ctx, q, vals...)
	return err
}

func applyUpdate(ctx context.Context, pool *pgxpool.Pool, table string, before, after map[string]any) error {
	if len(after) == 0 || before["id"] == nil {
		return nil
	}
	set, vals := setParts(after)
	if set == "" {
		return nil
	}
	q := fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d", table, set, len(vals)+1)
	_, err := pool.Exec(ctx, q, append(vals, before["id"])...)
	return err
}

func applyDelete(ctx context.Context, pool *pgxpool.Pool, table string, before map[string]any) error {
	if before["id"] == nil {
		return nil
	}
	_, err := pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=$1", table), before["id"])
	return err
}

func insertParts(row map[string]any) (cols string, vals []any, placeholders string) {
	var cs, ps []string
	i := 1
	for k, v := range row {
		cs = append(cs, k)
		vals = append(vals, v)
		ps = append(ps, fmt.Sprintf("$%d", i))
		i++
	}
	return strings.Join(cs, ","), vals, strings.Join(ps, ",")
}

func setParts(row map[string]any) (string, []any) {
	var parts []string
	var vals []any
	i := 1
	for k, v := range row {
		if k == "id" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=$%d", k, i))
		vals = append(vals, v)
		i++
	}
	return strings.Join(parts, ","), vals
}
