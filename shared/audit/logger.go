package audit

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type Event struct {
	ID           uuid.UUID      `json:"id"`
	Component    string         `json:"component"`
	ShardID      string         `json:"shard_id"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	ActorID      string         `json:"actor_id"`
	Payload      map[string]any `json:"payload"`
	OccurredAt   time.Time      `json:"occurred_at"`
}

type Logger struct {
	p         *kafka.Producer
	component string
	shardID   string
}

func New(component, shardID string, p *kafka.Producer) *Logger {
	return &Logger{p: p, component: component, shardID: shardID}
}

func (l *Logger) Log(_ context.Context, action, resourceType, resourceID, actorID string, payload map[string]any) {
	ev := Event{
		ID: uuid.New(), Component: l.component, ShardID: l.shardID,
		Action: action, ResourceType: resourceType, ResourceID: resourceID,
		ActorID: actorID, Payload: payload, OccurredAt: time.Now().UTC(),
	}
	b, _ := json.Marshal(ev)
	topic := "audit.events"
	_ = l.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(ev.ID.String()),
		Value:          b,
	}, nil)
	slog.Info("audit", "action", action, "resource", resourceType, "id", resourceID)
}
