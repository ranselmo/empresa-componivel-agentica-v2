package messaging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/resilience"
)

const (
	TopicCmdCriar        = "commands.pedidos.criar"
	TopicCmdCancelar     = "commands.pedidos.cancelar"
	TopicReplyCriado     = "replies.pedidos.criado"
	TopicReplyCancelado  = "replies.pedidos.cancelado"
	TopicEventConfirmado = "events.pedidos.confirmado"
)

type Reply struct {
	ReplyID       uuid.UUID      `json:"reply_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	CommandType   string         `json:"command_type"`
	Status        string         `json:"status"`
	Payload       map[string]any `json:"payload"`
	Error         string         `json:"error,omitempty"`
	RepliedAt     time.Time      `json:"replied_at"`
}

type Producer struct {
	p       *kafka.Producer
	breaker *resilience.Breaker
}

func NewProducer() (*Producer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return nil, fmt.Errorf("producer: %w", err)
	}
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery", "err", m.TopicPartition.Error)
			}
		}
	}()
	shardID := os.Getenv("SHARD_ID")
	return &Producer{
		p:       p,
		breaker: resilience.NewBreaker("cell-pedidos", shardID, "kafka-producer"),
	}, nil
}

func (pr *Producer) PublishReply(topic string, reply Reply) error {
	b, _ := json.Marshal(reply)
	key := reply.CorrelationID.String()
	return pr.breaker.Execute(func() error {
		return pr.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key), Value: b,
		}, nil)
	})
}

func (pr *Producer) PublishBusinessEvent(topic string, payload map[string]any) {
	ev := map[string]any{
		"event_id":    uuid.New().String(),
		"occurred_at": time.Now().UTC(),
		"payload":     payload,
	}
	b, _ := json.Marshal(ev)
	id := uuid.New().String()
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(id), Value: b,
	}, nil)
}

func (pr *Producer) KafkaProducer() *kafka.Producer { return pr.p }
func (pr *Producer) Flush()                         { pr.p.Flush(5000) }
func (pr *Producer) Close()                         { pr.p.Flush(5000); pr.p.Close() }
