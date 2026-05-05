package messaging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/shared/monitoring"
	"github.com/ranselmo/poc-eci/shared/resilience"
)

const (
	TopicCmdEnviar    = "commands.notificacoes.enviar"
	TopicReplyEnviada = "replies.notificacoes.enviada"
	TopicEventEnviada = "events.notificacoes.enviada"
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
	shardID string
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
		breaker: resilience.NewBreaker("cell-notificacoes", shardID, "kafka-producer"),
		shardID: shardID,
	}, nil
}

func (pr *Producer) PublishReply(reply Reply) error {
	topic := TopicReplyEnviada
	b, _ := json.Marshal(reply)
	err := pr.breaker.Execute(func() error {
		return pr.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(reply.CorrelationID.String()),
			Value:          b,
		}, nil)
	})
	monitoring.KafkaMessages.WithLabelValues("cell-notificacoes", pr.shardID, topic, "publish").Inc()
	return err
}

func (pr *Producer) PublishEvent(tipo string, payload map[string]any) {
	topic := TopicEventEnviada
	ev, _ := json.Marshal(map[string]any{
		"event_id": uuid.New().String(), "tipo": tipo,
		"occurred_at": time.Now().UTC(), "payload": payload,
	})
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          ev,
	}, nil)
}

func (pr *Producer) KafkaProducer() *kafka.Producer { return pr.p }
func (pr *Producer) Flush()                         { pr.p.Flush(5000) }
func (pr *Producer) Close()                         { pr.p.Flush(5000); pr.p.Close() }
