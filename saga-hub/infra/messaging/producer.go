package messaging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
)

type Producer struct{ p *kafka.Producer }

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
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery failed", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) PublishCommand(topic string, cmd domain.Command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	key := cmd.CorrelationID.String()
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          b,
	}, nil)
}

func (pr *Producer) PublishBusinessEvent(topic string, ev domain.BusinessEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		slog.Error("marshal business event", "err", err)
		return
	}
	key := ev.EventID.String()
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          b,
	}, nil)
}

func (pr *Producer) Flush() { pr.p.Flush(5000) }
func (pr *Producer) Close() { pr.p.Close() }
