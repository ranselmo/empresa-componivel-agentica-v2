package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

// Tópicos exclusivos do PBC estoque
const (
	TopicCmdReservar    = "commands.estoque.reservar"
	TopicCmdLiberar     = "commands.estoque.liberar"
	TopicReplyReservado    = "replies.estoque.reservado"
	TopicReplyInsuficiente = "replies.estoque.insuficiente"
)

type Command struct {
	CommandID     uuid.UUID      `json:"command_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	ShardID       string         `json:"shard_id"`
	CommandType   string         `json:"command_type"`
	Payload       map[string]any `json:"payload"`
	IssuedAt      time.Time      `json:"issued_at"`
}

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

type CommandProcessor interface {
	ProcessCommand(ctx context.Context, cmd Command) (Reply, error)
}

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
		return nil, fmt.Errorf("producer: %w", err)
	}
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) PublishReply(topic string, reply Reply) error {
	b, _ := json.Marshal(reply)
	key := reply.CorrelationID.String()
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key), Value: b,
	}, nil)
}

func (pr *Producer) Flush() { pr.p.Flush(5000) }
func (pr *Producer) Close() { pr.p.Flush(5000); pr.p.Close() }

type Consumer struct {
	c    *kafka.Consumer
	proc CommandProcessor
	prod *Producer
}

func NewConsumer(proc CommandProcessor, prod *Producer) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}

	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	if cellRole == "passive" {
		slog.Info("passive cell — command consumer disabled", "shard", shardID)
		return &Consumer{}, nil
	}

	groupID := fmt.Sprintf("cell-estoque-%s-%s-cmd", shardID, cellRole)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		return nil, err
	}

	if err := c.SubscribeTopics([]string{TopicCmdReservar, TopicCmdLiberar}, nil); err != nil {
		return nil, err
	}
	slog.Info("command consumer started", "group", groupID, "shard", shardID)
	return &Consumer{c: c, proc: proc, prod: prod}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	if cs.c == nil {
		<-ctx.Done()
		return
	}
	failCount := 0
	for {
		select {
		case <-ctx.Done():
			cs.c.Close()
			return
		default:
			msg, err := cs.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(strings.ToLower(err.Error()), "timed out") {
					slog.Error("kafka read", "err", err)
				}
				continue
			}
			var cmd Command
			if err := json.Unmarshal(msg.Value, &cmd); err != nil {
				slog.Error("unmarshal cmd", "err", err)
				continue
			}
			reply, err := cs.proc.ProcessCommand(ctx, cmd)
			if err != nil {
				failCount++
				if failCount >= 3 {
					cs.prod.SendToDLQ("estoque", *msg.TopicPartition.Topic,
						string(msg.Key), msg.Value, err, failCount)
					slog.Warn("sent to DLQ after repeated failures", "topic", *msg.TopicPartition.Topic, "attempts", failCount)
					failCount = 0
					continue
				}
				reply = Reply{
					ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
					SagaID: cmd.SagaID, CommandType: cmd.CommandType,
					Status: "failure", Error: err.Error(),
					RepliedAt: time.Now().UTC(),
				}
			} else {
				failCount = 0
			}
			replyTopic := replyTopicFor(cmd.CommandType, reply.Status)
			if err := cs.prod.PublishReply(replyTopic, reply); err != nil {
				slog.Error("publish reply", "err", err)
			}
		}
	}
}

func replyTopicFor(cmdType, status string) string {
	if cmdType == "reservar_estoque" && status == "failure" {
		return TopicReplyInsuficiente
	}
	return TopicReplyReservado
}
