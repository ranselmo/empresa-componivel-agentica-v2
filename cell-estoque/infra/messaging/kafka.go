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
	"github.com/ranselmo/poc-eci/cell-estoque/infra/monitoring"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/resilience"
)

const (
	TopicCmdReservar       = "commands.estoque.reservar"
	TopicCmdLiberar        = "commands.estoque.liberar"
	TopicReplyReservado    = "replies.estoque.reservado"
	TopicReplyInsuficiente = "replies.estoque.insuficiente"
	TopicReplyLiberado     = "replies.estoque.liberado"
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
		breaker: resilience.NewBreaker("cell-estoque", shardID, "kafka-producer"),
		shardID: shardID,
	}, nil
}

func (pr *Producer) PublishReply(topic string, reply Reply) error {
	b, _ := json.Marshal(reply)
	key := reply.CorrelationID.String()
	err := pr.breaker.Execute(func() error {
		return pr.p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key), Value: b,
		}, nil)
	})
	monitoring.KafkaMessages.WithLabelValues("cell-estoque", pr.shardID, topic, "publish").Inc()
	return err
}

func (pr *Producer) KafkaProducer() *kafka.Producer { return pr.p }
func (pr *Producer) Flush()                         { pr.p.Flush(5000) }
func (pr *Producer) Close()                         { pr.p.Flush(5000); pr.p.Close() }

type Consumer struct {
	c        *kafka.Consumer
	proc     CommandProcessor
	prod     *Producer
	bulkhead *resilience.Bulkhead
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
		return &Consumer{bulkhead: resilience.NewBulkhead("cell-estoque", shardID, "cmd-processor", 10)}, nil
	}

	groupID := fmt.Sprintf("cell-estoque-%s-%s-cmd", shardID, cellRole)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       "false",
		"enable.auto.offset.store": "false",
	})
	if err != nil {
		return nil, err
	}
	if err := c.SubscribeTopics([]string{TopicCmdReservar, TopicCmdLiberar}, nil); err != nil {
		return nil, err
	}
	slog.Info("command consumer started", "group", groupID, "shard", shardID)
	return &Consumer{
		c:        c,
		proc:     proc,
		prod:     prod,
		bulkhead: resilience.NewBulkhead("cell-estoque", shardID, "cmd-processor", 10),
	}, nil
}

func offsetKey(msg *kafka.Message) string {
	return fmt.Sprintf("%d:%d", msg.TopicPartition.Partition, msg.TopicPartition.Offset)
}

func (cs *Consumer) Run(ctx context.Context) {
	if cs.c == nil {
		<-ctx.Done()
		return
	}
	failCounts := make(map[string]int)
	for {
		select {
		case <-ctx.Done():
			cs.c.Close()
			return
		default:
			msg, err := cs.c.ReadMessage(200)
			if err != nil {
				kafkaErr, ok := err.(kafka.Error)
				if !ok || kafkaErr.Code() != kafka.ErrTimedOut {
					slog.Error("kafka read", "err", err)
				}
				continue
			}
			var cmd Command
			if err := json.Unmarshal(msg.Value, &cmd); err != nil {
				slog.Error("unmarshal cmd", "err", err)
				if _, cErr := cs.c.CommitMessage(msg); cErr != nil {
					slog.Error("commit offset", "err", cErr)
				}
				continue
			}

			var reply Reply
			err = cs.bulkhead.Do(ctx, func() error {
				var processErr error
				reply, processErr = cs.proc.ProcessCommand(ctx, cmd)
				return processErr
			})

			if err != nil {
				if strings.Contains(err.Error(), "capacity exceeded") {
					slog.Warn("bulkhead rejected command", "type", cmd.CommandType)
					continue
				}
				key := offsetKey(msg)
				failCounts[key]++
				if failCounts[key] >= 3 {
					cs.prod.SendToDLQ("estoque", *msg.TopicPartition.Topic,
						string(msg.Key), msg.Value, err, failCounts[key])
					slog.Warn("sent to DLQ after repeated failures", "topic", *msg.TopicPartition.Topic, "attempts", failCounts[key])
					delete(failCounts, key)
					if _, cErr := cs.c.CommitMessage(msg); cErr != nil {
						slog.Error("commit offset", "err", cErr)
					}
					continue
				}
				reply = Reply{
					ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
					SagaID: cmd.SagaID, CommandType: cmd.CommandType,
					Status: "failure", Error: err.Error(),
					RepliedAt: time.Now().UTC(),
				}
			} else {
				delete(failCounts, offsetKey(msg))
			}

			replyTopic := replyTopicFor(cmd.CommandType, reply.Status)
			if err := cs.prod.PublishReply(replyTopic, reply); err != nil {
				slog.Error("publish reply", "err", err)
				continue
			}
			if _, err := cs.c.CommitMessage(msg); err != nil {
				slog.Error("commit offset", "err", err)
			}
		}
	}
}

func replyTopicFor(cmdType, status string) string {
	switch cmdType {
	case "reservar_estoque":
		if status == "failure" {
			return TopicReplyInsuficiente
		}
		return TopicReplyReservado
	case "liberar_estoque":
		return TopicReplyLiberado
	default:
		return TopicReplyReservado
	}
}
