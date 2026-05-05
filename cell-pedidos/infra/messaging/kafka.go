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
	"github.com/ranselmo/poc-eci/shared/resilience"
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

type CommandProcessor interface {
	ProcessCommand(ctx context.Context, cmd Command) (Reply, error)
}

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
		return &Consumer{bulkhead: resilience.NewBulkhead("cell-pedidos", shardID, "cmd-processor", 10)}, nil
	}

	groupID := fmt.Sprintf("cell-pedidos-%s-%s-cmd", shardID, cellRole)

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
	if err := c.SubscribeTopics([]string{TopicCmdCriar, TopicCmdCancelar}, nil); err != nil {
		return nil, err
	}
	slog.Info("command consumer started", "group", groupID, "shard", shardID)
	return &Consumer{
		c:        c,
		proc:     proc,
		prod:     prod,
		bulkhead: resilience.NewBulkhead("cell-pedidos", shardID, "cmd-processor", 10),
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
					cs.prod.SendToDLQ("pedidos", *msg.TopicPartition.Topic,
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

			replyTopic := replyTopicFor(cmd.CommandType)
			if err := cs.prod.PublishReply(replyTopic, reply); err != nil {
				slog.Error("publish reply", "err", err)
				continue
			}
			if _, cErr := cs.c.CommitMessage(msg); cErr != nil {
				slog.Error("commit offset", "err", cErr)
			}
		}
	}
}

func replyTopicFor(cmdType string) string {
	switch cmdType {
	case "cancelar_pedido":
		return TopicReplyCancelado
	default:
		return TopicReplyCriado
	}
}
