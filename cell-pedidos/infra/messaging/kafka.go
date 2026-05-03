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

	groupID := fmt.Sprintf("cell-pedidos-%s-%s-cmd", shardID, cellRole)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		return nil, err
	}

	if err := c.SubscribeTopics([]string{TopicCmdCriar, TopicCmdCancelar}, nil); err != nil {
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
					cs.prod.SendToDLQ("pedidos", *msg.TopicPartition.Topic,
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
			replyTopic := replyTopicFor(cmd.CommandType)
			if err := cs.prod.PublishReply(replyTopic, reply); err != nil {
				slog.Error("publish reply", "err", err)
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
