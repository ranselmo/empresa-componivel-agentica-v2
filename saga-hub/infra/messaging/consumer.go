package messaging

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
	"github.com/ranselmo/poc-eci/saga-hub/infra/resilience"
)

type ReplyHandler func(ctx context.Context, reply domain.Reply) error

type Consumer struct {
	c        *kafka.Consumer
	handler  ReplyHandler
	bulkhead *resilience.Bulkhead
}

func NewConsumer(handler ReplyHandler) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 "saga-hub-replies-group",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       "false",
		"enable.auto.offset.store": "false",
	})
	if err != nil {
		return nil, err
	}
	topics := []string{
		domain.TopicReplyPedidoCriado,
		domain.TopicReplyPedidoCancelado,
		domain.TopicReplyEstoqueReservado,
		domain.TopicReplyEstoqueInsuficiente,
		domain.TopicReplyEstoqueLiberado,
		domain.TopicReplyNotificacaoEnviada,
	}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}
	return &Consumer{
		c:        c,
		handler:  handler,
		bulkhead: resilience.NewBulkhead("saga-hub", "", "reply-handler", 20),
	}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	slog.Info("saga-hub reply consumer started")
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
			var reply domain.Reply
			if err := json.Unmarshal(msg.Value, &reply); err != nil {
				slog.Error("unmarshal reply", "err", err)
				continue
			}
			err = cs.bulkhead.Do(ctx, func() error {
				return cs.handler(ctx, reply)
			})
			if err != nil {
				if strings.Contains(err.Error(), "capacity exceeded") {
					slog.Warn("bulkhead rejected reply", "type", reply.CommandType)
					continue
				}
				slog.Error("handle reply", "err", err,
					"correlation_id", reply.CorrelationID, "type", reply.CommandType)
				continue
			}
			if _, err := cs.c.CommitMessage(msg); err != nil {
				slog.Error("commit offset", "err", err)
			}
		}
	}
}
