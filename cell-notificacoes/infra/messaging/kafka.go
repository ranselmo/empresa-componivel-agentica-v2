package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-notificacoes/domain"
	"github.com/ranselmo/poc-eci/cell-notificacoes/infra/db"
	"github.com/ranselmo/poc-eci/shared/audit"
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

type Consumer struct {
	c        *kafka.Consumer
	store    *db.Store
	prod     *Producer
	audit    *audit.Logger
	bulkhead *resilience.Bulkhead
	shardID  string
}

func NewConsumer(store *db.Store, prod *Producer, al *audit.Logger) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}
	shardID := os.Getenv("SHARD_ID")
	cellRole := os.Getenv("CELL_ROLE")

	if cellRole == "passive" {
		slog.Info("passive cell — command consumer disabled", "shard", shardID)
		return &Consumer{
			bulkhead: resilience.NewBulkhead("cell-notificacoes", shardID, "cmd-processor", 10),
			shardID:  shardID,
		}, nil
	}

	groupID := fmt.Sprintf("cell-notificacoes-%s-%s-cmd", shardID, cellRole)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
	})
	if err != nil {
		return nil, err
	}
	if err := c.SubscribeTopics([]string{TopicCmdEnviar}, nil); err != nil {
		return nil, err
	}
	slog.Info("command consumer started", "group", groupID, "shard", shardID)
	return &Consumer{
		c:        c,
		store:    store,
		prod:     prod,
		audit:    al,
		bulkhead: resilience.NewBulkhead("cell-notificacoes", shardID, "cmd-processor", 10),
		shardID:  shardID,
	}, nil
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

			var processErr error
			bulkErr := cs.bulkhead.Do(ctx, func() error {
				processErr = cs.processCommand(ctx, cmd)
				return processErr
			})

			if bulkErr != nil {
				if bulkErr == processErr {
					key := fmt.Sprintf("%d:%d", msg.TopicPartition.Partition, msg.TopicPartition.Offset)
					failCounts[key]++
					if failCounts[key] >= 3 {
						SendToDLQ(cs.prod.KafkaProducer(), "notificacoes", *msg.TopicPartition.Topic,
							string(msg.Key), msg.Value, bulkErr, failCounts[key])
						slog.Warn("sent to DLQ", "topic", *msg.TopicPartition.Topic, "attempts", failCounts[key])
						delete(failCounts, key)
						if _, cErr := cs.c.CommitMessage(msg); cErr != nil {
							slog.Error("commit offset", "err", cErr)
						}
					}
				} else {
					slog.Warn("bulkhead rejected command", "type", cmd.CommandType)
				}
				continue
			}

			delete(failCounts, fmt.Sprintf("%d:%d", msg.TopicPartition.Partition, msg.TopicPartition.Offset))
			if _, cErr := cs.c.CommitMessage(msg); cErr != nil {
				slog.Error("commit offset", "err", cErr)
			}
		}
	}
}

func (cs *Consumer) processCommand(ctx context.Context, cmd Command) error {
	clienteIDStr := fmt.Sprintf("%v", cmd.Payload["cliente_id"])
	destID, _ := uuid.Parse(clienteIDStr)
	tipo, _ := cmd.Payload["tipo"].(string)
	if tipo == "" {
		tipo = "NOTIFICACAO"
	}

	conteudo := fmt.Sprintf("Notificação tipo=%s para cliente=%s", tipo, clienteIDStr)
	n := domain.NewNotificacao(destID, tipo, "email", conteudo)

	slog.Info("enviando notificação", "tipo", tipo, "destinatario", destID)
	if err := cs.store.Salvar(ctx, n); err != nil {
		slog.Error("salvar notificacao", "err", err)
		cs.audit.Log(ctx, "enviar_notificacao_falhou", "notificacao", n.ID.String(),
			clienteIDStr, map[string]any{"tipo": tipo, "erro": err.Error()})
		_ = cs.prod.PublishReply(Reply{
			ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
			SagaID: cmd.SagaID, CommandType: cmd.CommandType,
			Status: "failure", Error: err.Error(), RepliedAt: time.Now().UTC(),
		})
		return err
	}

	cs.audit.Log(ctx, "enviar_notificacao", "notificacao", n.ID.String(),
		clienteIDStr, map[string]any{"tipo": tipo, "canal": "email"})
	_ = cs.prod.PublishReply(Reply{
		ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
		SagaID: cmd.SagaID, CommandType: cmd.CommandType,
		Status: "success", RepliedAt: time.Now().UTC(),
		Payload: map[string]any{"notificacao_id": n.ID.String()},
	})
	cs.prod.PublishEvent(tipo, map[string]any{
		"destinatario_id": destID.String(),
		"notificacao_id":  n.ID.String(),
	})
	return nil
}
