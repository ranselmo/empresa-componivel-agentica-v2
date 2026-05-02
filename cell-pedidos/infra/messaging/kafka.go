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
	"github.com/ranselmo/poc-eci/cell-pedidos/domain"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/db"
)

func brokers() string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		return "localhost:9092"
	}
	return b
}

// ── Producer ───────────────────────────────────────────────────────

type Producer struct {
	p *kafka.Producer
}

func NewProducer() (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers(),
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	// Log delivery reports async
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("kafka delivery error", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) Publish(topic string, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          body,
	}, nil)
}

func (pr *Producer) Close() { pr.p.Flush(5000); pr.p.Close() }

// ── Consumer ───────────────────────────────────────────────────────

type Consumer struct {
	c     *kafka.Consumer
	store *db.Store
	prod  *Producer
}

func NewConsumer(store *db.Store, prod *Producer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers(),
		"group.id":           "cell-pedidos-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}

	topics := []string{domain.TopicEstoqueReservado, domain.TopicEstoqueInsuficiente}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	return &Consumer{c: c, store: store, prod: prod}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	slog.Info("consumer cell-pedidos aguardando eventos de estoque")
	for {
		select {
		case <-ctx.Done():
			cs.c.Close()
			return
		default:
			msg, err := cs.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(err.Error(), "timed out") {
					slog.Error("kafka read", "err", err)
				}
				continue
			}
			cs.process(ctx, msg)
		}
	}
}

func (cs *Consumer) process(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic
	slog.Info("evento recebido", "topic", topic)

	switch topic {
	case domain.TopicEstoqueReservado:
		var ev domain.EstoqueReservado
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			slog.Error("unmarshal EstoqueReservado", "err", err)
			return
		}
		p, err := cs.store.BuscarPorID(ctx, ev.PedidoID)
		if err != nil {
			slog.Error("buscar pedido", "err", err)
			return
		}
		if err := p.Confirmar(); err != nil {
			slog.Error("confirmar pedido", "err", err)
			return
		}
		cs.store.Salvar(ctx, p)
		cs.prod.Publish(domain.TopicPedidoConfirmado, p.ID.String(), domain.PedidoConfirmado{
			EventID: uuid.New(), EventType: "PedidoConfirmado",
			PedidoID: p.ID, ClienteID: p.ClienteID,
			ValorTotal: p.ValorTotal(), Timestamp: time.Now().UTC(),
		})
		slog.Info("pedido CONFIRMADO", "id", p.ID)

	case domain.TopicEstoqueInsuficiente:
		var ev domain.EstoqueInsuficiente
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			slog.Error("unmarshal EstoqueInsuficiente", "err", err)
			return
		}
		p, err := cs.store.BuscarPorID(ctx, ev.PedidoID)
		if err != nil {
			slog.Error("buscar pedido", "err", err)
			return
		}
		motivo := fmt.Sprintf("estoque insuficiente para produto %s (disponível: %d, solicitado: %d)",
			ev.ProdutoID, ev.QuantidadeDisponivel, ev.QuantidadeSolicitada)
		p.Cancelar()
		cs.store.Salvar(ctx, p)
		cs.prod.Publish(domain.TopicPedidoCancelado, p.ID.String(), domain.PedidoCancelado{
			EventID: uuid.New(), EventType: "PedidoCancelado",
			PedidoID: p.ID, Motivo: motivo, Timestamp: time.Now().UTC(),
		})
		slog.Info("pedido CANCELADO", "id", p.ID, "motivo", motivo)
	}
}
