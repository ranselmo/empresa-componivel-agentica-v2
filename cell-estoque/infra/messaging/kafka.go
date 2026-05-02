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
	"github.com/ranselmo/poc-eci/cell-estoque/domain"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/db"
)

func brokers() string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		return "localhost:9092"
	}
	return b
}

type Producer struct{ p *kafka.Producer }

func NewProducer() (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers(),
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery error", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) Publish(topic, key string, v any) error {
	body, _ := json.Marshal(v)
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          body,
	}, nil)
}

func (pr *Producer) Close() { pr.p.Flush(5000); pr.p.Close() }

type Consumer struct {
	c     *kafka.Consumer
	store *db.Store
	prod  *Producer
}

func NewConsumer(store *db.Store, prod *Producer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers(),
		"group.id":           "cell-estoque-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	})
	if err != nil {
		return nil, err
	}
	c.SubscribeTopics([]string{domain.TopicPedidoCriado}, nil)
	return &Consumer{c: c, store: store, prod: prod}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	slog.Info("consumer cell-estoque aguardando PedidoCriado")
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
	var ev domain.PedidoCriado
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		slog.Error("unmarshal PedidoCriado", "err", err)
		return
	}
	slog.Info("processando PedidoCriado", "pedido_id", ev.PedidoID)

	var reservados []domain.ItemEvento
	for _, item := range ev.Itens {
		prod, err := cs.store.BuscarPorID(ctx, item.ProdutoID)
		if err != nil {
			slog.Warn("produto não encontrado", "produto_id", item.ProdutoID)
			cs.prod.Publish(domain.TopicEstoqueInsuficiente, ev.PedidoID.String(),
				domain.EstoqueInsuficiente{
					EventID: uuid.New(), EventType: "EstoqueInsuficiente",
					PedidoID: ev.PedidoID, ProdutoID: item.ProdutoID,
					QuantidadeSolicitada: item.Quantidade, QuantidadeDisponivel: 0,
					Timestamp: time.Now().UTC(),
				})
			return
		}
		if !prod.Reservar(item.Quantidade) {
			slog.Warn("estoque insuficiente", "produto", item.ProdutoID,
				"disponivel", prod.QuantidadeDisponivel, "solicitado", item.Quantidade)
			cs.prod.Publish(domain.TopicEstoqueInsuficiente, ev.PedidoID.String(),
				domain.EstoqueInsuficiente{
					EventID: uuid.New(), EventType: "EstoqueInsuficiente",
					PedidoID: ev.PedidoID, ProdutoID: prod.ID,
					QuantidadeSolicitada: item.Quantidade,
					QuantidadeDisponivel: prod.QuantidadeDisponivel,
					Timestamp: time.Now().UTC(),
				})
			return
		}
		cs.store.Salvar(ctx, prod)
		reservados = append(reservados, domain.ItemEvento{
			ProdutoID: prod.ID, Quantidade: item.Quantidade, PrecoUnit: prod.Preco,
		})
	}

	cs.prod.Publish(domain.TopicEstoqueReservado, ev.PedidoID.String(),
		domain.EstoqueReservado{
			EventID: uuid.New(), EventType: "EstoqueReservado",
			PedidoID: ev.PedidoID, ItensReservados: reservados,
			Timestamp: time.Now().UTC(),
		})
	slog.Info("EstoqueReservado publicado", "pedido_id", ev.PedidoID)
}
