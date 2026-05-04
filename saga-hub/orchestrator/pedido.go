package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/infra/messaging"
)

var (
	sagaStarted = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "saga_started_total"}, []string{"type"})
	sagaDone = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "saga_completed_total"}, []string{"type", "outcome"})
	sagaDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "saga_duration_seconds",
		Buckets: []float64{.1, .5, 1, 2, 5, 10, 30},
	}, []string{"type"})
)

type PedidoSaga struct {
	store *db.SagaStore
	prod  *messaging.Producer
}

func NewPedidoSaga(store *db.SagaStore, prod *messaging.Producer) *PedidoSaga {
	return &PedidoSaga{store: store, prod: prod}
}

func (o *PedidoSaga) Start(ctx context.Context, clienteID uuid.UUID, shardID string, payload map[string]any) (*domain.Saga, error) {
	saga := domain.NewSaga(clienteID, shardID, payload)
	if err := o.store.Save(ctx, saga); err != nil {
		return nil, fmt.Errorf("save saga: %w", err)
	}
	sagaStarted.WithLabelValues("pedido").Inc()
	cmdPayload := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		cmdPayload[k] = v
	}
	cmdPayload["cliente_id"] = clienteID.String()
	cmd := domain.Command{
		CommandID: uuid.New(), CorrelationID: saga.CorrelationID,
		SagaID: saga.ID, ShardID: shardID,
		CommandType: "criar_pedido", Payload: cmdPayload,
		IssuedAt: time.Now().UTC(),
	}
	if err := o.prod.PublishCommand(domain.TopicCmdPedidoCriar, cmd); err != nil {
		return nil, fmt.Errorf("publish cmd: %w", err)
	}
	slog.Info("saga started", "saga_id", saga.ID, "shard", shardID)
	return saga, nil
}

func (o *PedidoSaga) HandleReply(ctx context.Context, reply domain.Reply) error {
	saga, err := o.store.FindByCorrelationID(ctx, reply.CorrelationID)
	if err != nil {
		return fmt.Errorf("saga not found correlation_id=%s", reply.CorrelationID)
	}
	switch reply.CommandType {
	case "criar_pedido":
		return o.onPedidoCriado(ctx, saga, reply)
	case "reservar_estoque":
		return o.onEstoqueReply(ctx, saga, reply)
	case "enviar_notificacao":
		return o.onNotificacaoReply(ctx, saga, reply)
	case "cancelar_pedido":
		return o.onPedidoCancelado(ctx, saga, reply)
	default:
		return fmt.Errorf("unknown command_type: %s", reply.CommandType)
	}
}

func (o *PedidoSaga) newCmd(saga *domain.Saga, cmdType string, payload map[string]any) domain.Command {
	return domain.Command{
		CommandID: uuid.New(), CorrelationID: saga.CorrelationID,
		SagaID: saga.ID, ShardID: saga.ShardID,
		CommandType: cmdType, Payload: payload,
		IssuedAt: time.Now().UTC(),
	}
}

func (o *PedidoSaga) onPedidoCriado(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	if reply.Status == "failure" {
		return o.failSaga(ctx, saga, reply.Error)
	}
	saga.CurrentStep = domain.StepReservarEstoque
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	return o.prod.PublishCommand(domain.TopicCmdEstoqueReservar,
		o.newCmd(saga, "reservar_estoque", saga.Payload))
}

func (o *PedidoSaga) onEstoqueReply(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	if reply.Status == "failure" {
		saga.Status = domain.StatusCompensating
		saga.CurrentStep = domain.StepCompensarPedido
		saga.UpdatedAt = time.Now().UTC()
		_ = o.store.Save(ctx, saga)
		return o.prod.PublishCommand(domain.TopicCmdPedidoCancelar,
			o.newCmd(saga, "cancelar_pedido", map[string]any{"motivo": reply.Error}))
	}
	saga.CurrentStep = domain.StepEnviarNotificacao
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	return o.prod.PublishCommand(domain.TopicCmdNotificacaoEnviar,
		o.newCmd(saga, "enviar_notificacao", map[string]any{
			"cliente_id": saga.ClienteID.String(),
			"tipo":       "PEDIDO_CONFIRMADO",
			"payload":    saga.Payload,
		}))
}

func (o *PedidoSaga) onNotificacaoReply(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	saga.Status = domain.StatusCompleted
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "success").Inc()
	sagaDur.WithLabelValues("pedido").Observe(time.Since(saga.CreatedAt).Seconds())
	o.prod.PublishBusinessEvent(domain.TopicEventPedidoConfirmado, domain.BusinessEvent{
		EventID: uuid.New(), EventType: "PedidoConfirmado",
		ShardID: saga.ShardID, OccurredAt: time.Now().UTC(), Payload: saga.Payload,
	})
	slog.Info("saga completed", "saga_id", saga.ID)
	return nil
}

func (o *PedidoSaga) onPedidoCancelado(ctx context.Context, saga *domain.Saga, _ domain.Reply) error {
	saga.Status = domain.StatusFailed
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "compensated").Inc()
	o.prod.PublishBusinessEvent(domain.TopicEventPedidoCancelado, domain.BusinessEvent{
		EventID: uuid.New(), EventType: "PedidoCancelado",
		ShardID: saga.ShardID, OccurredAt: time.Now().UTC(), Payload: saga.Payload,
	})
	_ = o.prod.PublishCommand(domain.TopicCmdNotificacaoEnviar,
		o.newCmd(saga, "enviar_notificacao", map[string]any{
			"cliente_id": saga.ClienteID.String(),
			"tipo":       "PEDIDO_CANCELADO",
		}))
	slog.Warn("saga compensated", "saga_id", saga.ID)
	return nil
}

func (o *PedidoSaga) failSaga(ctx context.Context, saga *domain.Saga, reason string) error {
	saga.Status = domain.StatusFailed
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "failed").Inc()
	slog.Error("saga failed", "saga_id", saga.ID, "reason", reason)
	return nil
}
