package domain

import (
	"time"

	"github.com/google/uuid"
)

type SagaStatus string
type SagaStep string

const (
	StatusStarted      SagaStatus = "STARTED"
	StatusCompleted    SagaStatus = "COMPLETED"
	StatusCompensating SagaStatus = "COMPENSATING"
	StatusFailed       SagaStatus = "FAILED"

	StepCriarPedido       SagaStep = "CRIAR_PEDIDO"
	StepReservarEstoque   SagaStep = "RESERVAR_ESTOQUE"
	StepEnviarNotificacao SagaStep = "ENVIAR_NOTIFICACAO"
	StepCompensarPedido   SagaStep = "COMPENSAR_PEDIDO"
	StepLiberarEstoque    SagaStep = "LIBERAR_ESTOQUE"
)

// Tópicos Kafka — contrato único para todo o sistema
const (
	// saga-hub → PBC
	TopicCmdPedidoCriar       = "commands.pedidos.criar"
	TopicCmdPedidoCancelar    = "commands.pedidos.cancelar"
	TopicCmdEstoqueReservar   = "commands.estoque.reservar"
	TopicCmdEstoqueLiberar    = "commands.estoque.liberar"
	TopicCmdNotificacaoEnviar = "commands.notificacoes.enviar"
	// PBC → saga-hub
	TopicReplyPedidoCriado        = "replies.pedidos.criado"
	TopicReplyPedidoCancelado     = "replies.pedidos.cancelado"
	TopicReplyEstoqueReservado    = "replies.estoque.reservado"
	TopicReplyEstoqueInsuficiente = "replies.estoque.insuficiente"
	TopicReplyEstoqueLiberado     = "replies.estoque.liberado"
	TopicReplyNotificacaoEnviada  = "replies.notificacoes.enviada"
	// PBC → todos (democratizado)
	TopicEventPedidoConfirmado   = "events.pedidos.confirmado"
	TopicEventPedidoCancelado    = "events.pedidos.cancelado"
	TopicEventPedidoFalhou       = "events.pedidos.falhou"
	TopicEventNotificacaoEnviada = "events.notificacoes.enviada"
)

type Saga struct {
	ID            uuid.UUID
	CorrelationID uuid.UUID
	Status        SagaStatus
	CurrentStep   SagaStep
	ClienteID     uuid.UUID
	ShardID       string
	Payload       map[string]any
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewSaga(clienteID uuid.UUID, shardID string, payload map[string]any) *Saga {
	id := uuid.New()
	now := time.Now().UTC()
	return &Saga{
		ID: id, CorrelationID: id,
		Status: StatusStarted, CurrentStep: StepCriarPedido,
		ClienteID: clienteID, ShardID: shardID, Payload: payload,
		CreatedAt: now, UpdatedAt: now,
	}
}

// Command é a mensagem que saga-hub envia a um PBC
type Command struct {
	CommandID     uuid.UUID      `json:"command_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	ShardID       string         `json:"shard_id"`
	CommandType   string         `json:"command_type"`
	Payload       map[string]any `json:"payload"`
	IssuedAt      time.Time      `json:"issued_at"`
}

// Reply é a mensagem que um PBC envia de volta ao saga-hub
type Reply struct {
	ReplyID       uuid.UUID      `json:"reply_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	CommandType   string         `json:"command_type"`
	Status        string         `json:"status"` // "success" | "failure"
	Payload       map[string]any `json:"payload"`
	Error         string         `json:"error,omitempty"`
	RepliedAt     time.Time      `json:"replied_at"`
}

// BusinessEvent é publicado em events.* após saga concluída (democratizado)
type BusinessEvent struct {
	EventID    uuid.UUID      `json:"event_id"`
	EventType  string         `json:"event_type"`
	ShardID    string         `json:"shard_id"`
	OccurredAt time.Time      `json:"occurred_at"`
	Payload    map[string]any `json:"payload"`
}
