package domain

import (
	"time"

	"github.com/google/uuid"
)

// Tópicos Kafka — contratos imutáveis entre células
const (
	TopicPedidoCriado      = "dominio.pedido.criado"
	TopicPedidoConfirmado  = "dominio.pedido.confirmado"
	TopicPedidoCancelado   = "dominio.pedido.cancelado"
	TopicEstoqueReservado  = "dominio.estoque.reservado"
	TopicEstoqueInsuficiente = "dominio.estoque.insuficiente"
	TopicNotificacaoEnviada = "dominio.notificacao.enviada"
)

// ── Eventos publicados por cell-pedidos ────────────────────────────

type ItemEvento struct {
	ProdutoID    uuid.UUID `json:"produto_id"`
	Quantidade   int       `json:"quantidade"`
	PrecoUnit    float64   `json:"preco_unitario"`
}

type PedidoCriado struct {
	EventID    uuid.UUID    `json:"event_id"`
	EventType  string       `json:"event_type"`
	PedidoID   uuid.UUID    `json:"pedido_id"`
	ClienteID  uuid.UUID    `json:"cliente_id"`
	Itens      []ItemEvento `json:"itens"`
	ValorTotal float64      `json:"valor_total"`
	Timestamp  time.Time    `json:"timestamp"`
}

type PedidoConfirmado struct {
	EventID    uuid.UUID `json:"event_id"`
	EventType  string    `json:"event_type"`
	PedidoID   uuid.UUID `json:"pedido_id"`
	ClienteID  uuid.UUID `json:"cliente_id"`
	ValorTotal float64   `json:"valor_total"`
	Timestamp  time.Time `json:"timestamp"`
}

type PedidoCancelado struct {
	EventID   uuid.UUID `json:"event_id"`
	EventType string    `json:"event_type"`
	PedidoID  uuid.UUID `json:"pedido_id"`
	Motivo    string    `json:"motivo"`
	Timestamp time.Time `json:"timestamp"`
}

// ── Eventos consumidos por cell-pedidos (vêm de cell-estoque) ──────

type EstoqueReservado struct {
	EventID          uuid.UUID    `json:"event_id"`
	EventType        string       `json:"event_type"`
	PedidoID         uuid.UUID    `json:"pedido_id"`
	ItensReservados  []ItemEvento `json:"itens_reservados"`
	Timestamp        time.Time    `json:"timestamp"`
}

type EstoqueInsuficiente struct {
	EventID              uuid.UUID `json:"event_id"`
	EventType            string    `json:"event_type"`
	PedidoID             uuid.UUID `json:"pedido_id"`
	ProdutoID            uuid.UUID `json:"produto_id"`
	QuantidadeSolicitada int       `json:"quantidade_solicitada"`
	QuantidadeDisponivel int       `json:"quantidade_disponivel"`
	Timestamp            time.Time `json:"timestamp"`
}
