package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ── Domain model ───────────────────────────────────────────────────

type Produto struct {
	ID                  uuid.UUID
	Nome                string
	QuantidadeDisponivel int
	Preco               float64
	AtualizadoEm        time.Time
}

func (p *Produto) Reservar(quantidade int) bool {
	if quantidade <= 0 {
		return false
	}
	if p.QuantidadeDisponivel < quantidade {
		return false
	}
	p.QuantidadeDisponivel -= quantidade
	p.AtualizadoEm = time.Now().UTC()
	return true
}

func (p *Produto) Repor(quantidade int) error {
	if quantidade <= 0 {
		return errors.New("quantidade deve ser positiva")
	}
	p.QuantidadeDisponivel += quantidade
	p.AtualizadoEm = time.Now().UTC()
	return nil
}

// ── Tópicos Kafka ──────────────────────────────────────────────────

const (
	TopicPedidoCriado        = "dominio.pedido.criado"
	TopicEstoqueReservado    = "dominio.estoque.reservado"
	TopicEstoqueInsuficiente = "dominio.estoque.insuficiente"
)

// ── Eventos ────────────────────────────────────────────────────────

type ItemEvento struct {
	ProdutoID  uuid.UUID `json:"produto_id"`
	Quantidade int       `json:"quantidade"`
	PrecoUnit  float64   `json:"preco_unitario"`
}

type PedidoCriado struct {
	EventID   uuid.UUID    `json:"event_id"`
	EventType string       `json:"event_type"`
	PedidoID  uuid.UUID    `json:"pedido_id"`
	ClienteID uuid.UUID    `json:"cliente_id"`
	Itens     []ItemEvento `json:"itens"`
	Timestamp time.Time    `json:"timestamp"`
}

type EstoqueReservado struct {
	EventID         uuid.UUID    `json:"event_id"`
	EventType       string       `json:"event_type"`
	PedidoID        uuid.UUID    `json:"pedido_id"`
	ItensReservados []ItemEvento `json:"itens_reservados"`
	Timestamp       time.Time    `json:"timestamp"`
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
