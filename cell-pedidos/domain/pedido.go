package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type StatusPedido string

const (
	StatusPendente   StatusPedido = "PENDENTE"
	StatusConfirmado StatusPedido = "CONFIRMADO"
	StatusCancelado  StatusPedido = "CANCELADO"
)

type ItemPedido struct {
	ProdutoID  uuid.UUID
	Quantidade int
	PrecoUnit  float64
}

type Pedido struct {
	ID          uuid.UUID
	ClienteID   uuid.UUID
	Itens       []ItemPedido
	Status      StatusPedido
	CriadoEm   time.Time
	AtualizadoEm time.Time
}

// NewPedido cria um pedido validado. Regras de negócio puras — sem framework.
func NewPedido(clienteID uuid.UUID, itens []ItemPedido) (*Pedido, error) {
	if len(itens) == 0 {
		return nil, errors.New("pedido deve ter ao menos um item")
	}
	for _, i := range itens {
		if i.Quantidade <= 0 {
			return nil, errors.New("quantidade deve ser positiva")
		}
		if i.PrecoUnit <= 0 {
			return nil, errors.New("preço deve ser positivo")
		}
	}
	now := time.Now().UTC()
	return &Pedido{
		ID:           uuid.New(),
		ClienteID:    clienteID,
		Itens:        itens,
		Status:       StatusPendente,
		CriadoEm:    now,
		AtualizadoEm: now,
	}, nil
}

func (p *Pedido) ValorTotal() float64 {
	total := 0.0
	for _, i := range p.Itens {
		total += float64(i.Quantidade) * i.PrecoUnit
	}
	return total
}

func (p *Pedido) Confirmar() error {
	if p.Status != StatusPendente {
		return errors.New("pedido só pode ser confirmado quando PENDENTE")
	}
	p.Status = StatusConfirmado
	p.AtualizadoEm = time.Now().UTC()
	return nil
}

func (p *Pedido) Cancelar() error {
	if p.Status == StatusConfirmado {
		return errors.New("pedido já confirmado não pode ser cancelado diretamente")
	}
	p.Status = StatusCancelado
	p.AtualizadoEm = time.Now().UTC()
	return nil
}
