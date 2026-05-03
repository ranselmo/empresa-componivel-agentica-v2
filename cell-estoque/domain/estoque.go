package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Produto struct {
	ID                   uuid.UUID
	Nome                 string
	QuantidadeDisponivel int
	Preco                float64
	AtualizadoEm         time.Time
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

// Tópicos democratizados publicados por este PBC
const (
	TopicEventEstoqueReservado    = "events.estoque.reservado"
	TopicEventEstoqueInsuficiente = "events.estoque.insuficiente"
)
