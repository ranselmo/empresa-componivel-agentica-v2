package domain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/ranselmo/poc-eci/cell-estoque/domain"
)

func novoProduto(qtd int) *Produto {
	return &Produto{
		ID:                   uuid.New(),
		Nome:                 "Produto Teste",
		QuantidadeDisponivel: qtd,
		Preco:                99.90,
		AtualizadoEm:         time.Now().UTC(),
	}
}

func TestProduto_Reservar_Suficiente(t *testing.T) {
	p := novoProduto(10)
	if !p.Reservar(3) {
		t.Fatal("expected Reservar to return true")
	}
	if p.QuantidadeDisponivel != 7 {
		t.Fatalf("expected 7, got %d", p.QuantidadeDisponivel)
	}
}

func TestProduto_Reservar_Insuficiente(t *testing.T) {
	p := novoProduto(2)
	original := p.QuantidadeDisponivel
	if p.Reservar(5) {
		t.Fatal("expected Reservar to return false")
	}
	if p.QuantidadeDisponivel != original {
		t.Fatal("quantity must not change on failed reservation")
	}
}

func TestProduto_Reservar_QuantidadeZero(t *testing.T) {
	p := novoProduto(10)
	if p.Reservar(0) {
		t.Fatal("expected false for zero quantity")
	}
}

func TestProduto_Liberar(t *testing.T) {
	p := novoProduto(5)
	before := p.AtualizadoEm
	time.Sleep(time.Millisecond)
	if err := p.Liberar(3); err != nil {
		t.Fatal(err)
	}
	if p.QuantidadeDisponivel != 8 {
		t.Fatalf("expected 8, got %d", p.QuantidadeDisponivel)
	}
	if !p.AtualizadoEm.After(before) {
		t.Fatal("AtualizadoEm should be updated")
	}
}

func TestProduto_Liberar_QuantidadeInvalida(t *testing.T) {
	p := novoProduto(5)
	if err := p.Liberar(0); err == nil {
		t.Fatal("expected error for zero quantity")
	}
}

func TestProduto_Repor(t *testing.T) {
	p := novoProduto(5)
	if err := p.Repor(10); err != nil {
		t.Fatal(err)
	}
	if p.QuantidadeDisponivel != 15 {
		t.Fatalf("expected 15, got %d", p.QuantidadeDisponivel)
	}
}

func TestProduto_Repor_QuantidadeInvalida(t *testing.T) {
	p := novoProduto(5)
	if err := p.Repor(-1); err == nil {
		t.Fatal("expected error for negative quantity")
	}
}
