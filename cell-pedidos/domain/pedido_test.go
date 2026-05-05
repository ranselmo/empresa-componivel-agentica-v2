package domain_test

import (
	"testing"

	"github.com/google/uuid"
	. "github.com/ranselmo/poc-eci/cell-pedidos/domain"
)

var (
	clienteID = uuid.New()
	produtoID = uuid.New()
)

func validItem() ItemPedido {
	return ItemPedido{ProdutoID: produtoID, Quantidade: 2, PrecoUnit: 50.0}
}

func TestNewPedido_Valido(t *testing.T) {
	p, err := NewPedido(clienteID, []ItemPedido{validItem()})
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != StatusPendente {
		t.Fatalf("expected PENDENTE, got %s", p.Status)
	}
	if p.ID == uuid.Nil {
		t.Fatal("ID must be set")
	}
}

func TestNewPedido_SemItens(t *testing.T) {
	_, err := NewPedido(clienteID, nil)
	if err == nil {
		t.Fatal("expected error for empty items")
	}
}

func TestNewPedido_QuantidadeZero(t *testing.T) {
	_, err := NewPedido(clienteID, []ItemPedido{{ProdutoID: produtoID, Quantidade: 0, PrecoUnit: 10}})
	if err == nil {
		t.Fatal("expected error for zero quantity")
	}
}

func TestNewPedido_PrecoNegativo(t *testing.T) {
	_, err := NewPedido(clienteID, []ItemPedido{{ProdutoID: produtoID, Quantidade: 1, PrecoUnit: -5}})
	if err == nil {
		t.Fatal("expected error for negative price")
	}
}

func TestPedido_ValorTotal(t *testing.T) {
	itens := []ItemPedido{
		{ProdutoID: produtoID, Quantidade: 2, PrecoUnit: 30.0},
		{ProdutoID: produtoID, Quantidade: 1, PrecoUnit: 20.0},
	}
	p, _ := NewPedido(clienteID, itens)
	if got := p.ValorTotal(); got != 80.0 {
		t.Fatalf("expected 80.0, got %f", got)
	}
}

func TestPedido_Confirmar(t *testing.T) {
	p, _ := NewPedido(clienteID, []ItemPedido{validItem()})
	if err := p.Confirmar(); err != nil {
		t.Fatal(err)
	}
	if p.Status != StatusConfirmado {
		t.Fatalf("expected CONFIRMADO, got %s", p.Status)
	}
}

func TestPedido_Confirmar_NaoPendente(t *testing.T) {
	p, _ := NewPedido(clienteID, []ItemPedido{validItem()})
	_ = p.Confirmar()
	if err := p.Confirmar(); err == nil {
		t.Fatal("expected error confirming already-confirmed pedido")
	}
}

func TestPedido_Cancelar(t *testing.T) {
	p, _ := NewPedido(clienteID, []ItemPedido{validItem()})
	if err := p.Cancelar(); err != nil {
		t.Fatal(err)
	}
	if p.Status != StatusCancelado {
		t.Fatalf("expected CANCELADO, got %s", p.Status)
	}
}

func TestPedido_Cancelar_Confirmado(t *testing.T) {
	p, _ := NewPedido(clienteID, []ItemPedido{validItem()})
	_ = p.Confirmar()
	if err := p.Cancelar(); err == nil {
		t.Fatal("expected error cancelling confirmed pedido")
	}
}
