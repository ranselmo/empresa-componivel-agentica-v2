package db

import (
	"context"
)

type EstoqueStats struct {
	TotalProdutos     int     `json:"total_produtos"`
	ProdutosDisponiveis int   `json:"produtos_disponiveis"`
	ProdutosSemEstoque  int   `json:"produtos_sem_estoque"`
	ValorTotalEstoque   float64 `json:"valor_total_estoque"`
}

func (s *Store) Stats(ctx context.Context) (*EstoqueStats, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*)                                                   AS total_produtos,
			COUNT(*) FILTER (WHERE quantidade_disponivel > 0)          AS produtos_disponiveis,
			COUNT(*) FILTER (WHERE quantidade_disponivel = 0)          AS produtos_sem_estoque,
			COALESCE(SUM(preco * quantidade_disponivel), 0)            AS valor_total_estoque
		FROM produtos
	`)
	var st EstoqueStats
	if err := row.Scan(&st.TotalProdutos, &st.ProdutosDisponiveis, &st.ProdutosSemEstoque, &st.ValorTotalEstoque); err != nil {
		return nil, err
	}
	return &st, nil
}
