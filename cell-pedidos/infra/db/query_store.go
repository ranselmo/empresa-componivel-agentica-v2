package db

import (
	"context"
)

type PedidoStats struct {
	Total       int     `json:"total"`
	Pendentes   int     `json:"pendentes"`
	Confirmados int     `json:"confirmados"`
	Cancelados  int     `json:"cancelados"`
	ValorTotal  float64 `json:"valor_total"`
}

func (s *Store) Stats(ctx context.Context) (*PedidoStats, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*)                                          AS total,
			COUNT(*) FILTER (WHERE status = 'PENDENTE')      AS pendentes,
			COUNT(*) FILTER (WHERE status = 'CONFIRMADO')    AS confirmados,
			COUNT(*) FILTER (WHERE status = 'CANCELADO')     AS cancelados,
			COALESCE(SUM(valor_total), 0)                    AS valor_total
		FROM pedidos
	`)
	var st PedidoStats
	if err := row.Scan(&st.Total, &st.Pendentes, &st.Confirmados, &st.Cancelados, &st.ValorTotal); err != nil {
		return nil, err
	}
	return &st, nil
}
