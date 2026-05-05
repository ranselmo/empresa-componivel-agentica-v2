package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranselmo/poc-eci/cell-pedidos/domain"
	"github.com/ranselmo/poc-eci/shared/cache"
	"github.com/ranselmo/poc-eci/shared/monitoring"
	"github.com/ranselmo/poc-eci/shared/resilience"
)

type Store struct {
	pool    *pgxpool.Pool
	breaker *resilience.Breaker
	cache   *cache.Cache
	shardID string
}

func New(ctx context.Context) (*Store, error) {
	dsn := os.Getenv("DATABASE_URL")
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	config.MaxConns = 10
	config.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	shardID := os.Getenv("SHARD_ID")
	ca, err := cache.New("pedidos:", cache.TTLFromEnv(60*time.Second))
	if err != nil {
		slog.Warn("cache unavailable, running without cache", "err", err)
		ca = nil
	}
	return &Store{
		pool:    pool,
		breaker: resilience.NewBreaker("cell-pedidos", shardID, "db"),
		cache:   ca,
		shardID: shardID,
	}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pedidos (
			id            UUID PRIMARY KEY,
			cliente_id    UUID NOT NULL,
			status        TEXT NOT NULL DEFAULT 'PENDENTE',
			valor_total   NUMERIC(12,2) NOT NULL,
			itens         JSONB NOT NULL,
			criado_em     TIMESTAMPTZ NOT NULL,
			atualizado_em TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_pedidos_cliente ON pedidos(cliente_id);
		CREATE INDEX IF NOT EXISTS idx_pedidos_status  ON pedidos(status);
	`)
	return err
}

type itemJSON struct {
	ProdutoID  string  `json:"produto_id"`
	Quantidade int     `json:"quantidade"`
	PrecoUnit  float64 `json:"preco_unitario"`
}

func (s *Store) Salvar(ctx context.Context, p *domain.Pedido) error {
	itens := make([]itemJSON, len(p.Itens))
	for i, it := range p.Itens {
		itens[i] = itemJSON{ProdutoID: it.ProdutoID.String(), Quantidade: it.Quantidade, PrecoUnit: it.PrecoUnit}
	}
	itensJSON, _ := json.Marshal(itens)

	err := resilience.Retry(ctx, 3, 100*time.Millisecond, func() error {
		return s.breaker.Execute(func() error {
			_, err := s.pool.Exec(ctx, `
				INSERT INTO pedidos (id, cliente_id, status, valor_total, itens, criado_em, atualizado_em)
				VALUES ($1,$2,$3,$4,$5,$6,$7)
				ON CONFLICT (id) DO UPDATE SET
					status = EXCLUDED.status,
					valor_total = EXCLUDED.valor_total,
					atualizado_em = EXCLUDED.atualizado_em
			`, p.ID, p.ClienteID, string(p.Status), p.ValorTotal(), itensJSON, p.CriadoEm, p.AtualizadoEm)
			return err
		})
	})
	monitoring.DBQueries.WithLabelValues("cell-pedidos", s.shardID, "salvar").Inc()
	monitoring.TxCost.WithLabelValues("cell-pedidos", s.shardID, "salvar").Observe(0.001)
	if err == nil && s.cache != nil {
		_ = s.cache.Del(ctx, p.ID.String())
	}
	return err
}

func (s *Store) BuscarPorID(ctx context.Context, id uuid.UUID) (*domain.Pedido, error) {
	if s.cache != nil {
		var p domain.Pedido
		if hit, _ := s.cache.Get(ctx, id.String(), &p); hit {
			return &p, nil
		}
	}

	var (
		pid, cid               uuid.UUID
		status                 string
		itensRaw               []byte
		criadoEm, atualizadoEm time.Time
	)
	err := s.breaker.Execute(func() error {
		row := s.pool.QueryRow(ctx,
			`SELECT id, cliente_id, status, itens, criado_em, atualizado_em FROM pedidos WHERE id=$1`, id)
		return row.Scan(&pid, &cid, &status, &itensRaw, &criadoEm, &atualizadoEm)
	})
	monitoring.DBQueries.WithLabelValues("cell-pedidos", s.shardID, "buscar").Inc()
	if err != nil {
		return nil, err
	}

	var raw []itemJSON
	if err := json.Unmarshal(itensRaw, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal itens: %w", err)
	}
	itens := make([]domain.ItemPedido, len(raw))
	for i, r := range raw {
		uid, _ := uuid.Parse(r.ProdutoID)
		itens[i] = domain.ItemPedido{ProdutoID: uid, Quantidade: r.Quantidade, PrecoUnit: r.PrecoUnit}
	}
	p := &domain.Pedido{
		ID: pid, ClienteID: cid,
		Status: domain.StatusPedido(status),
		Itens:  itens, CriadoEm: criadoEm, AtualizadoEm: atualizadoEm,
	}
	if s.cache != nil {
		_ = s.cache.Set(ctx, id.String(), p)
	}
	return p, nil
}

func (s *Store) Listar(ctx context.Context, limit int) ([]*domain.Pedido, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, cliente_id, status, itens, criado_em, atualizado_em
		 FROM pedidos ORDER BY criado_em DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pedidos []*domain.Pedido
	for rows.Next() {
		var pid, cid uuid.UUID
		var status string
		var itensRaw []byte
		var criadoEm, atualizadoEm time.Time
		if err := rows.Scan(&pid, &cid, &status, &itensRaw, &criadoEm, &atualizadoEm); err != nil {
			continue
		}
		var raw []itemJSON
		if err := json.Unmarshal(itensRaw, &raw); err != nil {
			slog.Warn("unmarshal itens", "err", err)
		}
		itens := make([]domain.ItemPedido, len(raw))
		for i, r := range raw {
			uid, _ := uuid.Parse(r.ProdutoID)
			itens[i] = domain.ItemPedido{ProdutoID: uid, Quantidade: r.Quantidade, PrecoUnit: r.PrecoUnit}
		}
		pedidos = append(pedidos, &domain.Pedido{
			ID: pid, ClienteID: cid,
			Status: domain.StatusPedido(status),
			Itens: itens, CriadoEm: criadoEm, AtualizadoEm: atualizadoEm,
		})
	}
	return pedidos, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }
