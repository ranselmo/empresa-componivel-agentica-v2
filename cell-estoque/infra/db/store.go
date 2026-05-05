package db

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranselmo/poc-eci/cell-estoque/domain"
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
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	shardID := os.Getenv("SHARD_ID")
	ca, err := cache.New("estoque:", 60*time.Second)
	if err != nil {
		slog.Warn("cache unavailable, running without cache", "err", err)
		ca = nil
	}
	return &Store{
		pool:    pool,
		breaker: resilience.NewBreaker("cell-estoque", shardID, "db"),
		cache:   ca,
		shardID: shardID,
	}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS produtos (
			id                    UUID PRIMARY KEY,
			nome                  TEXT NOT NULL,
			quantidade_disponivel INT  NOT NULL DEFAULT 0,
			preco                 NUMERIC(12,2) NOT NULL,
			atualizado_em         TIMESTAMPTZ NOT NULL
		);
	`)
	if err != nil {
		return err
	}
	return s.seed(ctx)
}

func (s *Store) seed(ctx context.Context) error {
	seeds := []struct {
		id    string
		nome  string
		qty   int
		preco float64
	}{
		{"11111111-1111-1111-1111-111111111111", "Notebook Pro", 10, 4999.90},
		{"22222222-2222-2222-2222-222222222222", "Mouse Ergonômico", 50, 199.90},
		{"33333333-3333-3333-3333-333333333333", "Teclado Mecânico", 5, 599.90},
		{"44444444-4444-4444-4444-444444444444", "Monitor 4K", 0, 2499.90},
	}
	for _, s2 := range seeds {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO produtos (id, nome, quantidade_disponivel, preco, atualizado_em)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO NOTHING
		`, s2.id, s2.nome, s2.qty, s2.preco, time.Now().UTC())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) BuscarPorID(ctx context.Context, id uuid.UUID) (*domain.Produto, error) {
	if s.cache != nil {
		var p domain.Produto
		if hit, _ := s.cache.Get(ctx, id.String(), &p); hit {
			return &p, nil
		}
	}

	var p domain.Produto
	err := s.breaker.Execute(func() error {
		row := s.pool.QueryRow(ctx,
			`SELECT id, nome, quantidade_disponivel, preco, atualizado_em FROM produtos WHERE id=$1`, id)
		return row.Scan(&p.ID, &p.Nome, &p.QuantidadeDisponivel, &p.Preco, &p.AtualizadoEm)
	})
	monitoring.DBQueries.WithLabelValues("cell-estoque", s.shardID, "buscar").Inc()
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		_ = s.cache.Set(ctx, id.String(), &p)
	}
	return &p, nil
}

func (s *Store) Salvar(ctx context.Context, p *domain.Produto) error {
	err := resilience.Retry(ctx, 3, 100*time.Millisecond, func() error {
		return s.breaker.Execute(func() error {
			_, err := s.pool.Exec(ctx, `
				INSERT INTO produtos (id, nome, quantidade_disponivel, preco, atualizado_em)
				VALUES ($1,$2,$3,$4,$5)
				ON CONFLICT (id) DO UPDATE SET
					quantidade_disponivel = EXCLUDED.quantidade_disponivel,
					atualizado_em = EXCLUDED.atualizado_em
			`, p.ID, p.Nome, p.QuantidadeDisponivel, p.Preco, p.AtualizadoEm)
			return err
		})
	})
	monitoring.DBQueries.WithLabelValues("cell-estoque", s.shardID, "salvar").Inc()
	monitoring.TxCost.WithLabelValues("cell-estoque", s.shardID, "salvar").Observe(0.001)
	if err == nil && s.cache != nil {
		_ = s.cache.Del(ctx, p.ID.String())
	}
	return err
}

type ReservaItem struct {
	ProdutoID uuid.UUID
	Quantidade int
}

func (s *Store) ReservarItens(ctx context.Context, itens []ReservaItem) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, item := range itens {
		var disponivel int
		if err := tx.QueryRow(ctx,
			`SELECT quantidade_disponivel FROM produtos WHERE id=$1 FOR UPDATE`,
			item.ProdutoID).Scan(&disponivel); err != nil {
			return fmt.Errorf("produto %s: %w", item.ProdutoID, err)
		}
		if disponivel < item.Quantidade {
			return fmt.Errorf("estoque insuficiente produto %s: disponivel=%d solicitado=%d",
				item.ProdutoID, disponivel, item.Quantidade)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE produtos SET quantidade_disponivel = quantidade_disponivel - $1, atualizado_em = $2 WHERE id=$3`,
			item.Quantidade, time.Now().UTC(), item.ProdutoID); err != nil {
			return fmt.Errorf("update produto %s: %w", item.ProdutoID, err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) LiberarItens(ctx context.Context, itens []ReservaItem) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, item := range itens {
		if _, err := tx.Exec(ctx,
			`UPDATE produtos SET quantidade_disponivel = quantidade_disponivel + $1, atualizado_em = $2 WHERE id=$3`,
			item.Quantidade, time.Now().UTC(), item.ProdutoID); err != nil {
			return fmt.Errorf("liberar produto %s: %w", item.ProdutoID, err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) Listar(ctx context.Context) ([]*domain.Produto, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, nome, quantidade_disponivel, preco, atualizado_em FROM produtos ORDER BY nome`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Produto
	for rows.Next() {
		var p domain.Produto
		if err := rows.Scan(&p.ID, &p.Nome, &p.QuantidadeDisponivel, &p.Preco, &p.AtualizadoEm); err != nil {
			continue
		}
		result = append(result, &p)
	}
	return result, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }
