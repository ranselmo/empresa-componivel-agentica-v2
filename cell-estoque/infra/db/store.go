package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranselmo/poc-eci/cell-estoque/domain"
)

type Store struct{ pool *pgxpool.Pool }

func New(ctx context.Context) (*Store, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://estoque:estoque123@localhost:5434/estoque?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Store{pool: pool}, nil
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
		id   string
		nome string
		qty  int
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
	row := s.pool.QueryRow(ctx,
		`SELECT id, nome, quantidade_disponivel, preco, atualizado_em FROM produtos WHERE id=$1`, id)
	var p domain.Produto
	if err := row.Scan(&p.ID, &p.Nome, &p.QuantidadeDisponivel, &p.Preco, &p.AtualizadoEm); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Store) Salvar(ctx context.Context, p *domain.Produto) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO produtos (id, nome, quantidade_disponivel, preco, atualizado_em)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (id) DO UPDATE SET
			quantidade_disponivel = EXCLUDED.quantidade_disponivel,
			atualizado_em = EXCLUDED.atualizado_em
	`, p.ID, p.Nome, p.QuantidadeDisponivel, p.Preco, p.AtualizadoEm)
	return err
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

func (s *Store) Close() { s.pool.Close() }
