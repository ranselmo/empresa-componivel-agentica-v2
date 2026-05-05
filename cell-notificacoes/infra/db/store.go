package db

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranselmo/poc-eci/cell-notificacoes/domain"
	"github.com/ranselmo/poc-eci/shared/cache"
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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS notificacoes (
			id              UUID PRIMARY KEY,
			destinatario_id UUID NOT NULL,
			tipo            TEXT NOT NULL,
			canal           TEXT NOT NULL,
			conteudo        TEXT NOT NULL,
			enviado_em      TIMESTAMPTZ NOT NULL
		);
	`)
	if err != nil {
		return nil, err
	}

	shardID := os.Getenv("SHARD_ID")
	ca, err := cache.New("notificacoes:", cache.TTLFromEnv(5*time.Second))
	if err != nil {
		slog.Warn("cache unavailable, running without cache", "err", err)
		ca = nil
	}
	return &Store{
		pool:    pool,
		breaker: resilience.NewBreaker("cell-notificacoes", shardID, "db"),
		cache:   ca,
		shardID: shardID,
	}, nil
}

func (s *Store) Salvar(ctx context.Context, n *domain.Notificacao) error {
	err := resilience.Retry(ctx, 3, 100*time.Millisecond, func() error {
		return s.breaker.Execute(func() error {
			_, err := s.pool.Exec(ctx,
				`INSERT INTO notificacoes (id, destinatario_id, tipo, canal, conteudo, enviado_em)
				 VALUES ($1,$2,$3,$4,$5,$6)`,
				n.ID, n.DestinatarioID, n.Tipo, n.Canal, n.Conteudo, n.EnviadoEm)
			return err
		})
	})
	if err == nil && s.cache != nil {
		_ = s.cache.Del(ctx, "list")
	}
	return err
}

func (s *Store) Listar(ctx context.Context) ([]gin.H, error) {
	if s.cache != nil {
		var cached []gin.H
		if hit, _ := s.cache.Get(ctx, "list", &cached); hit {
			return cached, nil
		}
	}

	rows, err := s.pool.Query(ctx,
		`SELECT id, destinatario_id, tipo, canal, conteudo, enviado_em
		 FROM notificacoes ORDER BY enviado_em DESC LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []gin.H
	for rows.Next() {
		var id, dest uuid.UUID
		var tipo, canal, conteudo string
		var enviadoEm time.Time
		if err := rows.Scan(&id, &dest, &tipo, &canal, &conteudo, &enviadoEm); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "destinatario_id": dest, "tipo": tipo,
			"canal": canal, "conteudo": conteudo, "enviado_em": enviadoEm,
		})
	}
	if s.cache != nil && result != nil {
		_ = s.cache.Set(ctx, "list", result)
	}
	return result, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *Store) Close()                         { s.pool.Close() }
