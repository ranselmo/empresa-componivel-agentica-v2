package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
	"github.com/ranselmo/poc-eci/saga-hub/infra/resilience"
)


type SagaStore struct {
	pool    *pgxpool.Pool
	breaker *resilience.Breaker
}

func NewSagaStore(ctx context.Context) (*SagaStore, error) {
	dsn := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	return &SagaStore{
		pool:    pool,
		breaker: resilience.NewBreaker("saga-hub", "", "db"),
	}, nil
}

func (s *SagaStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS sagas (
			id             UUID PRIMARY KEY,
			correlation_id UUID NOT NULL UNIQUE,
			status         TEXT NOT NULL,
			current_step   TEXT NOT NULL,
			cliente_id     UUID NOT NULL,
			shard_id       TEXT NOT NULL,
			payload        JSONB NOT NULL,
			created_at     TIMESTAMPTZ NOT NULL,
			updated_at     TIMESTAMPTZ NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_sagas_status ON sagas(status);
	`)
	return err
}

func (s *SagaStore) Save(ctx context.Context, saga *domain.Saga) error {
	p, _ := json.Marshal(saga.Payload)
	return resilience.Retry(ctx, 3, 100*time.Millisecond, func() error {
		return s.breaker.Execute(func() error {
			_, err := s.pool.Exec(ctx, `
				INSERT INTO sagas (id,correlation_id,status,current_step,cliente_id,shard_id,payload,created_at,updated_at)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
				ON CONFLICT (id) DO UPDATE SET
					status=EXCLUDED.status, current_step=EXCLUDED.current_step,
					payload=EXCLUDED.payload, updated_at=EXCLUDED.updated_at`,
				saga.ID, saga.CorrelationID, string(saga.Status), string(saga.CurrentStep),
				saga.ClienteID, saga.ShardID, p, saga.CreatedAt, saga.UpdatedAt,
			)
			return err
		})
	})
}

func (s *SagaStore) FindByCorrelationID(ctx context.Context, cid uuid.UUID) (*domain.Saga, error) {
	var saga domain.Saga
	var status, step string
	var payload []byte
	err := s.breaker.Execute(func() error {
		row := s.pool.QueryRow(ctx,
			`SELECT id,correlation_id,status,current_step,cliente_id,shard_id,payload,created_at,updated_at
			 FROM sagas WHERE correlation_id=$1`, cid)
		return row.Scan(&saga.ID, &saga.CorrelationID, &status, &step,
			&saga.ClienteID, &saga.ShardID, &payload, &saga.CreatedAt, &saga.UpdatedAt)
	})
	if err != nil {
		return nil, err
	}
	saga.Status = domain.SagaStatus(status)
	saga.CurrentStep = domain.SagaStep(step)
	_ = json.Unmarshal(payload, &saga.Payload)
	return &saga, nil
}

func (s *SagaStore) FindByID(ctx context.Context, id uuid.UUID) (*domain.Saga, error) {
	var saga domain.Saga
	var status, step string
	var payload []byte
	err := s.breaker.Execute(func() error {
		row := s.pool.QueryRow(ctx,
			`SELECT id,correlation_id,status,current_step,cliente_id,shard_id,payload,created_at,updated_at
			 FROM sagas WHERE id=$1`, id)
		return row.Scan(&saga.ID, &saga.CorrelationID, &status, &step,
			&saga.ClienteID, &saga.ShardID, &payload, &saga.CreatedAt, &saga.UpdatedAt)
	})
	if err != nil {
		return nil, err
	}
	saga.Status = domain.SagaStatus(status)
	saga.CurrentStep = domain.SagaStep(step)
	_ = json.Unmarshal(payload, &saga.Payload)
	return &saga, nil
}

func (s *SagaStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
func (s *SagaStore) Close()                         { s.pool.Close() }
