//go:build integration

package db_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-estoque/infra/db"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPostgres(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_DB":       "estoque_test",
			"POSTGRES_USER":     "estoque",
			"POSTGRES_PASSWORD": "estoque123",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	dsn = fmt.Sprintf("postgres://estoque:estoque123@%s:%s/estoque_test?sslmode=disable", host, port.Port())
	cleanup = func() { _ = c.Terminate(ctx) }
	return
}

func TestReservarItensConcorrente(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	os.Setenv("DATABASE_URL", dsn)
	os.Setenv("SHARD_ID", "test")
	os.Unsetenv("REDIS_URL")

	ctx := context.Background()
	store, err := db.New(ctx)
	if err != nil {
		t.Fatalf("db.New: %v", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := store.Seed(ctx); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// "Teclado Mecânico" seed quantity = 5
	prodID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	// Two goroutines each try to reserve 4 items — only one should succeed
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = store.ReservarItens(ctx, []db.ReservaItem{
				{ProdutoID: prodID, Quantidade: 4},
			})
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Errorf("expected exactly 1 success, got %d (errs: %v %v)", successes, errs[0], errs[1])
	}
}
