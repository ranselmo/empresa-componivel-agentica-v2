//go:build integration

package db_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/cell-pedidos/domain"
	"github.com/ranselmo/poc-eci/cell-pedidos/infra/db"
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
			"POSTGRES_DB":       "pedidos_test",
			"POSTGRES_USER":     "pedidos",
			"POSTGRES_PASSWORD": "pedidos123",
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
	dsn = fmt.Sprintf("postgres://pedidos:pedidos123@%s:%s/pedidos_test?sslmode=disable", host, port.Port())
	cleanup = func() { _ = c.Terminate(ctx) }
	return
}

func TestStoreSalvarBuscar(t *testing.T) {
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

	clienteID := uuid.New()
	pedido, _ := domain.NewPedido(clienteID, []domain.ItemPedido{
		{ProdutoID: uuid.New(), Quantidade: 2, PrecoUnit: 100.0},
	})

	if err := store.Salvar(ctx, pedido); err != nil {
		t.Fatalf("Salvar: %v", err)
	}

	got, err := store.BuscarPorID(ctx, pedido.ID)
	if err != nil {
		t.Fatalf("BuscarPorID: %v", err)
	}
	if got.ID != pedido.ID {
		t.Errorf("got id %v, want %v", got.ID, pedido.ID)
	}
	if got.ClienteID != clienteID {
		t.Errorf("got clienteID %v, want %v", got.ClienteID, clienteID)
	}
}

func TestStoreListarCursorPagination(t *testing.T) {
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

	clienteID := uuid.New()
	for i := 0; i < 5; i++ {
		p, _ := domain.NewPedido(clienteID, []domain.ItemPedido{{ProdutoID: uuid.New(), Quantidade: 1, PrecoUnit: 10}})
		if err := store.Salvar(ctx, p); err != nil {
			t.Fatalf("Salvar: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	page1, err := store.Listar(ctx, time.Time{}, 3)
	if err != nil {
		t.Fatalf("Listar p1: %v", err)
	}
	if len(page1) != 3 {
		t.Errorf("page1 len=%d want 3", len(page1))
	}

	cursor := page1[len(page1)-1].CriadoEm
	page2, err := store.Listar(ctx, cursor, 3)
	if err != nil {
		t.Fatalf("Listar p2: %v", err)
	}
	if len(page2) != 2 {
		t.Errorf("page2 len=%d want 2", len(page2))
	}
}
