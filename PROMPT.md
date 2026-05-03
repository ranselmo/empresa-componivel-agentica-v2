# CLAUDE CODE — MASTER PROMPT v3

# ECI: Empresa Componível Inteligente — Arquitetura Celular com Shards

# Linguagem: Go 1.22 | Python 3.12 (agent-mcp único)

# Regra zero: código que não compila não existe

---

## §0 — ARQUITETURA (leia, não implemente ainda)

```
Internet
   │
[Traefik :80]
   │
[shard-router :8080]  ←── único entry point de negócio
   │  hash(X-Client-ID) % 3
   ├── shard-1 (clientes hash=1)
   │     ├── cell-pedidos-s1-active   :8001  ←── DB pedidos-s1-a
   │     ├── cell-pedidos-s1-passive  :8002  ←── DB pedidos-s1-p (CDC)
   │     ├── cell-estoque-s1-active   :8003  ←── DB estoque-s1-a
   │     ├── cell-estoque-s1-passive  :8004  ←── DB estoque-s1-p (CDC)
   │     ├── cell-notif-s1-active     :8005
   │     └── cell-notif-s1-passive    :8006
   ├── shard-2 (clientes hash=2) — portas :8011–:8016
   └── shard-3 (clientes hash=3) — portas :8021–:8026

[saga-hub :9090]   ←── ÚNICO integrador entre PBCs
   │  Async-Request-Reply via Kafka correlation_id
   │  PBCs nunca se chamam entre si
   │
[Kafka]  tópicos:
   commands.{pbc}.{acao}   → saga-hub publica, PBC consome
   replies.{pbc}.{acao}    → PBC publica, saga-hub consome
   events.{pbc}.{tipo}     → PBC publica (democratizado, qualquer um lê)
   cdc.{shard}.{pbc}.{table} → Debezium publica, data-sync consome

[data-sync]  ←── CDC ativo→passivo dentro de cada shard

[Prometheus + Grafana + Jaeger + Alertmanager]
```

### Leis arquiteturais (violação = `go build` com `//go:build ignore` no arquivo infrator)

```
L1: PBCs são ilhas. cell-pedidos, cell-estoque, cell-notificacoes:
    - ZERO imports de pacotes de outros PBCs
    - ZERO chamadas HTTP para outros PBCs
    - ZERO subscrição de tópicos de outros PBCs ou saga-hub

L2: saga-hub é o único integrador. Só ele conhece múltiplos PBCs.

L3: Toda requisição de negócio entra pelo shard-router.
    PBCs recebem apenas via proxy do router OU comandos Kafka do saga-hub.

L4: Células passivas recusam escrita (HTTP 503) e não processam comandos Kafka.
    Dados chegam exclusivamente via data-sync CDC.

L5: Scale Unit = 1 célula = 1 binary + 1 DB + 1 Redis + 1 consumer group Kafka.
    Nunca compartilhe DB ou Redis entre células.
```

---

## §1 — REGRAS DE EXECUÇÃO

```
R1  Leia antes de escrever: find . -name "*.go" | head -20
R2  Compile após CADA arquivo criado/modificado: cd <componente> && go build ./...
R3  Build quebrado = pare. Não crie arquivo novo enquanto há erro de compilação.
R4  Sem stubs. Funções que retornam (nil, nil) ou // TODO são proibidas.
R5  go.mod independente por componente. Nunca use go.work.
R6  Após qualquer go.mod: go mod tidy && go mod verify && go build ./...
R7  Commits: feat(<componente>/<item>): <descrição> — um commit por item concluído
R8  Ordem obrigatória: §3(F0) completo → §4(F1) → §5(F2) → §6(F3) → §7(F4) → §8(F5)
    Não inicie fase seguinte com checklist da fase anterior incompleto.
```

---

## §2 — ESTRUTURA DE DIRETÓRIOS

```
poc-eci-go/
├── shard-router/          go.mod · domain/ · infra/ · api/ · cmd/main.go · Dockerfile
├── saga-hub/              go.mod · domain/ · infra/db/ · infra/messaging/ · orchestrator/ · api/ · cmd/main.go · Dockerfile
├── data-sync/             go.mod · domain/ · infra/ · cmd/main.go · Dockerfile
├── cell-pedidos/          go.mod · domain/ · infra/db/ · infra/cache/ · infra/messaging/ · infra/resilience/ · api/ · cmd/main.go · Dockerfile
├── cell-estoque/          (mesma estrutura)
├── cell-notificacoes/     (mesma estrutura)
├── agent-mcp/             Python — não modificar estrutura Go
├── fitness-functions/     run_all.py (atualizar com novos checks)
├── k8s/                   namespace · shard-router · saga-hub · data-sync · shards/{1,2,3}/ · infra/
├── infra/monitoring/      prometheus.yml · alert-rules.yml · slo-rules.yml · dashboards/
├── runbooks/
└── docker-compose.yml     (reescrever completo)
```

---

## §3 — FASE 0: FUNDAÇÃO (implementar por completo antes de qualquer outra fase)

### F0.1 — shard-router

**`shard-router/go.mod`**

```
module github.com/ranselmo/poc-eci/shard-router
go 1.22
require (
    github.com/gin-gonic/gin v1.10.0
    github.com/google/uuid v1.6.0
    github.com/prometheus/client_golang v1.19.0
    go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.49.0
    go.opentelemetry.io/otel v1.24.0
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
    go.opentelemetry.io/otel/sdk v1.24.0
    go.opentelemetry.io/otel/semconv/v1.24.0 v1.24.0
)
```

**`shard-router/domain/routing.go`**

```go
package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const TotalShards = 3

type ShardID = string
type CellRole = string

const (
	RoleActive  CellRole = "active"
	RolePassive CellRole = "passive"
)

// Route retorna "shard-1", "shard-2" ou "shard-3"
func Route(routingKey string) ShardID {
	h := sha256.Sum256([]byte(routingKey))
	n := binary.BigEndian.Uint64(h[:8])
	return fmt.Sprintf("shard-%d", (n%TotalShards)+1)
}
```

**`shard-router/infra/registry.go`**

```go
package infra

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ranselmo/poc-eci/shard-router/domain"
)

type CellEntry struct {
	ID      string
	ShardID domain.ShardID
	PBC     string
	Role    domain.CellRole
	BaseURL string
	Healthy bool
	Updated time.Time
}

type Registry struct {
	mu    sync.RWMutex
	cells map[string]*CellEntry // key: "shard-1:pedidos:active"
}

func NewRegistry() *Registry {
	r := &Registry{cells: make(map[string]*CellEntry)}
	pbcs := []string{"pedidos", "estoque", "notificacoes"}
	roles := []string{domain.RoleActive, domain.RolePassive}
	for i := 1; i <= domain.TotalShards; i++ {
		for _, pbc := range pbcs {
			for _, role := range roles {
				envKey := fmt.Sprintf("SHARD%d_%s_%s_URL",
					i, strings.ToUpper(pbc), strings.ToUpper(role))
				url := os.Getenv(envKey)
				if url == "" {
					continue
				}
				key := fmt.Sprintf("shard-%d:%s:%s", i, pbc, role)
				r.cells[key] = &CellEntry{
					ID:      fmt.Sprintf("cell-%s-s%d-%s", pbc, i, role),
					ShardID: fmt.Sprintf("shard-%d", i),
					PBC:     pbc,
					Role:    role,
					BaseURL: url,
					Healthy: true,
					Updated: time.Now(),
				}
			}
		}
	}
	return r
}

// ActiveCell retorna célula ativa; se down, faz failover para passiva.
func (r *Registry) ActiveCell(shardID, pbc string) (*CellEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.cells[shardID+":"+pbc+":active"]; e != nil && e.Healthy {
		return e, false // false = não é failover
	}
	if e := r.cells[shardID+":"+pbc+":passive"]; e != nil && e.Healthy {
		return e, true // true = failover aconteceu
	}
	return nil, false
}

func (r *Registry) SetHealthy(shardID, pbc, role string, healthy bool) {
	key := shardID + ":" + pbc + ":" + role
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.cells[key]; e != nil {
		e.Healthy = healthy
		e.Updated = time.Now()
	}
}

func (r *Registry) Snapshot() []CellEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CellEntry, 0, len(r.cells))
	for _, e := range r.cells {
		out = append(out, *e)
	}
	return out
}
```

**`shard-router/infra/watcher.go`**

```go
package infra

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type HealthWatcher struct {
	reg    *Registry
	client *http.Client
}

func NewHealthWatcher(reg *Registry) *HealthWatcher {
	return &HealthWatcher{
		reg:    reg,
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func (w *HealthWatcher) Run(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, cell := range w.reg.Snapshot() {
				go w.check(ctx, cell)
			}
		}
	}
}

func (w *HealthWatcher) check(ctx context.Context, cell CellEntry) {
	url := fmt.Sprintf("%s/healthz/ready", cell.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		w.reg.SetHealthy(cell.ShardID, cell.PBC, cell.Role, false)
		return
	}
	resp, err := w.client.Do(req)
	healthy := err == nil && resp != nil && resp.StatusCode == 200
	if resp != nil {
		resp.Body.Close()
	}
	w.reg.SetHealthy(cell.ShardID, cell.PBC, cell.Role, healthy)
	if !healthy {
		slog.Warn("cell unhealthy", "cell", cell.ID, "shard", cell.ShardID, "role", cell.Role)
	}
}
```

**`shard-router/api/handlers.go`**

```go
package api

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ranselmo/poc-eci/shard-router/domain"
	"github.com/ranselmo/poc-eci/shard-router/infra"
)

var (
	reqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shard_router_requests_total",
	}, []string{"shard", "pbc", "role"})

	reqDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shard_router_duration_seconds",
		Buckets: []float64{.001, .005, .01, .05, .1, .5},
	}, []string{"shard", "pbc"})

	failoverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shard_router_failover_total",
	}, []string{"shard", "pbc"})

	cellHealth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shard_router_cell_health",
		Help: "1=healthy 0=unhealthy",
	}, []string{"shard", "pbc", "role", "cell_id"})
)

type Handler struct{ reg *infra.Registry }

func New(reg *infra.Registry) *Handler { return &Handler{reg: reg} }

func (h *Handler) Register(r *gin.Engine) {
	r.Any("/pedidos/*path",      h.proxy("pedidos"))
	r.Any("/estoque/*path",      h.proxy("estoque"))
	r.Any("/notificacoes/*path", h.proxy("notificacoes"))
	r.GET("/healthz/live",  func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/healthz/ready", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/router/cells",  h.cells)
}

func (h *Handler) proxy(pbc string) gin.HandlerFunc {
	return func(c *gin.Context) {
		t0 := time.Now()

		key := c.GetHeader("X-Client-ID")
		if key == "" {
			key = c.Query("cliente_id")
		}
		if key == "" {
			key = "shard-1-default"
		}

		shard := domain.Route(key)
		cell, isFailover := h.reg.ActiveCell(shard, pbc)
		if cell == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "no healthy cell", "shard": shard, "pbc": pbc,
			})
			return
		}
		if isFailover {
			failoverTotal.WithLabelValues(shard, pbc).Inc()
			slog.Warn("failover active", "shard", shard, "pbc", pbc, "cell", cell.ID)
		}

		c.Request.Header.Set("X-Shard-ID", shard)
		c.Request.Header.Set("X-Cell-ID", cell.ID)
		c.Request.Header.Set("X-Cell-Role", cell.Role)

		target, _ := url.Parse(cell.BaseURL)
		rp := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = target.Scheme
				req.URL.Host = target.Host
				req.Host = target.Host
			},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.Error("proxy error", "cell", cell.ID, "err", err)
				w.WriteHeader(http.StatusBadGateway)
			},
		}
		rp.ServeHTTP(c.Writer, c.Request)

		reqTotal.WithLabelValues(shard, pbc, cell.Role).Inc()
		reqDur.WithLabelValues(shard, pbc).Observe(time.Since(t0).Seconds())
	}
}

func (h *Handler) cells(c *gin.Context) {
	snap := h.reg.Snapshot()
	for _, e := range snap {
		v := 0.0
		if e.Healthy {
			v = 1.0
		}
		cellHealth.WithLabelValues(e.ShardID, e.PBC, e.Role, e.ID).Set(v)
	}
	c.JSON(200, gin.H{"cells": snap})
}
```

**`shard-router/cmd/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/shard-router/api"
	"github.com/ranselmo/poc-eci/shard-router/infra"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func setupOTel(ctx context.Context) func() {
	ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep == "" {
		ep = "http://jaeger:4317"
	}
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(ep),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		slog.Warn("otel init failed", "err", err)
		return func() {}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName("shard-router"),
		)),
	)
	otel.SetTracerProvider(tp)
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdown := setupOTel(ctx)
	defer shutdown()

	reg := infra.NewRegistry()
	go infra.NewHealthWatcher(reg).Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("shard-router"))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	api.New(reg).Register(r)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("shard-router up", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = srv.Shutdown(ctx2)
}
```

**`shard-router/Dockerfile`** (multi-stage, librdkafka não necessária aqui)

```dockerfile
FROM golang:1.22-bookworm AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /shard-router ./cmd/
FROM gcr.io/distroless/static-debian12
COPY --from=build /shard-router /shard-router
EXPOSE 8080
ENTRYPOINT ["/shard-router"]
```

**Critério F0.1:**

```bash
cd shard-router && go mod tidy && go build ./... && echo "PASS F0.1"
```

---

### F0.2 — saga-hub

**`saga-hub/go.mod`**

```
module github.com/ranselmo/poc-eci/saga-hub
go 1.22
require (
    github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
    github.com/gin-gonic/gin v1.10.0
    github.com/google/uuid v1.6.0
    github.com/jackc/pgx/v5 v5.5.5
    github.com/prometheus/client_golang v1.19.0
    go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.49.0
    go.opentelemetry.io/otel v1.24.0
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
    go.opentelemetry.io/otel/sdk v1.24.0
    go.opentelemetry.io/otel/semconv/v1.24.0 v1.24.0
)
```

**`saga-hub/domain/saga.go`**

```go
package domain

import (
	"time"
	"github.com/google/uuid"
)

type SagaStatus string
type SagaStep   string

const (
	StatusStarted      SagaStatus = "STARTED"
	StatusCompleted    SagaStatus = "COMPLETED"
	StatusCompensating SagaStatus = "COMPENSATING"
	StatusFailed       SagaStatus = "FAILED"

	StepCriarPedido       SagaStep = "CRIAR_PEDIDO"
	StepReservarEstoque   SagaStep = "RESERVAR_ESTOQUE"
	StepEnviarNotificacao SagaStep = "ENVIAR_NOTIFICACAO"
	StepCompensarPedido   SagaStep = "COMPENSAR_PEDIDO"
)

// Tópicos Kafka — contrato único para todo o sistema
const (
	// saga-hub → PBC
	TopicCmdPedidoCriar       = "commands.pedidos.criar"
	TopicCmdPedidoCancelar    = "commands.pedidos.cancelar"
	TopicCmdEstoqueReservar   = "commands.estoque.reservar"
	TopicCmdNotificacaoEnviar = "commands.notificacoes.enviar"
	// PBC → saga-hub
	TopicReplyPedidoCriado         = "replies.pedidos.criado"
	TopicReplyPedidoCancelado      = "replies.pedidos.cancelado"
	TopicReplyEstoqueReservado     = "replies.estoque.reservado"
	TopicReplyEstoqueInsuficiente  = "replies.estoque.insuficiente"
	TopicReplyNotificacaoEnviada   = "replies.notificacoes.enviada"
	// PBC → todos (democratizado)
	TopicEventPedidoConfirmado  = "events.pedidos.confirmado"
	TopicEventPedidoCancelado   = "events.pedidos.cancelado"
	TopicEventNotificacaoEnviada = "events.notificacoes.enviada"
)

type Saga struct {
	ID            uuid.UUID
	CorrelationID uuid.UUID
	Status        SagaStatus
	CurrentStep   SagaStep
	ClienteID     uuid.UUID
	ShardID       string
	Payload       map[string]any
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewSaga(clienteID uuid.UUID, shardID string, payload map[string]any) *Saga {
	id := uuid.New()
	now := time.Now().UTC()
	return &Saga{
		ID: id, CorrelationID: id,
		Status: StatusStarted, CurrentStep: StepCriarPedido,
		ClienteID: clienteID, ShardID: shardID, Payload: payload,
		CreatedAt: now, UpdatedAt: now,
	}
}

// Command é a mensagem que saga-hub envia a um PBC
type Command struct {
	CommandID     uuid.UUID      `json:"command_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	ShardID       string         `json:"shard_id"`
	CommandType   string         `json:"command_type"`
	Payload       map[string]any `json:"payload"`
	IssuedAt      time.Time      `json:"issued_at"`
}

// Reply é a mensagem que um PBC envia de volta ao saga-hub
type Reply struct {
	ReplyID       uuid.UUID      `json:"reply_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	CommandType   string         `json:"command_type"`
	Status        string         `json:"status"` // "success" | "failure"
	Payload       map[string]any `json:"payload"`
	Error         string         `json:"error,omitempty"`
	RepliedAt     time.Time      `json:"replied_at"`
}

// BusinessEvent é publicado em events.* após saga concluída (democratizado)
type BusinessEvent struct {
	EventID    uuid.UUID      `json:"event_id"`
	EventType  string         `json:"event_type"`
	ShardID    string         `json:"shard_id"`
	OccurredAt time.Time      `json:"occurred_at"`
	Payload    map[string]any `json:"payload"`
}
```

**`saga-hub/infra/db/store.go`**

```go
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
)

type SagaStore struct{ pool *pgxpool.Pool }

func NewSagaStore(ctx context.Context) (*SagaStore, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://saga:saga123@db-saga:5432/saga?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	return &SagaStore{pool: pool}, nil
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
}

func (s *SagaStore) FindByCorrelationID(ctx context.Context, cid uuid.UUID) (*domain.Saga, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id,correlation_id,status,current_step,cliente_id,shard_id,payload,created_at,updated_at
		 FROM sagas WHERE correlation_id=$1`, cid)
	var saga domain.Saga
	var status, step string
	var payload []byte
	err := row.Scan(&saga.ID, &saga.CorrelationID, &status, &step,
		&saga.ClienteID, &saga.ShardID, &payload, &saga.CreatedAt, &saga.UpdatedAt)
	if err != nil {
		return nil, err
	}
	saga.Status = domain.SagaStatus(status)
	saga.CurrentStep = domain.SagaStep(step)
	_ = json.Unmarshal(payload, &saga.Payload)
	return &saga, nil
}

func (s *SagaStore) FindByID(ctx context.Context, id uuid.UUID) (*domain.Saga, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id,correlation_id,status,current_step,cliente_id,shard_id,payload,created_at,updated_at
		 FROM sagas WHERE id=$1`, id)
	var saga domain.Saga
	var status, step string
	var payload []byte
	err := row.Scan(&saga.ID, &saga.CorrelationID, &status, &step,
		&saga.ClienteID, &saga.ShardID, &payload, &saga.CreatedAt, &saga.UpdatedAt)
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
```

**`saga-hub/infra/messaging/producer.go`**

```go
package messaging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
)

type Producer struct{ p *kafka.Producer }

func NewProducer() (*Producer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"retries":           3,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery failed", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) PublishCommand(topic string, cmd domain.Command) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	key := cmd.CorrelationID.String()
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          b,
	}, nil)
}

func (pr *Producer) PublishBusinessEvent(topic string, ev domain.BusinessEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		slog.Error("marshal business event", "err", err)
		return
	}
	key := ev.EventID.String()
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          b,
	}, nil)
}

func (pr *Producer) Flush() { pr.p.Flush(5000) }
func (pr *Producer) Close() { pr.p.Close() }
```

**`saga-hub/infra/messaging/consumer.go`**

```go
package messaging

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
)

type ReplyHandler func(ctx context.Context, reply domain.Reply) error

type Consumer struct {
	c       *kafka.Consumer
	handler ReplyHandler
}

func NewConsumer(handler ReplyHandler) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "saga-hub-replies-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		return nil, err
	}
	topics := []string{
		domain.TopicReplyPedidoCriado,
		domain.TopicReplyPedidoCancelado,
		domain.TopicReplyEstoqueReservado,
		domain.TopicReplyEstoqueInsuficiente,
		domain.TopicReplyNotificacaoEnviada,
	}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}
	return &Consumer{c: c, handler: handler}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	slog.Info("saga-hub reply consumer started")
	for {
		select {
		case <-ctx.Done():
			cs.c.Close()
			return
		default:
			msg, err := cs.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(err.Error(), "timed out") {
					slog.Error("kafka read", "err", err)
				}
				continue
			}
			var reply domain.Reply
			if err := json.Unmarshal(msg.Value, &reply); err != nil {
				slog.Error("unmarshal reply", "err", err)
				continue
			}
			if err := cs.handler(ctx, reply); err != nil {
				slog.Error("handle reply", "err", err,
					"correlation_id", reply.CorrelationID, "type", reply.CommandType)
			}
		}
	}
}
```

**`saga-hub/orchestrator/pedido.go`**

```go
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ranselmo/poc-eci/saga-hub/domain"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/infra/messaging"
)

var (
	sagaStarted = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "saga_started_total"}, []string{"type"})
	sagaDone = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "saga_completed_total"}, []string{"type", "outcome"})
	sagaDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "saga_duration_seconds",
		Buckets: []float64{.1, .5, 1, 2, 5, 10, 30},
	}, []string{"type"})
)

type PedidoSaga struct {
	store *db.SagaStore
	prod  *messaging.Producer
}

func NewPedidoSaga(store *db.SagaStore, prod *messaging.Producer) *PedidoSaga {
	return &PedidoSaga{store: store, prod: prod}
}

func (o *PedidoSaga) Start(ctx context.Context, clienteID uuid.UUID, shardID string, payload map[string]any) (*domain.Saga, error) {
	saga := domain.NewSaga(clienteID, shardID, payload)
	if err := o.store.Save(ctx, saga); err != nil {
		return nil, fmt.Errorf("save saga: %w", err)
	}
	sagaStarted.WithLabelValues("pedido").Inc()
	cmd := domain.Command{
		CommandID: uuid.New(), CorrelationID: saga.CorrelationID,
		SagaID: saga.ID, ShardID: shardID,
		CommandType: "criar_pedido", Payload: payload,
		IssuedAt: time.Now().UTC(),
	}
	if err := o.prod.PublishCommand(domain.TopicCmdPedidoCriar, cmd); err != nil {
		return nil, fmt.Errorf("publish cmd: %w", err)
	}
	slog.Info("saga started", "saga_id", saga.ID, "shard", shardID)
	return saga, nil
}

func (o *PedidoSaga) HandleReply(ctx context.Context, reply domain.Reply) error {
	saga, err := o.store.FindByCorrelationID(ctx, reply.CorrelationID)
	if err != nil {
		return fmt.Errorf("saga not found correlation_id=%s", reply.CorrelationID)
	}
	switch reply.CommandType {
	case "criar_pedido":
		return o.onPedidoCriado(ctx, saga, reply)
	case "reservar_estoque":
		return o.onEstoqueReply(ctx, saga, reply)
	case "enviar_notificacao":
		return o.onNotificacaoReply(ctx, saga, reply)
	case "cancelar_pedido":
		return o.onPedidoCancelado(ctx, saga, reply)
	default:
		return fmt.Errorf("unknown command_type: %s", reply.CommandType)
	}
}

func (o *PedidoSaga) newCmd(saga *domain.Saga, cmdType string, payload map[string]any) domain.Command {
	return domain.Command{
		CommandID: uuid.New(), CorrelationID: saga.CorrelationID,
		SagaID: saga.ID, ShardID: saga.ShardID,
		CommandType: cmdType, Payload: payload,
		IssuedAt: time.Now().UTC(),
	}
}

func (o *PedidoSaga) onPedidoCriado(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	if reply.Status == "failure" {
		return o.failSaga(ctx, saga, reply.Error)
	}
	saga.CurrentStep = domain.StepReservarEstoque
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	return o.prod.PublishCommand(domain.TopicCmdEstoqueReservar,
		o.newCmd(saga, "reservar_estoque", saga.Payload))
}

func (o *PedidoSaga) onEstoqueReply(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	if reply.Status == "failure" {
		saga.Status = domain.StatusCompensating
		saga.CurrentStep = domain.StepCompensarPedido
		saga.UpdatedAt = time.Now().UTC()
		_ = o.store.Save(ctx, saga)
		return o.prod.PublishCommand(domain.TopicCmdPedidoCancelar,
			o.newCmd(saga, "cancelar_pedido", map[string]any{"motivo": reply.Error}))
	}
	saga.CurrentStep = domain.StepEnviarNotificacao
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	return o.prod.PublishCommand(domain.TopicCmdNotificacaoEnviar,
		o.newCmd(saga, "enviar_notificacao", map[string]any{
			"cliente_id": saga.ClienteID.String(),
			"tipo":       "PEDIDO_CONFIRMADO",
			"payload":    saga.Payload,
		}))
}

func (o *PedidoSaga) onNotificacaoReply(ctx context.Context, saga *domain.Saga, reply domain.Reply) error {
	saga.Status = domain.StatusCompleted
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "success").Inc()
	sagaDur.WithLabelValues("pedido").Observe(time.Since(saga.CreatedAt).Seconds())
	o.prod.PublishBusinessEvent(domain.TopicEventPedidoConfirmado, domain.BusinessEvent{
		EventID: uuid.New(), EventType: "PedidoConfirmado",
		ShardID: saga.ShardID, OccurredAt: time.Now().UTC(), Payload: saga.Payload,
	})
	slog.Info("saga completed", "saga_id", saga.ID)
	return nil
}

func (o *PedidoSaga) onPedidoCancelado(ctx context.Context, saga *domain.Saga, _ domain.Reply) error {
	saga.Status = domain.StatusFailed
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "compensated").Inc()
	o.prod.PublishBusinessEvent(domain.TopicEventPedidoCancelado, domain.BusinessEvent{
		EventID: uuid.New(), EventType: "PedidoCancelado",
		ShardID: saga.ShardID, OccurredAt: time.Now().UTC(), Payload: saga.Payload,
	})
	// Notifica cliente do cancelamento
	_ = o.prod.PublishCommand(domain.TopicCmdNotificacaoEnviar,
		o.newCmd(saga, "enviar_notificacao", map[string]any{
			"cliente_id": saga.ClienteID.String(),
			"tipo":       "PEDIDO_CANCELADO",
		}))
	slog.Warn("saga compensated", "saga_id", saga.ID)
	return nil
}

func (o *PedidoSaga) failSaga(ctx context.Context, saga *domain.Saga, reason string) error {
	saga.Status = domain.StatusFailed
	saga.UpdatedAt = time.Now().UTC()
	_ = o.store.Save(ctx, saga)
	sagaDone.WithLabelValues("pedido", "failed").Inc()
	slog.Error("saga failed", "saga_id", saga.ID, "reason", reason)
	return nil
}
```

**`saga-hub/api/handlers.go`**

```go
package api

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/orchestrator"
)

type Handler struct {
	saga  *orchestrator.PedidoSaga
	store *db.SagaStore
}

func New(saga *orchestrator.PedidoSaga, store *db.SagaStore) *Handler {
	return &Handler{saga: saga, store: store}
}

func (h *Handler) Register(r *gin.Engine) {
	r.POST("/saga/pedido",  h.iniciar)
	r.GET("/saga/:id",      h.consultar)
	r.GET("/healthz/live",  func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/healthz/ready", h.ready)
}

type IniciarRequest struct {
	ClienteID string         `json:"cliente_id" binding:"required"`
	ShardID   string         `json:"shard_id"   binding:"required"`
	Payload   map[string]any `json:"payload"    binding:"required"`
}

func (h *Handler) iniciar(c *gin.Context) {
	var req IniciarRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	clienteID, err := uuid.Parse(req.ClienteID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cliente_id"})
		return
	}
	saga, err := h.saga.Start(c.Request.Context(), clienteID, req.ShardID, req.Payload)
	if err != nil {
		slog.Error("start saga", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"saga_id":        saga.ID,
		"correlation_id": saga.CorrelationID,
		"status":         saga.Status,
	})
}

func (h *Handler) consultar(c *gin.Context) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	saga, err := h.store.FindByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "saga not found"})
		return
	}
	c.JSON(200, gin.H{
		"saga_id":      saga.ID,
		"status":       saga.Status,
		"current_step": saga.CurrentStep,
		"shard_id":     saga.ShardID,
		"created_at":   saga.CreatedAt,
		"updated_at":   saga.UpdatedAt,
	})
}

func (h *Handler) ready(c *gin.Context) {
	if err := h.store.Ping(c.Request.Context()); err != nil {
		c.JSON(503, gin.H{"status": "db unhealthy", "error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}
```

**`saga-hub/cmd/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sagaapi "github.com/ranselmo/poc-eci/saga-hub/api"
	"github.com/ranselmo/poc-eci/saga-hub/infra/db"
	"github.com/ranselmo/poc-eci/saga-hub/infra/messaging"
	"github.com/ranselmo/poc-eci/saga-hub/orchestrator"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

func setupOTel(ctx context.Context, svcName string) func() {
	ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep == "" { ep = "http://jaeger:4317" }
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(ep), otlptracegrpc.WithInsecure())
	if err != nil { slog.Warn("otel init", "err", err); return func() {} }
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(svcName))),
	)
	otel.SetTracerProvider(tp)
	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer setupOTel(ctx, "saga-hub")()

	store, err := db.NewSagaStore(ctx)
	if err != nil { slog.Error("db", "err", err); os.Exit(1) }
	defer store.Close()
	if err := store.Migrate(ctx); err != nil { slog.Error("migrate", "err", err); os.Exit(1) }

	prod, err := messaging.NewProducer()
	if err != nil { slog.Error("producer", "err", err); os.Exit(1) }
	defer prod.Close()

	orch := orchestrator.NewPedidoSaga(store, prod)

	cons, err := messaging.NewConsumer(orch.HandleReply)
	if err != nil { slog.Error("consumer", "err", err); os.Exit(1) }
	go cons.Run(ctx)

	r := gin.New()
	r.Use(gin.Recovery(), otelgin.Middleware("saga-hub"))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	sagaapi.New(orch, store).Register(r)

	port := os.Getenv("PORT")
	if port == "" { port = "9090" }
	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		slog.Info("saga-hub up", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	ctx2, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = srv.Shutdown(ctx2)
}
```

**Critério F0.2:**

```bash
cd saga-hub && go mod tidy && go build ./... && echo "PASS F0.2"
```

---

### F0.3 — PBCs: refatoração para Async-Request-Reply

Aplique a cada célula existente (`cell-pedidos`, `cell-estoque`, `cell-notificacoes`).

**Regra de boundary que DEVE compilar sem erro** — adicione em cada `cmd/main.go`:

```go
// Linha comentada — serve de documentação e como lembrete do constraint
// Se qualquer import abaixo aparecer, o build falha intencionalmente:
// _ "github.com/ranselmo/poc-eci/cell-pedidos"    // PROIBIDO em cell-estoque
// _ "github.com/ranselmo/poc-eci/cell-estoque"    // PROIBIDO em cell-pedidos
// _ "github.com/ranselmo/poc-eci/cell-notificacoes" // PROIBIDO nos outros
```

**Padrão de consumer para cada PBC** (substitua `{pbc}` e `{Pbc}` pelo nome):

```go
// cell-{pbc}/infra/messaging/consumer.go
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

// Tópicos exclusivos deste PBC — nunca importar tópicos de outros PBCs
const (
	// PBC recebe
	TopicCmdCriar    = "commands.{pbc}.criar"
	TopicCmdCancelar = "commands.{pbc}.cancelar"  // adaptar por PBC
	// PBC envia ao saga-hub
	TopicReplyCriado   = "replies.{pbc}.criado"
	TopicReplyCancelado = "replies.{pbc}.cancelado"
	// PBC democratiza (qualquer sistema pode consumir)
	TopicEventOperado = "events.{pbc}.operado"
)

type Command struct {
	CommandID     uuid.UUID      `json:"command_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	ShardID       string         `json:"shard_id"`
	CommandType   string         `json:"command_type"`
	Payload       map[string]any `json:"payload"`
	IssuedAt      time.Time      `json:"issued_at"`
}

type Reply struct {
	ReplyID       uuid.UUID      `json:"reply_id"`
	CorrelationID uuid.UUID      `json:"correlation_id"`
	SagaID        uuid.UUID      `json:"saga_id"`
	CommandType   string         `json:"command_type"`
	Status        string         `json:"status"`
	Payload       map[string]any `json:"payload"`
	Error         string         `json:"error,omitempty"`
	RepliedAt     time.Time      `json:"replied_at"`
}

type CommandProcessor interface {
	ProcessCommand(ctx context.Context, cmd Command) (Reply, error)
}

type Consumer struct {
	c    *kafka.Consumer
	proc CommandProcessor
	prod *Producer
}

func NewConsumer(proc CommandProcessor, prod *Producer) (*Consumer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" { brokers = "kafka:29092" }

	shardID  := os.Getenv("SHARD_ID")   // ex: "shard-1"
	cellRole := os.Getenv("CELL_ROLE")  // "active" | "passive"

	// Células passivas NÃO consomem comandos — apenas dados via CDC
	if cellRole == "passive" {
		slog.Info("passive cell — command consumer disabled", "shard", shardID)
		return &Consumer{}, nil // consumer no-op
	}

	// Consumer group único por shard + role: garante que cada shard
	// tem seu próprio consumer group e não interfere com outros shards
	groupID := fmt.Sprintf("cell-{pbc}-%s-%s-cmd", shardID, cellRole)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil { return nil, err }

	if err := c.SubscribeTopics([]string{TopicCmdCriar, TopicCmdCancelar}, nil); err != nil {
		return nil, err
	}
	slog.Info("command consumer started", "group", groupID, "shard", shardID)
	return &Consumer{c: c, proc: proc, prod: prod}, nil
}

func (cs *Consumer) Run(ctx context.Context) {
	if cs.c == nil { // passive mode — no-op
		<-ctx.Done()
		return
	}
	for {
		select {
		case <-ctx.Done():
			cs.c.Close()
			return
		default:
			msg, err := cs.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(err.Error(), "timed out") {
					slog.Error("kafka read", "err", err)
				}
				continue
			}
			var cmd Command
			if err := json.Unmarshal(msg.Value, &cmd); err != nil {
				slog.Error("unmarshal cmd", "err", err)
				continue
			}
			reply, err := cs.proc.ProcessCommand(ctx, cmd)
			if err != nil {
				reply = Reply{
					ReplyID: uuid.New(), CorrelationID: cmd.CorrelationID,
					SagaID: cmd.SagaID, CommandType: cmd.CommandType,
					Status: "failure", Error: err.Error(),
					RepliedAt: time.Now().UTC(),
				}
			}
			replyTopic := replyTopicFor(cmd.CommandType)
			if err := cs.prod.PublishReply(replyTopic, reply); err != nil {
				slog.Error("publish reply", "err", err)
			}
		}
	}
}

func replyTopicFor(cmdType string) string {
	switch cmdType {
	case "criar_{pbc}":
		return TopicReplyCriado
	case "cancelar_{pbc}":
		return TopicReplyCancelado
	default:
		return TopicReplyCriado
	}
}
```

**Padrão de producer para cada PBC:**

```go
// cell-{pbc}/infra/messaging/producer.go
package messaging

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"time"
)

type Producer struct{ p *kafka.Producer }

func NewProducer() (*Producer, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" { brokers = "kafka:29092" }
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"acks":              "all",
		"retries":           3,
	})
	if err != nil { return nil, fmt.Errorf("producer: %w", err) }
	go func() {
		for e := range p.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				slog.Error("delivery", "err", m.TopicPartition.Error)
			}
		}
	}()
	return &Producer{p: p}, nil
}

func (pr *Producer) PublishReply(topic string, reply Reply) error {
	b, _ := json.Marshal(reply)
	key := reply.CorrelationID.String()
	return pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(key), Value: b,
	}, nil)
}

func (pr *Producer) PublishBusinessEvent(topic string, payload map[string]any) {
	ev := map[string]any{
		"event_id":    uuid.New().String(),
		"occurred_at": time.Now().UTC(),
		"payload":     payload,
	}
	b, _ := json.Marshal(ev)
	id := uuid.New().String()
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(id), Value: b,
	}, nil)
}

func (pr *Producer) Flush() { pr.p.Flush(5000) }
func (pr *Producer) Close() { pr.p.Close() }
```

**Para cada PBC, implemente `CommandProcessor`** no pacote `api` ou `domain`:

- `cell-pedidos`: `ProcessCommand` cria/cancela pedido no DB próprio, retorna Reply
- `cell-estoque`: `ProcessCommand` reserva/libera estoque no DB próprio, retorna Reply
- `cell-notificacoes`: `ProcessCommand` envia notificação (simula), retorna Reply

**Modo passivo — proteção em `cmd/main.go` de cada célula:**

```go
cellRole := os.Getenv("CELL_ROLE")
if cellRole == "passive" {
	// Registra rotas de escrita que retornam 503
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		r.Handle(method, "/*path", func(c *gin.Context) {
			c.JSON(503, gin.H{
				"error":     "cell is passive — writes forbidden",
				"cell_role": "passive",
				"shard_id":  os.Getenv("SHARD_ID"),
			})
		})
	}
	slog.Info("passive cell — write routes blocked",
		"shard", os.Getenv("SHARD_ID"))
}
```

**Critério F0.3:**

```bash
for cell in cell-pedidos cell-estoque cell-notificacoes; do
  cd $cell && go mod tidy && go build ./... && cd ..
  echo "PASS $cell"
done
```

---

### F0.4 — data-sync

**`data-sync/go.mod`**

```
module github.com/ranselmo/poc-eci/data-sync
go 1.22
require (
    github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
    github.com/jackc/pgx/v5 v5.5.5
    github.com/google/uuid v1.6.0
    github.com/prometheus/client_golang v1.19.0
)
```

**`data-sync/infra/applier.go`**

```go
package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applied = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "data_sync_applied_total"},
		[]string{"shard", "pbc", "op"})
	syncErr = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "data_sync_errors_total"},
		[]string{"shard", "pbc"})
)

// Debezium envelopes para PostgreSQL (formato Kafka Connect)
type debeziumMsg struct {
	Payload struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Op     string         `json:"op"` // "c" "u" "d" "r"(snapshot)
		Source struct {
			Table string `json:"table"`
		} `json:"source"`
	} `json:"payload"`
}

type Applier struct {
	c     *kafka.Consumer
	pools map[string]*pgxpool.Pool // "shard-1:pedidos" → pool da passiva
}

func NewApplier(passiveDSNs map[string]string) (*Applier, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" { brokers = "kafka:29092" }

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "data-sync-cdc-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil { return nil, err }

	// Padrão regex suportado por confluent-kafka-go para múltiplos tópicos
	// Tópicos CDC: cdc.shard-1.pedidos.pedidos, cdc.shard-1.estoque.produtos, etc.
	// Debezium nomeia: <prefix>.<schema>.<table>
	// Aqui usamos prefixo configurável via env CDC_TOPIC_PREFIX (default: "cdc")
	prefix := os.Getenv("CDC_TOPIC_PREFIX")
	if prefix == "" { prefix = "cdc" }
	// Lista explícita de tópicos (mais confiável que regex)
	var topics []string
	shards := []string{"shard-1", "shard-2", "shard-3"}
	pbcs := map[string][]string{
		"pedidos":      {"pedidos"},
		"estoque":      {"produtos"},
		"notificacoes": {"notificacoes"},
	}
	for _, shard := range shards {
		for pbc, tables := range pbcs {
			for _, table := range tables {
				topics = append(topics, fmt.Sprintf("%s.%s.%s.%s",
					prefix, shard, pbc, table))
			}
		}
	}
	if err := c.SubscribeTopics(topics, nil); err != nil { return nil, err }

	pools := make(map[string]*pgxpool.Pool)
	for key, dsn := range passiveDSNs {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			slog.Warn("passive pool failed", "key", key, "err", err)
			continue
		}
		pools[key] = pool
	}
	return &Applier{c: c, pools: pools}, nil
}

func (a *Applier) Run(ctx context.Context) {
	slog.Info("data-sync started", "pools", len(a.pools))
	for {
		select {
		case <-ctx.Done():
			a.c.Close()
			return
		default:
			msg, err := a.c.ReadMessage(200)
			if err != nil {
				if !strings.Contains(err.Error(), "timed out") {
					slog.Error("read", "err", err)
				}
				continue
			}
			a.apply(ctx, msg)
		}
	}
}

func (a *Applier) apply(ctx context.Context, msg *kafka.Message) {
	topic := *msg.TopicPartition.Topic
	// topic: cdc.shard-1.pedidos.pedidos
	parts := strings.SplitN(topic, ".", 4)
	if len(parts) < 4 { return }
	shard, pbc := parts[1], parts[2]
	key := shard + ":" + pbc

	pool, ok := a.pools[key]
	if !ok {
		slog.Debug("no passive pool", "key", key)
		return
	}

	var dm debeziumMsg
	if err := json.Unmarshal(msg.Value, &dm); err != nil {
		syncErr.WithLabelValues(shard, pbc).Inc()
		slog.Error("unmarshal debezium", "err", err)
		return
	}

	table := dm.Payload.Source.Table
	var err error
	switch dm.Payload.Op {
	case "c", "r": // create / snapshot read
		err = applyInsert(ctx, pool, table, dm.Payload.After)
	case "u":
		err = applyUpdate(ctx, pool, table, dm.Payload.Before, dm.Payload.After)
	case "d":
		err = applyDelete(ctx, pool, table, dm.Payload.Before)
	}

	if err != nil {
		syncErr.WithLabelValues(shard, pbc).Inc()
		slog.Error("apply", "shard", shard, "pbc", pbc, "table", table, "err", err)
		return
	}
	applied.WithLabelValues(shard, pbc, dm.Payload.Op).Inc()
}

func applyInsert(ctx context.Context, pool *pgxpool.Pool, table string, row map[string]any) error {
	if len(row) == 0 { return nil }
	cols, vals, placeholders := insertParts(row)
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO NOTHING",
		table, cols, placeholders)
	_, err := pool.Exec(ctx, q, vals...)
	return err
}

func applyUpdate(ctx context.Context, pool *pgxpool.Pool, table string, before, after map[string]any) error {
	if len(after) == 0 || before["id"] == nil { return nil }
	set, vals := setParts(after)
	if set == "" { return nil }
	q := fmt.Sprintf("UPDATE %s SET %s WHERE id=$%d", table, set, len(vals)+1)
	_, err := pool.Exec(ctx, q, append(vals, before["id"])...)
	return err
}

func applyDelete(ctx context.Context, pool *pgxpool.Pool, table string, before map[string]any) error {
	if before["id"] == nil { return nil }
	_, err := pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=$1", table), before["id"])
	return err
}

func insertParts(row map[string]any) (cols string, vals []any, placeholders string) {
	var cs, ps []string
	i := 1
	for k, v := range row {
		cs = append(cs, k)
		vals = append(vals, v)
		ps = append(ps, fmt.Sprintf("$%d", i))
		i++
	}
	return strings.Join(cs, ","), vals, strings.Join(ps, ",")
}

func setParts(row map[string]any) (string, []any) {
	var parts []string
	var vals []any
	i := 1
	for k, v := range row {
		if k == "id" { continue }
		parts = append(parts, fmt.Sprintf("%s=$%d", k, i))
		vals = append(vals, v)
		i++
	}
	return strings.Join(parts, ","), vals
}
```

**`data-sync/cmd/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ranselmo/poc-eci/data-sync/infra"
)

func passiveDSNs() map[string]string {
	// Lê DSNs das células passivas de env vars
	// PASSIVE_DSN_SHARD1_PEDIDOS=postgres://...
	m := make(map[string]string)
	pairs := []struct{ key, env string }{
		{"shard-1:pedidos",      "PASSIVE_DSN_SHARD1_PEDIDOS"},
		{"shard-2:pedidos",      "PASSIVE_DSN_SHARD2_PEDIDOS"},
		{"shard-3:pedidos",      "PASSIVE_DSN_SHARD3_PEDIDOS"},
		{"shard-1:estoque",      "PASSIVE_DSN_SHARD1_ESTOQUE"},
		{"shard-2:estoque",      "PASSIVE_DSN_SHARD2_ESTOQUE"},
		{"shard-3:estoque",      "PASSIVE_DSN_SHARD3_ESTOQUE"},
		{"shard-1:notificacoes", "PASSIVE_DSN_SHARD1_NOTIFICACOES"},
		{"shard-2:notificacoes", "PASSIVE_DSN_SHARD2_NOTIFICACOES"},
		{"shard-3:notificacoes", "PASSIVE_DSN_SHARD3_NOTIFICACOES"},
	}
	for _, p := range pairs {
		if v := os.Getenv(p.env); v != "" {
			m[p.key] = v
		}
	}
	return m
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := infra.NewApplier(passiveDSNs())
	if err != nil { slog.Error("applier", "err", err); os.Exit(1) }
	go app.Run(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	srv := &http.Server{Addr: ":9191", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
}
```

**Critério F0.4:**

```bash
cd data-sync && go mod tidy && go build ./... && echo "PASS F0.4"
```

---

### F0.5 — docker-compose.yml (reescrever completo)

Gere o arquivo `docker-compose.yml` com a estrutura abaixo. **É um YAML válido completo** — não pseudocódigo.

Serviços obrigatórios:

1. `zookeeper` — confluent 7.6.0, porta 2181
2. `kafka` — confluent 7.6.0, porta 9092 (host) / 29092 (interno), `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`
3. `kafka-ui` — provectuslabs/kafka-ui:latest, porta 8090
4. `debezium` — debezium/connect:2.5, porta 8083, `BOOTSTRAP_SERVERS=kafka:29092`
5. `prometheus` — porta 9090, monta `./infra/monitoring/prometheus.yml`
6. `grafana` — porta 3000, admin/poc123
7. `jaeger` — jaegertracing/all-in-one:1.52, porta 16686 e 4317
8. `alertmanager` — prom/alertmanager:v0.26.0, porta 9093
9. `db-saga` — postgres:16-alpine, porta 5430, `POSTGRES_DB=saga POSTGRES_USER=saga POSTGRES_PASSWORD=saga123`
10. `shard-router` — build: ./shard-router, porta 8080, env SHARD{1,2,3}_{PEDIDOS,ESTOQUE,NOTIFICACOES}_{ACTIVE,PASSIVE}\_URL
11. `saga-hub` — build: ./saga-hub, porta 9090 (interno), env `DATABASE_URL=postgres://saga:saga123@db-saga:5432/saga?sslmode=disable`
12. `data-sync` — build: ./data-sync, env PASSIVE*DSN_SHARD{1,2,3}*{PEDIDOS,ESTOQUE,NOTIFICACOES}
13. Para cada shard (1,2,3) × PBC (pedidos,estoque,notificacoes) × role (active,passive):
    - Container célula: `cell-{pbc}-s{N}-{role}` — build: ./cell-{pbc}
    - Container DB: `db-{pbc}-s{N}-{role}` — postgres:16-alpine

    Env vars de cada célula:

    ```
    SHARD_ID: shard-{N}
    CELL_ROLE: {role}
    KAFKA_BROKERS: kafka:29092
    DATABASE_URL: postgres://{pbc}:{pbc}123@db-{pbc}-s{N}-{role}:5432/{pbc}?sslmode=disable
    OTEL_EXPORTER_OTLP_ENDPOINT: http://jaeger:4317
    OTEL_SERVICE_NAME: cell-{pbc}-s{N}-{role}
    ```

Total de containers: 9 infra + 1 saga-db + 1 shard-router + 1 saga-hub + 1 data-sync + 18 células + 18 DBs = **49 containers**. Todos na mesma network `poc-eci`.

**Critério F0.5:**

```bash
docker compose config --quiet && echo "PASS F0.5 — YAML válido"
docker compose up -d --scale saga-hub=1
sleep 90  # Kafka + Debezium precisam de tempo
docker compose ps --format "table {{.Name}}\t{{.Status}}" | grep -v "Up" | grep -v "NAME"
# Saída deve estar vazia (todos Up)
```

---

## §4 — FASE 1: RESILIÊNCIA (após F0 completo)

Para cada componente Go, crie `infra/resilience/` com os seguintes arquivos.

### F1.1 — Circuit Breaker (`infra/resilience/breaker.go`)

```go
package resilience

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker"
)

var breakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "circuit_breaker_state", Help: "0=closed 1=half-open 2=open",
}, []string{"component", "shard", "breaker"})

func NewBreaker(component, shardID, name string) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 3
		},
		OnStateChange: func(n string, from, to gobreaker.State) {
			slog.Warn("circuit breaker state change",
				"breaker", n, "from", from, "to", to,
				"component", component, "shard", shardID)
			breakerState.WithLabelValues(component, shardID, n).Set(float64(to))
		},
	})
}
```

Adicione a cada go.mod: `github.com/sony/gobreaker v0.5.0`

Envolva com breaker: acessos ao DB e ao Kafka producer em cada PBC.

### F1.2 — Retry com backoff (`infra/resilience/retry.go`)

```go
package resilience

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

func Retry(ctx context.Context, maxAttempts int, base time.Duration, fn func() error) error {
	delay := base
	for i := range maxAttempts {
		if err := fn(); err == nil { return nil }
		if i == maxAttempts-1 { break }
		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(delay + jitter):
		}
		delay = min(delay*2, 5*time.Second)
	}
	return fmt.Errorf("max %d attempts reached", maxAttempts)
}
```

### F1.3 — Bulkhead (`infra/resilience/bulkhead.go`)

```go
package resilience

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var bulkheadRejected = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "bulkhead_rejected_total",
}, []string{"component", "shard", "name"})

type Bulkhead struct {
	sem       chan struct{}
	component string
	shard     string
	name      string
}

func NewBulkhead(component, shard, name string, max int) *Bulkhead {
	return &Bulkhead{sem: make(chan struct{}, max),
		component: component, shard: shard, name: name}
}

func (b *Bulkhead) Do(ctx context.Context, fn func() error) error {
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
		return fn()
	case <-ctx.Done():
		return ctx.Err()
	default:
		bulkheadRejected.WithLabelValues(b.component, b.shard, b.name).Inc()
		return fmt.Errorf("bulkhead %s/%s: capacity exceeded", b.component, b.name)
	}
}
```

### F1.4 — Timeout middleware Gin

Em `cmd/main.go` de cada célula, antes de `h.Register(r)`:

```go
r.Use(func(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	c.Request = c.Request.WithContext(ctx)
	c.Next()
})
```

### F1.5 — Dead Letter Queue (`infra/messaging/dlq.go`)

```go
package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DLQMessage struct {
	OriginalTopic string          `json:"original_topic"`
	Key           string          `json:"key"`
	Payload       json.RawMessage `json:"payload"`
	Error         string          `json:"error"`
	Attempts      int             `json:"attempts"`
	FailedAt      time.Time       `json:"failed_at"`
}

func (pr *Producer) SendToDLQ(pbc, originalTopic, key string, payload []byte, err error, attempts int) {
	topic := fmt.Sprintf("dlq.%s.%s", pbc, originalTopic)
	msg := DLQMessage{OriginalTopic: originalTopic, Key: key,
		Payload: payload, Error: err.Error(),
		Attempts: attempts, FailedAt: time.Now().UTC()}
	b, _ := json.Marshal(msg)
	_ = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: []byte(key), Value: b,
	}, nil)
}
```

No `Run()` dos consumers: após 3 falhas consecutivas em `ProcessCommand`, chamar `SendToDLQ` e fazer ACK.

### F1.6 — Health checks liveness vs readiness

Em cada célula, substitua o `/health` genérico:

```go
r.GET("/healthz/live", func(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok", "shard": os.Getenv("SHARD_ID"), "role": os.Getenv("CELL_ROLE")})
})
r.GET("/healthz/ready", func(c *gin.Context) {
	if err := store.Ping(c.Request.Context()); err != nil {
		c.JSON(503, gin.H{"status": "db unhealthy", "error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok", "shard": os.Getenv("SHARD_ID"), "role": os.Getenv("CELL_ROLE")})
})
```

`Store.Ping` usa `pool.Ping(ctx)`.

**Critério F1:**

```bash
for c in shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  cd $c && go build ./... && cd .. && echo "PASS $c"
done
# Com stack up:
docker stop cell-estoque-s1-active
sleep 2
curl -sf http://localhost:8080/estoque/healthz/ready && echo "PASS failover"
docker start cell-estoque-s1-active
```

---

## §5 — FASE 2: PERFORMANCE (após F1 completo)

### F2.1 — Kubernetes manifests (`k8s/`)

Crie manifests para cada componente. Template para célula ativa (adapte para passiva e outros PBCs):

```yaml
# k8s/shards/shard-1/cell-pedidos-active/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cell-pedidos-s1-active
  namespace: poc-eci
  labels: { app: cell-pedidos, shard: shard-1, role: active, pbc: pedidos }
spec:
  replicas: 1
  selector:
    matchLabels: { app: cell-pedidos, shard: shard-1, role: active }
  template:
    metadata:
      labels: { app: cell-pedidos, shard: shard-1, role: active, pbc: pedidos }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: "/metrics"
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  { app: cell-pedidos, shard: shard-1, role: passive }
              topologyKey: kubernetes.io/hostname
      containers:
        - name: cell-pedidos
          image: poc-eci/cell-pedidos:latest
          ports: [{ containerPort: 8000 }]
          env:
            - { name: SHARD_ID, value: shard-1 }
            - { name: CELL_ROLE, value: active }
            - { name: KAFKA_BROKERS, value: kafka:29092 }
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  { name: cell-pedidos-s1-active, key: database-url }
          resources:
            requests: { cpu: 100m, memory: 64Mi }
            limits: { cpu: 500m, memory: 256Mi }
          livenessProbe:
            httpGet: { path: /healthz/live, port: 8000 }
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet: { path: /healthz/ready, port: 8000 }
            initialDelaySeconds: 15
            periodSeconds: 5
```

HPA por célula ativa (passivas não recebem tráfego direto, não precisam de HPA):

```yaml
# k8s/shards/shard-1/cell-pedidos-active/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: cell-pedidos-s1-active
  namespace: poc-eci
spec:
  scaleTargetRef:
    { apiVersion: apps/v1, kind: Deployment, name: cell-pedidos-s1-active }
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
```

### F2.2 — Redis cache por célula ativa

Adicione `github.com/redis/go-redis/v9 v9.5.1` a cada célula.

Crie `infra/cache/redis.go` em cada célula (padrão idêntico para todos):

```go
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	c      *redis.Client
	prefix string
	ttl    time.Duration
}

func New(prefix string, ttl time.Duration) (*Cache, error) {
	addr := os.Getenv("REDIS_URL")
	if addr == "" { addr = "redis:6379" }
	c := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 10})
	if err := c.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return &Cache{c: c, prefix: prefix, ttl: ttl}, nil
}

func (ca *Cache) Get(ctx context.Context, key string, dest any) (bool, error) {
	v, err := ca.c.Get(ctx, ca.prefix+key).Result()
	if err == redis.Nil { return false, nil }
	if err != nil { return false, err }
	return true, json.Unmarshal([]byte(v), dest)
}

func (ca *Cache) Set(ctx context.Context, key string, val any) error {
	b, err := json.Marshal(val)
	if err != nil { return err }
	return ca.c.Set(ctx, ca.prefix+key, b, ca.ttl).Err()
}

func (ca *Cache) Del(ctx context.Context, key string) error {
	return ca.c.Del(ctx, ca.prefix+key).Err()
}

func (ca *Cache) Ping(ctx context.Context) error { return ca.c.Ping(ctx).Err() }
```

Adicione Redis ao docker-compose por shard × PBC ativo (total: 9 Redis):

```yaml
redis-pedidos-s1: { image: redis:7.2-alpine, networks: [poc-eci] }
# etc.
```

### F2.3 — CQRS read model em cell-pedidos e cell-estoque

Adicione `infra/db/query_store.go` com consultas de leitura separadas do `Store` de escrita.
Adicione endpoint `GET /{pbc}/stats` que usa `QueryStore`.

**Critério F2:**

```bash
kubectl apply --dry-run=client -f k8s/ -R && echo "PASS manifests"
```

---

## §6 — FASE 3: SEGURANÇA (após F2 completo)

### F3.1 — JWT middleware (`infra/auth/jwt.go`)

Adicione `github.com/lestrrat-go/jwx/v2 v2.0.21` a cada célula.
Implemente middleware que valida Bearer token contra JWKS_URL e extrai roles.
Aplique em rotas de escrita de cada PBC.

### F3.2 — Rate limiting (`infra/middleware/ratelimit.go`)

Adicione `golang.org/x/time v0.5.0`.
Implemente IP rate limiter (100 req/s, burst 50) e adicione como middleware Gin.

### F3.3 — SAST no pipeline

No `.github/workflows/fitness-functions.yml`, adicione job:

```yaml
sast:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      {
        cell:
          [
            shard-router,
            saga-hub,
            data-sync,
            cell-pedidos,
            cell-estoque,
            cell-notificacoes,
          ],
      }
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version: "1.22" }
    - name: govulncheck
      run: cd ${{ matrix.cell }} && go install golang.org/x/vuln/cmd/govulncheck@latest && govulncheck ./...
    - name: gosec
      run: cd ${{ matrix.cell }} && go install github.com/securego/gosec/v2/cmd/gosec@latest && gosec -severity medium ./...
```

### F3.4 — Audit log (`infra/audit/logger.go`)

```go
package audit

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type Event struct {
	ID           uuid.UUID      `json:"id"`
	Component    string         `json:"component"`
	ShardID      string         `json:"shard_id"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id"`
	ActorID      string         `json:"actor_id"`
	Payload      map[string]any `json:"payload"`
	OccurredAt   time.Time      `json:"occurred_at"`
}

type Logger struct {
	p         *kafka.Producer
	component string
	shardID   string
}

func New(component, shardID string, p *kafka.Producer) *Logger {
	return &Logger{p: p, component: component, shardID: shardID}
}

func (l *Logger) Log(_ context.Context, action, resourceType, resourceID, actorID string, payload map[string]any) {
	ev := Event{
		ID: uuid.New(), Component: l.component, ShardID: l.shardID,
		Action: action, ResourceType: resourceType, ResourceID: resourceID,
		ActorID: actorID, Payload: payload, OccurredAt: time.Now().UTC(),
	}
	b, _ := json.Marshal(ev)
	topic := "audit.events"
	_ = l.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(ev.ID.String()),
		Value:          b,
	}, nil)
	slog.Info("audit", "action", action, "resource", resourceType, "id", resourceID)
}
```

**Critério F3:**

```bash
for c in shard-router saga-hub cell-pedidos cell-estoque cell-notificacoes; do
  cd $c && go build ./... && cd .. && echo "PASS $c"
done
```

---

## §7 — FASE 4: FINOPS + SRE (após F3 completo)

### F4.1 — SLO rules (`infra/monitoring/slo-rules.yml`)

```yaml
groups:
  - name: slo.availability
    interval: 30s
    rules:
      - record: shard:availability:rate5m
        expr: |
          sum(rate(shard_router_requests_total{role="active"}[5m])) by (shard, pbc)
          /
          sum(rate(shard_router_requests_total[5m])) by (shard, pbc)
      - record: shard:error_budget_burn
        expr: (1 - shard:availability:rate5m) / (1 - 0.999)
```

### F4.2 — Alert rules (`infra/monitoring/alert-rules.yml`)

```yaml
groups:
  - name: eci.critical
    rules:
      - alert: ActiveCellDown
        expr: shard_router_cell_health{role="active"} == 0
        for: 30s
        labels: { severity: critical }
        annotations:
          summary: "Célula ativa down: {{ $labels.shard }}/{{ $labels.pbc }}"
          runbook: "runbooks/shard-failover.md"

      - alert: BothCellsDown
        expr: sum(shard_router_cell_health) by (shard, pbc) == 0
        for: 10s
        labels: { severity: page }
        annotations:
          summary: "SHARD TOTALMENTE DOWN: {{ $labels.shard }}/{{ $labels.pbc }}"
          runbook: "runbooks/shard-failover.md"

      - alert: DataSyncLagHigh
        expr: data_sync_lag_seconds > 5
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Sync lag {{ $value }}s: {{ $labels.shard }}/{{ $labels.pbc }}"
          runbook: "runbooks/data-sync-lag.md"

      - alert: SagaFailureRateHigh
        expr: |
          rate(saga_completed_total{outcome="failed"}[5m])
          / rate(saga_completed_total[5m]) > 0.05
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Taxa de falha de sagas > 5%"

      - alert: CircuitBreakerOpen
        expr: circuit_breaker_state == 2
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Circuit breaker aberto: {{ $labels.component }}/{{ $labels.breaker }}"
          runbook: "runbooks/circuit-breaker.md"

      - alert: ErrorBudgetBurn
        expr: shard:error_budget_burn > 14.4
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "Error budget crítico: {{ $labels.shard }}/{{ $labels.pbc }}"
```

### F4.3 — Runbooks obrigatórios

Crie um arquivo `.md` por alert acima em `runbooks/`:

- `shard-failover.md` — diagnóstico + ações + verificação
- `data-sync-lag.md`
- `circuit-breaker.md`

Cada runbook segue o template:

```markdown
# Runbook: <nome do alert>

## Trigger: `<expr do alert>`

## Diagnóstico (execute em ordem)

1. <comando bash verificável>

## Ações corretivas

### Cenário A: <causa mais comum>

- <ação concreta>

### Cenário B: <segunda causa>

- <ação concreta>

## Verificação de resolução

<comando bash que valida que o problema foi resolvido>
## Escalação
Após Xmin sem resolução: <processo>
```

### F4.4 — FinOps metrics (`infra/monitoring/finops.go`)

```go
package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TxCost = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cell_transaction_cost_cents",
		Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
	}, []string{"component", "shard", "operation"})

	DBQueries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cell_db_queries_total",
	}, []string{"component", "shard", "operation"})

	KafkaMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cell_kafka_messages_total",
	}, []string{"component", "shard", "topic", "direction"})
)
```

**Critério F4:**

```bash
# Alertas válidos
docker run --rm -v $(pwd)/infra/monitoring:/conf \
  prom/prometheus:v2.48.0 promtool check rules /conf/alert-rules.yml && echo "PASS alerts"
docker run --rm -v $(pwd)/infra/monitoring:/conf \
  prom/prometheus:v2.48.0 promtool check rules /conf/slo-rules.yml && echo "PASS SLO"
# Runbook para cada alert
for alert in shard-failover data-sync-lag circuit-breaker; do
  test -f runbooks/$alert.md && echo "PASS runbook/$alert" || echo "MISSING runbook/$alert"
done
```

---

## §8 — FASE 5: IA AVANÇADA (após F4 completo)

### F5.1 — agent-mcp: self-healing com consciência de shards

Adicione ao `agent-mcp/main.py` as tools:

```python
SHARD_TOOLS = [
    {
        "name": "listar_status_shards",
        "description": "Lista saúde de todas as células de todos os shards via /router/cells",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "verificar_saga",
        "description": "Consulta status de uma saga específica",
        "input_schema": {
            "type": "object",
            "properties": {"saga_id": {"type": "string"}},
            "required": ["saga_id"]
        }
    },
    {
        "name": "iniciar_saga_pedido",
        "description": "Inicia uma saga de pedido via saga-hub",
        "input_schema": {
            "type": "object",
            "properties": {
                "cliente_id": {"type": "string"},
                "shard_id":   {"type": "string"},
                "payload":    {"type": "object"}
            },
            "required": ["cliente_id", "shard_id", "payload"]
        }
    },
    {
        "name": "reiniciar_celula",
        "description": "Reinicia célula específica via docker restart (apenas local/dev)",
        "input_schema": {
            "type": "object",
            "properties": {
                "container_name": {"type": "string"},
                "motivo":         {"type": "string"}
            },
            "required": ["container_name", "motivo"]
        }
    },
    {
        "name": "consultar_prometheus",
        "description": "Executa query PromQL e retorna resultados",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]

SHARD_ROUTER_URL = os.getenv("SHARD_ROUTER_URL", "http://shard-router:8080")
SAGA_HUB_URL     = os.getenv("SAGA_HUB_URL",     "http://saga-hub:9090")

async def executar_shard_tool(name: str, inputs: dict) -> str:
    import subprocess
    async with httpx.AsyncClient(timeout=10.0) as http:
        if name == "listar_status_shards":
            r = await http.get(f"{SHARD_ROUTER_URL}/router/cells")
            return json.dumps(r.json(), ensure_ascii=False)
        elif name == "verificar_saga":
            r = await http.get(f"{SAGA_HUB_URL}/saga/{inputs['saga_id']}")
            return json.dumps(r.json(), ensure_ascii=False)
        elif name == "iniciar_saga_pedido":
            r = await http.post(f"{SAGA_HUB_URL}/saga/pedido", json=inputs)
            return json.dumps(r.json(), ensure_ascii=False)
        elif name == "reiniciar_celula":
            result = subprocess.run(
                ["docker", "restart", inputs["container_name"]],
                capture_output=True, text=True, timeout=30
            )
            return json.dumps({"stdout": result.stdout, "stderr": result.stderr,
                               "returncode": result.returncode})
        elif name == "consultar_prometheus":
            r = await http.get(f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": inputs["query"]})
            data = r.json().get("data", {}).get("result", [])
            return json.dumps(data, ensure_ascii=False)
        return f"unknown tool: {name}"
```

### F5.2 — Anomaly detection (`agent-mcp/anomaly/detector.py`)

Implemente `AnomalyDetector` com `IsolationForest` monitorando:

- `shard_router_requests_total` por shard
- `data_sync_lag_seconds` por shard/pbc
- `saga_duration_seconds` p99
- `circuit_breaker_state`

Adicione ao `requirements.txt`: `scikit-learn==1.4.2 numpy==1.26.4`
Exponha em `GET /agente/anomalias`.

### F5.3 — Predictive scaling (`agent-mcp/scaling/predictor.py`)

Implemente `EMAPPredictor` com EMA + slope para forecast de RPS por célula.
Exponha em `GET /agente/scaling/previsao`.

**Critério F5:**

```bash
# Com stack up e ANTHROPIC_API_KEY configurada:
curl -sf -X POST http://localhost:9000/agente/executar \
  -H "Content-Type: application/json" \
  -d '{"prompt":"Liste o status de todos os shards e identifique anomalias"}' \
  | python3 -c "import sys,json; r=json.load(sys.stdin); print('PASS' if r.get('resultado') else 'FAIL')"
```

---

## §9 — CRITÉRIOS GLOBAIS DE ACEITAÇÃO

Execute este script após completar todas as fases. Deve passar 100%.

```bash
#!/bin/bash
set -e
FAIL=0

check() { [ "$1" = "0" ] && echo "PASS: $2" || { echo "FAIL: $2"; FAIL=1; }; }

echo "=== BUILD ==="
for c in shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  (cd $c && go build ./...) && check 0 "$c build" || check 1 "$c build"
done

echo "=== BOUNDARY CHECK ==="
# Verifica que PBCs não importam uns aos outros
for check_cell in cell-pedidos cell-estoque cell-notificacoes; do
  for forbidden in cell-pedidos cell-estoque cell-notificacoes saga-hub; do
    if [ "$check_cell" = "$forbidden" ]; then continue; fi
    count=$(grep -r "\"github.com/ranselmo/poc-eci/$forbidden" $check_cell/ 2>/dev/null | wc -l)
    check "$count" "$check_cell não importa $forbidden (count=$count)"
  done
done

echo "=== STACK HEALTH ==="
sleep 90
for shard in 1 2 3; do
  for pbc in pedidos estoque notificacoes; do
    for role in active passive; do
      name="cell-${pbc}-s${shard}-${role}"
      status=$(docker inspect --format='{{.State.Status}}' $name 2>/dev/null || echo "missing")
      [ "$status" = "running" ] && check 0 "$name running" || check 1 "$name running ($status)"
    done
  done
done

echo "=== SHARD ROUTING ==="
# Três routing keys devem produzir distribuição entre os 3 shards
declare -A shards_seen
for key in "cliente-aaa" "cliente-bbb" "cliente-ccc" "cliente-ddd" "cliente-eee"; do
  shard=$(curl -sf -H "X-Client-ID: $key" http://localhost:8080/healthz/live \
    -D - 2>/dev/null | grep -i X-Shard-ID | awk '{print $2}' | tr -d '\r' || echo "")
  shards_seen[$shard]=1
done
[ "${#shards_seen[@]}" -ge 2 ] && check 0 "routing distributes across shards" \
  || check 1 "routing distributes across shards (only ${#shards_seen[@]} shards seen)"

echo "=== PASSIVE MODE ==="
resp=$(curl -sf -X POST http://cell-pedidos-s1-passive:8000/pedidos/ \
  -H "Content-Type: application/json" -d '{}' 2>/dev/null || echo "503")
[[ "$resp" == *"passive"* || "$resp" == "503" ]] && check 0 "passive blocks writes" \
  || check 1 "passive blocks writes (got: $resp)"

echo "=== SAGA E2E ==="
RESP=$(curl -sf -X POST http://localhost:8080/saga/pedido \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: test-001" \
  -d '{"cliente_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","shard_id":"shard-1","payload":{"itens":[{"produto_id":"11111111-1111-1111-1111-111111111111","quantidade":1,"preco_unitario":4999.90}]}}')
SAGA_ID=$(echo $RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['saga_id'])")
sleep 10
STATUS=$(curl -sf http://localhost:8080/saga/$SAGA_ID \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
[ "$STATUS" = "COMPLETED" ] && check 0 "saga e2e completed" \
  || check 1 "saga e2e (status=$STATUS)"

echo "=== SLO RULES ==="
docker run --rm -v $(pwd)/infra/monitoring:/conf prom/prometheus:v2.48.0 \
  promtool check rules /conf/slo-rules.yml 2>&1 | grep -q "SUCCESS" && check 0 "slo-rules valid" \
  || check 1 "slo-rules invalid"
docker run --rm -v $(pwd)/infra/monitoring:/conf prom/prometheus:v2.48.0 \
  promtool check rules /conf/alert-rules.yml 2>&1 | grep -q "SUCCESS" && check 0 "alert-rules valid" \
  || check 1 "alert-rules invalid"

echo "=== RESULT ==="
[ "$FAIL" = "0" ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED — review above"
exit $FAIL
```

---

## §10 — MÓDULOS POR COMPONENTE

```
shard-router:
  github.com/gin-gonic/gin v1.10.0
  github.com/google/uuid v1.6.0
  github.com/prometheus/client_golang v1.19.0
  go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.49.0
  go.opentelemetry.io/otel v1.24.0
  go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
  go.opentelemetry.io/otel/sdk v1.24.0
  go.opentelemetry.io/otel/semconv/v1.24.0 v1.24.0

saga-hub: (+ ao shard-router)
  github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
  github.com/jackc/pgx/v5 v5.5.5

cell-* e data-sync: (+ ao saga-hub)
  github.com/sony/gobreaker v0.5.0
  github.com/redis/go-redis/v9 v9.5.1          (cells only)
  github.com/lestrrat-go/jwx/v2 v2.0.21        (cells only, fase 3)
  golang.org/x/time v0.5.0                     (cells only, fase 3)
```

Após qualquer adição: `go mod tidy && go mod verify && go build ./...`

As células Go precisam de CGO para confluent-kafka-go. Dockerfile base:

```dockerfile
FROM golang:1.22-bullseye AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev pkg-config
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /app/service ./cmd/
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 ca-certificates curl
COPY --from=build /app/service /service
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s \
  CMD curl -sf http://localhost:8000/healthz/ready || exit 1
ENTRYPOINT ["/service"]
```

shard-router não usa Kafka → `CGO_ENABLED=0`, imagem `gcr.io/distroless/static-debian12`.
