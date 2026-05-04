# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Verify

```bash
# Build all Go components (run after every change)
for c in shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  (cd "$c" && go build ./...) && echo "OK: $c" || echo "FAIL: $c"
done

# Static acceptance checks (build + boundary + SLO rules)
./check.sh

# Full acceptance suite (requires stack up)
docker compose up -d && ./check.sh --full

# Single component
cd cell-pedidos && go build ./...

# Run tests (when they exist)
cd <component> && go test -race -count=1 ./...

# Fitness functions (requires stack up)
python3 fitness-functions/run_all.py
```

## Running Locally

```bash
docker compose up -d        # start everything (~60s for Kafka)
docker compose logs -f <svc>
./teardown.sh               # stop and remove all resources
./teardown.sh --images      # also remove built images
```

## Architecture

Six Go modules + one Python service. **Every module has its own `go.mod`** — they are not a monorepo workspace. Each is built independently.

```
shard-router  →  [cell-pedidos, cell-estoque, cell-notificacoes]  (HTTP proxy)
                         ↑ commands / replies ↓
                      saga-hub  (Kafka orchestrator)
                         ↑ CDC events ↓
                      data-sync  (Debezium → passive DB)
```

**3 shards × 3 PBCs × 2 roles (active/passive) = 18 cell containers** in Docker Compose.

### Architectural Laws — never violate

| Law | Rule |
|-----|------|
| L1 | PBCs never import each other — zero cross-PBC imports or HTTP calls |
| L2 | `saga-hub` is the only integrator between PBCs |
| L3 | All external requests enter via `shard-router` |
| L4 | Passive cells reject writes with HTTP 503 |
| L5 | Scale unit = 1 cell + 1 DB + 1 Redis + 1 Kafka consumer group |

The `boundary-check` CI job enforces L1 on every push.

### Routing

`shard-router/domain/routing.go`: `sha256(X-Client-ID) % 3` → `shard-1|2|3`. Registry in `infra/registry.go` maps `"shard-1:pedidos:active"` → `CellEntry{BaseURL, Healthy}`. `infra/watcher.go` polls `/healthz/ready` every 5s and triggers failover to passive when active is down.

### SAGA Orchestration

`saga-hub` runs a choreography over Kafka:
1. `commands.pedidos.criar` → cell-pedidos replies on `replies.pedidos.criado`
2. `commands.estoque.reservar` → cell-estoque replies on `replies.estoque.reservado` (or `insuficiente`)
3. On estoque failure: compensate via `commands.pedidos.cancelar`
4. `commands.notificacoes.enviar` → final step

State machine in `saga-hub/orchestrator/pedido.go`. Saga state persisted in PostgreSQL (`db-saga`). Topic constants are the source of truth in `saga-hub/domain/saga.go`.

### Cell Structure (cell-pedidos is the reference)

```
domain/      ← pure business logic, no framework imports
infra/
  auth/      ← JWT middleware (JWKS_URL env; passthrough when unset)
  audit/     ← publishes to Kafka topic audit.events
  cache/     ← Redis cache-aside (graceful degradation if Redis down)
  db/        ← pgxpool store + CQRS read model (query_store.go)
  messaging/ ← kafka.go (consumer + CommandProcessor interface), producer.go, dlq.go
  middleware/ ← IP rate limiter (100 req/s, burst 50)
  monitoring/ ← Prometheus counters/histograms (finops metrics)
  resilience/ ← Breaker (gobreaker), Bulkhead (semaphore), Retry (exponential+jitter)
api/         ← Gin handlers; implements messaging.CommandProcessor
cmd/main.go  ← bootstrap only: OTel, DB, Kafka, Gin, graceful shutdown
```

`cell-estoque` and `cell-notificacoes` follow the same structure (cell-notificacoes currently deviates — see REFINE.md R3.1).

### Kafka Message Flow

```
CommandType string  →  determines reply topic (replyTopicFor in kafka.go)
CorrelationID UUID  →  used by saga-hub to find the saga on reply
enable.auto.commit  →  currently true (at-most-once; REFINE.md R1.9 changes this)
```

DLQ: after 3 consecutive failures per message stream, `SendToDLQ` publishes to `dlq.<pbc>.<original-topic>`.

### Passive Cell Behaviour

`CELL_ROLE=passive` env var → consumer is disabled (nil in `NewConsumer`) and write routes return 503. Passive cells only serve reads and receive CDC updates from data-sync.

### Resilience Stack (per component)

- **Breaker**: wraps DB and Kafka produce calls; 3 consecutive failures open; 30s timeout
- **Bulkhead**: semaphore-based concurrency limit on command processing (10 cells, 20 saga-hub)
- **Retry**: 3 attempts, exponential backoff with jitter, capped at 5s, context-aware

### Observability

- OTel traces → Jaeger (`OTEL_EXPORTER_OTLP_ENDPOINT`, default `jaeger:4317`)
- Prometheus scrapes `/metrics` on each active cell + shard-router + saga-hub
- SLO rules: `infra/monitoring/slo-rules.yml` (availability 99.9%, error budget burn)
- Alert rules: `infra/monitoring/alert-rules.yml` (6 alerts with runbook links)
- **Known gap**: `slo-rules.yml` and `alert-rules.yml` are not mounted in `docker-compose.yml` volumes — see REFINE.md R4.7

### agent-mcp (Python)

FastAPI + Anthropic SDK. Tools split into `MCP_TOOLS` (HTTP to cells) and `SHARD_TOOLS` (HTTP to shard-router/saga-hub + `docker restart`). `ALL_TOOLS = MCP_TOOLS + SHARD_TOOLS` is passed to Claude. Background `loop_monitoramento` runs every 60s: IsolationForest anomaly detection → if anomaly triggers self-healing agent call.

## Key Environment Variables

| Var | Component | Default |
|-----|-----------|---------|
| `DATABASE_URL` | all cells, saga-hub | hardcoded (dev only) |
| `KAFKA_BROKERS` | all | `kafka:29092` |
| `SHARD_ID` | cells | — |
| `CELL_ROLE` | cells | — (`active`/`passive`) |
| `REDIS_URL` | cells | `redis:6379` |
| `JWKS_URL` | cells | unset = auth disabled |
| `ANTHROPIC_API_KEY` | agent-mcp | — (agent disabled if unset) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all | `http://jaeger:4317` |

## Planned Refactoring

`REFINE.md` contains the complete SDD for 47 improvements across 6 phases. Key structural changes planned:

- **R2**: Extract `shared/` module (resilience, telemetry, middleware, cache, auth, audit, monitoring) to eliminate duplication across the 6 Go modules
- **R3**: Refactor `cell-notificacoes` to match `cell-pedidos` structure; separate `CorrelationID` from `SagaID`
- **R1**: Critical bug fixes — atomic stock reservation transaction, compensation releases stock, Kafka at-least-once

When implementing anything that touches resilience, auth, cache, or middleware: check REFINE.md R2 first to avoid duplicating code that is about to be extracted.
