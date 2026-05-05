# PoC — Empresa Componível Inteligente v2 (Go + Kafka)

Implementação prática do framework de 4 pilares descrito no artigo  
**"Como Construir uma Empresa Componível Inteligente"** — Rafael Sá Anselmo.

Esta é a **versão 2** da PoC, com stack tecnológica alternativa:  
células escritas em **Go**, comunicação via **Apache Kafka**, arquitetura de shards com SAGA Hub centralizado.

---

## Arquitetura

```
                          ┌─────────────────────────────────────────────────────┐
 X-Client-ID header       │                   shard-router                      │
 ──────────────────────►  │  hash(clienteID) % 3  →  shard-1 | shard-2 | shard-3│
                          └──────────────┬──────────────────────────────────────┘
                                         │
                          ┌──────────────▼──────────────┐
                          │          saga-hub            │
                          │  único integrador entre PBCs │
                          │  SAGA PedidoCriar + compensação│
                          └──────┬────────┬────────┬─────┘
                                 │        │        │
                          ┌──────▼──┐ ┌───▼───┐ ┌─▼──────────────┐
                          │ cell-   │ │cell-  │ │cell-           │
                          │ pedidos │ │estoque│ │notificacoes    │
                          │active/  │ │active/│ │active/passive  │
                          │passive  │ │passive│ └────────────────┘
                          └─────────┘ └───────┘
                                 ↕ CDC via Debezium
                          ┌─────────────────────┐
                          │      data-sync       │
                          │  ativo → passivo     │
                          └─────────────────────┘
```

**Leis arquiteturais:**
- **L1** — PBCs são ilhas: zero imports/HTTP calls diretos entre células
- **L2** — `saga-hub` é o único integrador entre PBCs
- **L3** — Toda requisição entra pelo `shard-router`
- **L4** — Células passivas recusam escrita (HTTP 503)
- **L5** — Scale Unit = 1 célula + 1 DB + 1 Redis + 1 consumer group Kafka

---

## Stack Tecnológica

| Tecnologia | Versão | Papel na solução |
|---|---|---|
| Go | 1.22 | Linguagem das células e componentes de infraestrutura |
| Apache Kafka | 7.6 (Confluent) | Event Bus para comandos, replies e eventos de domínio |
| confluent-kafka-go | v2.3 | Driver Kafka para Go (alta performance, CGO) |
| PostgreSQL | 16 | Banco isolado por célula (boundary físico) |
| pgx | v5 | Driver Go nativo para PostgreSQL |
| Gin | v1.10 | Framework HTTP das células Go |
| lestrrat-go/jwx | v2.0.21 | Validação JWT Bearer via JWKS (F3) |
| golang.org/x/time | v0.5.0 | IP rate limiter — 100 req/s burst 50 (F3) |
| Traefik | v3.0 | API Gateway — entry point único |
| OpenTelemetry | v1.24 | Tracing distribuído entre células |
| Jaeger | 1.52 | Backend de tracing distribuído |
| Prometheus | 2.48 | Coleta de métricas de cada célula |
| Grafana | 10.2 | Dashboards de métricas |
| Python + httpx | 3.12 | Fitness functions no CI/CD |
| GitHub Actions | — | Pipeline CI/CD com FFs integradas |
| Anthropic Claude API | claude-sonnet-4-6 | LLM do agente autônomo |
| Anthropic SDK Python | 0.28+ | Function calling / MCP tools |
| FastAPI | 0.111 | Interface HTTP do agente |
| scikit-learn | — | Anomaly detection (IsolationForest — F5) |
| Kafka UI | latest | Inspeção visual de eventos Kafka |
| Docker Compose | v2 | Orquestração local da PoC |
| Kubernetes | 1.29+ | Manifests para deploy em produção (F2) |

---

## Componentes

### shard-router
Recebe todas as requisições externas. Aplica consistent hashing sobre o header `X-Client-ID` (`sha256(clienteID) % 3`) para rotear para shard-1, shard-2 ou shard-3. Mantém um registry de células ativas/passivas por shard e implementa circuit breaker de roteamento.

### saga-hub
Único integrador entre PBCs. Orquestra a SAGA `PedidoCriar` via padrão Async Request-Reply sobre Kafka:
1. `commands.pedidos.criar` → cell-pedidos
2. `commands.estoque.reservar` → cell-estoque
3. `commands.notificacoes.enviar` → cell-notificacoes

Inclui compensação automática: falha no estoque → `commands.pedidos.cancelar` + notificação ao cliente.

### data-sync
Consome eventos CDC do Debezium (tópicos `cdc.*`) e aplica as mudanças da célula ativa para a célula passiva de cada shard. Garante que o passive node está sempre pronto para assumir.

### cell-pedidos / cell-estoque / cell-notificacoes
PBCs Go + Gin + Kafka + PostgreSQL. Cada um implementa o padrão Async Request-Reply: consome `commands.*`, processa, publica em `replies.*`. DLQ implementada para mensagens que falham após N retries.

### agent-mcp
Agente Python + Anthropic SDK + FastAPI que expõe capacidades das células como MCP tools. Inclui:
- Loop de monitoramento autônomo (a cada 60s)
- Detecção de anomalias via IsolationForest sobre métricas Prometheus (`anomaly/detector.py`)
- Predictive scaling baseado em histórico (`scaling/predictor.py`)

---

## Tópicos Kafka

| Padrão | Exemplo | Direção |
|---|---|---|
| `commands.{pbc}.{acao}` | `commands.pedidos.criar` | saga-hub → célula |
| `replies.{pbc}.{acao}` | `replies.pedidos.criar` | célula → saga-hub |
| `events.{pbc}.{tipo}` | `events.pedidos.confirmado` | célula → consumidores |
| `cdc.*` | `cdc.pedidos.public.pedidos` | Debezium → data-sync |

---

## Diferenças em relação à v1 (Python + RabbitMQ)

| Aspecto | v1 (Python + RabbitMQ) | v2 (Go + Kafka) |
|---|---|---|
| Linguagem das células | Python 3.12 + FastAPI | Go 1.22 + Gin |
| Event Bus | RabbitMQ 3.12 | Apache Kafka 7.6 |
| Tamanho da imagem Docker | ~300MB por célula | ~15MB por célula |
| Modelo de concorrência | asyncio (cooperative) | goroutines (preemptive) |
| Replay de eventos | Não (fila descarta) | Sim (log retido 24h) |
| Roteamento | Sem shards | Shard Router (consistent hashing) |
| SAGA | Coreografia implícita | Orquestração explícita (saga-hub) |
| Replicação de dados | Não | Data Sync via CDC (Debezium) |
| Deploy em produção | Docker Compose apenas | Kubernetes manifests completos |
| Tipagem de eventos | Pydantic (runtime) | Structs Go (compile-time) |

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|---|---|
| Docker Desktop | 24+ |
| Docker Compose | v2 |
| Python | 3.12+ (só para fitness functions e agente) |
| curl | qualquer |

> Go **não precisa** ser instalado localmente — o build acontece dentro do Docker via multi-stage build.

---

## Instalação em 5 minutos

```bash
# 1. Clone
git clone https://github.com/ranselmo/poc-eci-go.git
cd poc-eci-go

# 2. Configure
cp .env.example .env
# Edite .env e adicione: ANTHROPIC_API_KEY=sk-ant-...

# 3. Suba o stack
docker compose up -d

# 4. Aguarde ~60s (Kafka leva mais para inicializar que RabbitMQ)
docker compose ps

# 5. Verifique
curl http://localhost/pedidos/health
curl http://localhost/estoque/health
curl http://localhost/notificacoes/health
```

---

## Demonstração manual

### SAGA — Pedido com estoque disponível

```bash
# Criar pedido roteado para o shard do cliente
curl -X POST http://localhost/pedidos/ \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" \
  -d '{
    "cliente_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "itens": [{
      "produto_id": "11111111-1111-1111-1111-111111111111",
      "quantidade": 1,
      "preco_unitario": 4999.90
    }]
  }'

# Aguardar SAGA via Kafka (5s)
sleep 5

# Consultar status — deve ser CONFIRMADO
curl http://localhost/pedidos/{pedido_id}
```

### SAGA — Pedido sem estoque (compensação)

```bash
# produto 44444444 tem estoque 0 → dispara compensação
curl -X POST http://localhost/pedidos/ \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" \
  -d '{
    "cliente_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "itens": [{
      "produto_id": "44444444-4444-4444-4444-444444444444",
      "quantidade": 1,
      "preco_unitario": 3499.90
    }]
  }'

# Status final deve ser CANCELADO com notificação enviada
sleep 5
curl http://localhost/pedidos/{pedido_id}
```

### Demo automatizada

```bash
bash demo.sh
```

### Fitness Functions

```bash
pip install httpx
python3 fitness-functions/run_all.py
```

### Agente de IA

```bash
curl -X POST http://localhost:9000/agente/executar \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Verifique a saúde do sistema e liste os pedidos recentes."}'
```

---

## Interfaces disponíveis

| Interface | URL | Credenciais |
|---|---|---|
| API Gateway (Traefik) | http://localhost:8080 | — |
| Shard Router | http://localhost:8000 | — |
| Saga Hub API | http://localhost:8100/docs | — |
| Swagger cell-pedidos | http://localhost/pedidos/docs | — |
| Swagger cell-estoque | http://localhost/estoque/docs | — |
| Kafka UI | http://localhost:8090 | — |
| Grafana | http://localhost:3000 | admin / poc123 |
| Jaeger | http://localhost:16686 | — |
| Prometheus | http://localhost:9090 | — |
| Agente MCP | http://localhost:9000/docs | — |

---

## Dados de seed

| UUID | Produto | Estoque |
|---|---|---|
| `11111111-...` | Notebook Pro | 10 |
| `22222222-...` | Mouse Ergonômico | 50 |
| `33333333-...` | Teclado Mecânico | 5 |
| `44444444-...` | Monitor 4K | **0** ← testa compensação |

---

## Estrutura do repositório

```
empresa-componivel-agentica-v2/
│
├── shard-router/               # F0: consistent hashing + registry de shards
│   ├── domain/routing.go       # sha256(clienteID) % 3
│   ├── infra/registry.go       # registry de células ativas/passivas
│   ├── infra/watcher.go        # health-check contínuo das células
│   ├── api/handlers.go
│   └── cmd/main.go
│
├── saga-hub/                   # F0: orquestrador de SAGAs
│   ├── domain/saga.go          # estados: pending → completed | failed | compensating
│   ├── orchestrator/pedido.go  # SAGA PedidoCriar com compensação
│   ├── infra/db/store.go       # persistência de estado da SAGA
│   ├── infra/messaging/        # producer + consumer Kafka
│   └── cmd/main.go
│
├── data-sync/                  # F0: CDC ativo → passivo via Debezium
│   ├── infra/applier.go
│   └── cmd/main.go
│
├── cell-pedidos/               # PBC de pedidos
│   ├── domain/
│   │   ├── pedido.go
│   │   └── events.go
│   ├── infra/
│   │   ├── auth/jwt.go         # F3: JWT middleware via JWKS_URL
│   │   ├── middleware/ratelimit.go # F3: IP rate limiter 100 req/s
│   │   ├── audit/logger.go     # F3: audit log via Kafka (audit.events)
│   │   ├── cache/redis.go      # F2: cache-aside Redis
│   │   ├── db/store.go
│   │   ├── db/query_store.go   # CQRS read model
│   │   ├── messaging/kafka.go
│   │   ├── messaging/producer.go
│   │   ├── messaging/dlq.go    # Dead Letter Queue
│   │   └── resilience/         # F1: breaker, retry, bulkhead
│   ├── api/handlers.go
│   └── cmd/main.go
│
├── cell-estoque/               # PBC de estoque (mesma estrutura)
├── cell-notificacoes/          # PBC de notificações (mesma estrutura)
│
├── agent-mcp/
│   ├── main.py                 # FastAPI + Anthropic SDK + MCP tools
│   ├── anomaly/detector.py     # F5: IsolationForest sobre métricas Prometheus
│   ├── scaling/predictor.py    # F5: predictive scaling
│   └── requirements.txt
│
├── k8s/
│   ├── base/namespace.yaml
│   ├── core/
│   │   ├── shard-router.yaml
│   │   └── saga-hub.yaml
│   └── shards/
│       ├── shard-1/{cell}-active/  deployment.yaml + hpa.yaml (×3 células)
│       ├── shard-2/                (mesma estrutura)
│       └── shard-3/                (mesma estrutura)
│
├── runbooks/
│   ├── circuit-breaker.md
│   ├── data-sync-lag.md
│   └── shard-failover.md
│
├── infra/
│   ├── auth/jwt.go             # F3: referência canônica JWT middleware
│   ├── middleware/ratelimit.go # F3: referência canônica rate limiter
│   ├── audit/logger.go         # F3: referência canônica audit log
│   └── monitoring/
│       ├── prometheus.yml
│       ├── grafana-datasources.yml
│       └── grafana-dashboards.yml
│
├── fitness-functions/
│   └── run_all.py
│
├── .github/workflows/
│   └── fitness-functions.yml   # CI: executa FFs a cada push
│
├── docker-compose.yml
├── Makefile
├── demo.sh
├── teardown.sh                 # para e remove todos os recursos da demo
├── PROMPT.md                   # Master Prompt v3 — especificação arquitetural completa
└── REFINE.md                   # SDD de refinamento — 47 melhorias identificadas em revisão de código
```

---

## Comandos úteis

```bash
# Logs de uma célula Go
docker compose logs -f cell-pedidos

# Ver consumer groups Kafka
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

# Ver lag do consumer de estoque
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group cell-estoque-group

# Acessar banco de pedidos
docker exec -it db-pedidos psql -U pedidos -d pedidos \
  -c "SELECT id, status, valor_total FROM pedidos ORDER BY criado_em DESC;"

# Ver estado das SAGAs
docker exec -it db-saga psql -U saga -d saga \
  -c "SELECT id, status, current_step, created_at FROM sagas ORDER BY created_at DESC;"

# Rebuild de uma célula Go após mudança
docker compose build cell-pedidos && docker compose restart cell-pedidos

# Executar fitness functions localmente
make fitness-check

# Parar demo e liberar todos os recursos (containers, volumes, rede, cache Python)
./teardown.sh

# Idem + remove imagens buildadas (~15MB × 7) para liberar disco completamente
./teardown.sh --images

# Critérios globais de aceitação — checks estáticos (build, boundary, SLO rules)
./check.sh

# Critérios globais de aceitação — suite completa (requer stack up)
docker compose up -d && ./check.sh --full
```

---

## Status de implementação por fase

| Fase | Descrição | Status |
|---|---|---|
| F0 — Fundação | shard-router, saga-hub, data-sync, PBCs Async Request-Reply | ✅ Completo |
| F1 — Resiliência | Circuit Breaker, Retry+backoff, Bulkhead, Timeout, DLQ | ✅ Completo |
| F2 — Performance | Kubernetes manifests, Redis cache, CQRS read model | ✅ Completo |
| F3 — Segurança | JWT middleware, Rate limiting, SAST pipeline, Audit log | ✅ Completo |
| F4 — FinOps + SRE | SLO rules, Alert rules, Runbooks, FinOps metrics | ✅ Completo |
| F5 — IA Avançada | Self-healing, Anomaly detection, Predictive scaling | ✅ Completo |
| R1 — Bugs críticos | Transação atômica, compensação SAGA, at-least-once Kafka, allowlists | ✅ Completo |
| R2 — Módulo shared/ | Elimina 18 pacotes duplicados; replace directive local | ✅ Completo |
| R3 — Consistência | cell-notificacoes refatorado, CorrelationID separado, TTLFromEnv | ✅ Completo |
| R4 — Observabilidade | HealthWatcher semáforo, proxy cache, SLO latência, data-sync metrics | ✅ Completo |
| R5 — CI/CD e Infra | Cache CI, versões fixas, KRaft, PDBs, NetworkPolicies, tópicos explícitos | ✅ Completo |
| R6 — Testes | Testes unitários de domínio e resilience, FF1/FF2 corrigidos | ✅ Completo |

---

## Changelog

### v2.15.0 — 2026-05-05

**demo.sh atualizado para v2.14.0**

- Banner atualizado: "Kafka KRaft" (ZooKeeper removido), versão e referência às 47 melhorias REFINE.md
- Setup aguarda `/healthz/ready` do `data-sync` antes de prosseguir
- SAGA compensação: fluxo correto exibido com `liberar_estoque` antes de FAILED (R1.2)
- Kafka topics: coloração adicionada para `cdc.*` (CDC Debezium), `audit.*` (audit log) e `events.pedidos.falhou`
- Pilar 2: bloco de data-sync readiness; Prometheus com queries de `data_sync_lag_seconds`, `data_sync_applied_total`, `circuit_breaker_state` e SLO de disponibilidade (`shard:availability:rate5m`)
- Pilar 4: "12 tools: 7 cell + 5 shard-aware" corrigido (era "5 MCP tools"); `ANTHROPIC_MODEL` env var documentada
- Resumo final: seção "Qualidade (REFINE.md)" com descrição de cada fase R1–R6

---

### v2.14.0 — 2026-05-05

**REFINE.md Fase R6 — Testes**

- **R6.1**: Testes unitários criados para 4 domínios: `cell-pedidos/domain/pedido_test.go` (8 casos: NewPedido válido/inválido, ValorTotal, Confirmar, Cancelar), `cell-estoque/domain/estoque_test.go` (7 casos: Reservar, Liberar, Repor), `shard-router/domain/routing_test.go` (3 casos: determinismo, formato, distribuição), `shared/resilience/retry_test.go` + `bulkhead_test.go` (7 casos). Todos passam com `-race`.
- **R6.2**: `BOUNDARY_RULES` removida de `fitness-functions/run_all.py` — os padrões (`"estoque123"`, etc.) nunca existiam no código e o check era inócuo. `check_boundary` agora usa apenas `FORBIDDEN_IMPORTS` para verificar imports reais via `github.com/ranselmo/poc-eci/{pbc}`.
- **R6.3**: `FF2_JWT_TOKEN` env var adicionada ao FF2 — quando definida injeta `Authorization: Bearer <token>` e ajusta `expected_status` para 201; quando ausente com `JWKS_URL` ativo espera 401 explicitamente.

---

### v2.13.0 — 2026-05-05

**REFINE.md Fase R5 — CI/CD e Infraestrutura**

- **R5.1–R5.4**: CI reescrito — `actions/cache@v4` para `~/go/pkg/mod` + `~/.cache/go-build` em todos os jobs; `govulncheck@v1.1.3` e `gosec@v2.21.4` fixados (era `@latest`); job `test` adicionado (`go test -race -count=1 ./...`); job `lint` adicionado com `golangci-lint-action@v6` v1.59.1.
- **R5.5**: `Makefile` com targets `build-images` e `push-images` usando `VERSION=$(git describe --tags --always --dirty)` — resolve imagens `latest` nos manifests k8s.
- **R5.6**: `k8s/core/data-sync.yaml` criado — Deployment + Service + readinessProbe em `/healthz/ready:9191`, DSNs passivos via `Secret data-sync-passive-dsns`.
- **R5.7**: 9 `PodDisruptionBudget` (`minAvailable: 1`) criados para os 9 deployments ativos (3 shards × 3 PBCs).
- **R5.8**: `k8s/base/network-policy.yaml` — 3 `NetworkPolicy` isolam PBCs: cada um só aceita ingress do `shard-router` e Prometheus (enforça L1 na camada de rede).
- **R5.9**: `shared/auth/jwt.go` emite `slog.Warn` quando `JWKS_URL` não está definida — torna visível que autenticação está desabilitada em dev.
- **R5.10–R5.12**: `ALLOWED_CONTAINERS` definida em `agent-mcp/main.py` — `reiniciar_celula` rejeita containers não listados; `subprocess.run` substituído por `asyncio.create_subprocess_exec` (não bloqueia o event loop); `ANTHROPIC_MODEL` env var (default `claude-sonnet-4-6`).
- **R5.13**: `monitor_log` declarado como `deque[dict]` com `maxlen=100` — remove acumulação ilimitada sem pop manual.
- **R5.14**: Kafka migrado de ZooKeeper para **KRaft** no `docker-compose.yml` — serviço `zookeeper` removido; `kafka` agora usa `KAFKA_PROCESS_ROLES: broker,controller` com `CLUSTER_ID` fixo; `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`.
- **R5.15**: `infra/kafka/create-topics.sh` criado com todos os 30+ tópicos explícitos (commands, replies, events, audit, DLQ, CDC); serviço `kafka-init` adicionado ao `docker-compose.yml` executa o script após Kafka estar healthy.

---

### v2.12.0 — 2026-05-05

**REFINE.md Fase R4 — Observabilidade e runtime**

- **R4.2**: `HealthWatcher` com semáforo de 5 slots — evita thundering herd de goroutines de health-check quando há muitas células
- **R4.3**: `ReverseProxy` cacheado por `BaseURL` no handler (`proxyCache map[string]*httputil.ReverseProxy`) — evita alocação de proxy por request
- **R4.6**: Targets passivos adicionados ao `infra/monitoring/prometheus.yml` (18 → 36 targets, incluindo as 9 células passivas e data-sync)
- **R4.7**: `slo-rules.yml` e `alert-rules.yml` montados como volumes no `docker-compose.yml` — Prometheus agora carrega as regras em runtime
- **R4.8**: `slo.latency` group adicionado ao `slo-rules.yml` — recording rules para `shard:latency_p99:rate5m` e detecção de breach (p99 > 500ms)
- **R4.9**: `/healthz/ready` adicionado ao `data-sync` — retorna 503 se nenhum pool passivo estiver conectado
- **R4.10**: `data_sync_lag_seconds` gauge por shard/pbc — emitido após cada apply bem-sucedido

---

### v2.11.0 — 2026-05-05

**REFINE.md Fase R3 — Consistência arquitetural**

- **R3.1**: `cell-notificacoes` refatorado do god-file para estrutura canônica: `domain/notificacao.go` (entidade pura), `infra/db/store.go` (Store tipado com domínio), `infra/messaging/kafka.go` (Consumer com bulkhead+DLQ+at-least-once) e `infra/messaging/producer.go` (Producer com breaker). `cmd/main.go` agora é bootstrap puro (~100 linhas)
- **R3.4**: `NewSaga` gera `ID` e `CorrelationID` como UUIDs independentes — separa identidade da saga do ID de correlação de mensagens
- **R3.6**: `PublishBusinessEvent` em `saga-hub` agora usa circuit breaker — falhas de publicação logadas em vez de silenciadas
- **R3.7**: `cache.TTLFromEnv(def)` adicionado ao `shared/cache` — lê `CACHE_TTL_SECONDS` para override operacional sem rebuild

---

### v2.10.0 — 2026-05-05

**REFINE.md Fase R2 — Módulo `shared/` (elimina 18 pacotes duplicados)**

- **`shared/` criado** como módulo Go independente (`github.com/ranselmo/poc-eci/shared`) com 6 pacotes: `resilience/` (Breaker, Bulkhead, Retry), `auth/` (JWT middleware), `audit/` (Logger Kafka), `cache/` (Redis cache-aside), `middleware/` (RateLimit), `monitoring/` (métricas Prometheus FinOps)
- **18 pacotes duplicados removidos** de `cell-pedidos/infra/`, `cell-estoque/infra/`, `cell-notificacoes/infra/` — cada um tinha cópia idêntica de todos os 6 pacotes
- **`replace` directives** adicionadas em cada `go.mod` (`../shared`) para resolução local sem publicação em registry
- **R4.1 incluído**: `middleware/ratelimit.go` agora limpa entradas idle via goroutine com ticker de 5min, evitando crescimento ilimitado do map de limiters por IP
- Todos os 6 componentes compilam limpos; todos os checks estáticos passam

---

### v2.9.0 — 2026-05-04

**REFINE.md Fase R1 — Correção de bugs críticos e segurança**

- **R1.1**: `ReservarItens` e `LiberarItens` com transação PostgreSQL (`SELECT FOR UPDATE`) em `cell-estoque/infra/db/store.go` — elimina race condition em reservas concorrentes
- **R1.2**: Compensação completa da SAGA: `onPedidoCancelado` agora publica `liberar_estoque` (novo tópico `commands.estoque.liberar`); `onEstoqueLiberado` marca saga como FAILED e notifica cliente. Saga nunca mais termina em COMPENSATING sem liberar o estoque.
- **R1.3**: Propagação de erro de `pedido.Cancelar()` — não mais silenciado com `_ = pedido.Cancelar()`
- **R1.4**: Todos os 5 `_ = o.store.Save(ctx, saga)` em `saga-hub/orchestrator/pedido.go` agora logam o erro
- **R1.5**: `json.Unmarshal` de itens em `cell-pedidos/infra/db/store.go` retorna/loga erro em vez de ignorar
- **R1.6**: `uuid.Parse` no handler `/estoque/:id/repor` valida e retorna 400 em vez de usar UUID zero
- **R1.7**: Allowlist de tabelas e colunas em `data-sync/infra/applier.go` — previne SQL injection via mensagens Debezium maliciosas
- **R1.8**: DSNs hardcoded removidos de todos os stores (cell-estoque, cell-pedidos, cell-notificacoes, saga-hub) — `DATABASE_URL` é obrigatória
- **R1.9**: Todos os 5 consumers Kafka mudaram de `enable.auto.commit: true` para `false` com commit manual pós-processamento — garante at-least-once; failCounts agora por offset (R4.5), detecção de timeout via `kafka.Error.Code()` (R4.4)
- **R2.5**: `var _ = time.Now` artifact removido de `saga-hub/infra/db/store.go`
- **R3.5**: `failSaga` publica evento `events.pedidos.falhou` para rastreabilidade de falhas

---

### v2.8.0 — 2026-05-04

**Revisão de código completa + REFINE.md (SDD de refinamento)**

- **Revisão de código:** varredura de todos os 120+ arquivos do repositório — Go, Python, YAML, shell scripts, Dockerfiles e manifests k8s. 47 melhorias identificadas e classificadas em 5 categorias de severidade.
- **`REFINE.md`** adicionado: Software Design Document com plano de execução completo das 47 melhorias em 6 fases (`R1`–`R6`). Cada item especifica o problema, a solução com pseudocódigo e o critério de verificação. Inclui decisões de design globais (módulo `shared`, módulo `contract`, refatoração de cell-notificacoes), ordem de execução com árvore de dependências e critérios globais de aceitação com comandos bash executáveis.
- **Correções pontuais** incluídas no mesmo commit: compatibilidade bash 3.2 no `check.sh` (substituição de associative array por `tmpfile+sort -u`, correção de prefixo/sufixo dos containers) e `cliente_id` adicionado ao payload do comando `criar_pedido` no `saga-hub/orchestrator/pedido.go`.

**Melhorias identificadas por categoria:**

| Categoria | Qtd | Exemplos |
|---|---|---|
| Bugs críticos e segurança | 9 | Reserva sem transação atômica, compensação não libera estoque, SQL injection no data-sync, at-most-once no Kafka |
| Duplicação de código | 6 | `setupOTel` em 5 componentes, `resilience/` em 6 módulos, passive-mode em 3 cells |
| Inconsistência arquitetural | 8 | cell-notificacoes god-file, estoqueProcessor inline, `CorrelationID == SagaID` |
| Observabilidade e runtime | 10 | `ipLimiter` sem TTL, `ReverseProxy` por request, SLO de latência ausente, regras Prometheus não montadas |
| CI/CD e infraestrutura | 15 | Sem cache de módulos, ferramentas `@latest`, Kafka com ZooKeeper, sem PDB, sem NetworkPolicy |
| Testes | 3 | Zero `*_test.go`, `BOUNDARY_RULES` inócuo no FF1, FF2 sem JWT |

---

### v2.7.0 — 2026-05-04

**§9 — Critérios Globais de Aceitação**

- **`check.sh`**: script completo dos critérios globais do PROMPT.md §9. Modo estático (`./check.sh`): BUILD (6 componentes), BOUNDARY CHECK (L1 — zero imports entre PBCs), SLO RULES (promtool). Modo completo (`./check.sh --full`, requer stack up): STACK HEALTH (18 containers), SHARD ROUTING (distribuição X-Client-ID), PASSIVE MODE (HTTP 503 em escrita), SAGA E2E (COMPLETED).
- Todos os checks estáticos passam: `ALL STATIC CHECKS PASSED`.

---

### v2.6.0 — 2026-05-04

**F5 — IA Avançada (completa)**

- **F5.1 — Self-healing com consciência de shards** (`agent-mcp/main.py`): `ALL_TOOLS = MCP_TOOLS + SHARD_TOOLS` — Claude recebe todas as 12 tools incluindo `listar_status_shards`, `verificar_saga`, `iniciar_saga_pedido`, `reiniciar_celula` e `consultar_prometheus`. Loop de monitoramento integra anomaly detection: se IsolationForest detecta anomalia, aciona Claude com prompt de self-healing que usa `listar_status_shards` + `consultar_prometheus` + `reiniciar_celula`; caso normal, faz checagem periódica de shards.
- **F5.2 — Anomaly detection** (`agent-mcp/anomaly/detector.py`): `AnomalyDetector` com `IsolationForest` monitorando `shard_router_requests_total`, `data_sync_lag_seconds`, `saga_duration_seconds` e `circuit_breaker_state`. Treina progressivamente com histórico (mínimo 10 amostras). Exposto em `GET /agente/anomalias`.
- **F5.3 — Predictive scaling** (`agent-mcp/scaling/predictor.py`): `EMAPPredictor` com EMA (α=0.3) + slope-based forecast de 5 passos à frente por célula (pedidos, estoque, notificacoes). Recomenda réplicas baseado em RPS previsto / 50. Exposto em `GET /agente/scaling/previsao`.
- `__init__.py` adicionado nos packages `anomaly/` e `scaling/` para importação correta no loop autônomo.
- `monitor_log` enriquecido com `anomalia`, `previsoes_scaling` e `tipo` (`monitoramento` | `self-healing`).

---

### v2.5.0 — 2026-05-04

**F4 — FinOps + SRE (completa)**

- **SLO rules** (`infra/monitoring/slo-rules.yml`): recording rules `shard:availability:rate5m` e `shard:error_budget_burn` com SLO de 99.9% — validado com `promtool check rules` (SUCCESS: 2 rules found).
- **Alert rules** (`infra/monitoring/alert-rules.yml`): 6 alertas operacionais — `ActiveCellDown` (30s), `BothCellsDown` (10s), `DataSyncLagHigh` (lag > 5s por 1min), `SagaFailureRateHigh` (> 5% por 5min), `CircuitBreakerOpen` (1min), `ErrorBudgetBurn` (burn > 14.4x por 5min). Cada alert referencia seu runbook. Validado com `promtool` (SUCCESS: 6 rules found).
- **Runbooks** (`runbooks/`): `shard-failover.md`, `data-sync-lag.md` e `circuit-breaker.md` conformes ao template com Trigger, Diagnóstico, Ações corretivas (cenários A/B/C), Verificação de resolução e Escalação.
- **FinOps metrics** (`infra/monitoring/finops.go`): `cell_transaction_cost_cents` (histogram), `cell_db_queries_total` (counter por operação), `cell_kafka_messages_total` (counter por tópico e direção). Instrumentados em `cell-pedidos` (store Salvar/BuscarPorID + producer PublishReply), `cell-estoque` (idem) e `cell-notificacoes` (publishReply).
- `prometheus.yml` atualizado com `rule_files` carregando `slo-rules.yml` e `alert-rules.yml`.

---

### v2.4.0 — 2026-05-04

**F3 — Segurança (completa)**

- **JWT middleware** (`infra/auth/jwt.go`): valida Bearer token via JWKS (`github.com/lestrrat-go/jwx/v2 v2.0.21`), cache de chaves com refresh de 15min, extrai `sub` e `roles` para o contexto Gin. Aplicado em rotas de escrita: `POST /pedidos/` e `PUT /estoque/:id/repor`. Dev mode: sem `JWKS_URL` passa direto sem bloquear.
- **IP Rate Limiter** (`infra/middleware/ratelimit.go`): `golang.org/x/time/rate` — 100 req/s com burst de 50 por IP, retorna HTTP 429. Registrado como middleware global em todas as células.
- **SAST no CI** (`.github/workflows/fitness-functions.yml`): já presente — `govulncheck` + `gosec -severity medium` em todos os 6 módulos Go a cada push.
- **Audit Log** (`infra/audit/logger.go`): publica eventos no tópico Kafka `audit.events` com `id`, `component`, `shard_id`, `action`, `resource_type`, `resource_id`, `actor_id` e `payload`. Integrado em `criar_pedido` (cell-pedidos), `repor_estoque` (cell-estoque) e `enviar_notificacao` (cell-notificacoes).
- Dependências adicionadas a `cell-pedidos`, `cell-estoque`, `cell-notificacoes`: `github.com/lestrrat-go/jwx/v2 v2.0.21`, `golang.org/x/time v0.5.0`.

---

### v2.3.0 — 2026-05-03

**F2 — Performance (completa)**

- **Redis cache por célula** (`infra/cache/redis.go`): package `cache` idêntico nos 3 módulos de célula — `Get`/`Set`/`Del` com JSON serialization, TTL configurável, pool de 10 conexões, degradação graciosa se Redis indisponível
- **Wiring cache-aside em `cell-pedidos`**: `BuscarPorID` lê do cache antes do DB; `Salvar` invalida a chave após persistência; prefixo `pedidos:`, TTL 60s
- **Wiring cache-aside em `cell-estoque`**: mesmo padrão para `Produto`; prefixo `estoque:`, TTL 60s
- **Wiring cache-aside em `cell-notificacoes`**: `Listar` cacheia lista com chave `notificacoes:list`; `Salvar` invalida; TTL 5s (lista muda frequentemente)
- **CQRS read model** (`infra/db/query_store.go`): `Stats()` com queries agregadas separadas do `Store` de escrita — já presente em `cell-pedidos` e `cell-estoque`; endpoints `GET /pedidos/stats` e `GET /estoque/stats`
- **Kubernetes manifests** completos: antiAffinity (`requiredDuringSchedulingIgnoredDuringExecution`), liveness/readiness probes (`/healthz/live`, `/healthz/ready`), resource requests/limits (CPU 100m–500m, memory 64Mi–256Mi), HPA `autoscaling/v2` (min=1, max=5, CPU 70%)
- Dependência `github.com/redis/go-redis/v9 v9.5.1` adicionada a `cell-pedidos`, `cell-estoque`, `cell-notificacoes`

---

### v2.2.0 — 2026-05-03

**F1 — Resiliência (completa)**

- `infra/resilience/` criado em todos os 6 módulos Go (`cell-pedidos`, `cell-estoque`, `cell-notificacoes`, `saga-hub`, `shard-router`, `data-sync`)
- **Circuit Breaker** (`breaker.go`): `Breaker.Execute(fn)` wrapping `gobreaker` — 3 falhas consecutivas abrem o CB, timeout de 30s para half-open, estado exposto via métrica `circuit_breaker_state{component,shard,breaker}`
- **Retry com backoff** (`retry.go`): `Retry(ctx, maxAttempts, base, fn)` — exponential backoff com jitter (`rand/v2`), respeita context deadline
- **Bulkhead** (`bulkhead.go`): semáforo com capacidade 10 (cells) / 20 (saga-hub) — rejeições contabilizadas em `bulkhead_rejected_total{component,shard,name}`
- **Wiring CB+Retry**: acessos a PostgreSQL (Salvar, BuscarPorID) em `cell-pedidos`, `cell-estoque`, `cell-notificacoes`, `saga-hub`; publicações Kafka em todos os producers
- **Wiring Bulkhead**: consumers Kafka de todas as células e do saga-hub envolvem `ProcessCommand`/`HandleReply` com bulkhead — rejeições por capacidade são descartadas sem incrementar failCount
- **Timeout middleware** adicionado ao `saga-hub/cmd/main.go` (5s por request)
- **CB no shard-router**: um `Breaker` por PBC (`pedidos`, `estoque`, `notificacoes`) envolve cada chamada de proxy reverso; falhas de transporte são contabilizadas no CB
- Dependência `github.com/sony/gobreaker v0.5.0` adicionada a todos os 6 módulos

---

### v2.1.0 — 2026-05-03

**Arquitetura de shards e SAGA Hub (F0)**

- Adicionado `shard-router`: consistent hashing via `sha256(X-Client-ID) % 3`, registry de células ativas/passivas com health-check contínuo
- Adicionado `saga-hub`: orquestrador de SAGAs com padrão Async Request-Reply via Kafka, persistência de estado em PostgreSQL, compensação automática (reserva falha → cancelar pedido + notificar cliente), métricas Prometheus (`saga_started_total`, `saga_completed_total`, `saga_duration_seconds`)
- Adicionado `data-sync`: consumer CDC Debezium para replicação ativo→passivo por shard

**PBCs refatorados para Async Request-Reply**

- `cell-pedidos`, `cell-estoque`, `cell-notificacoes`: migrados de comunicação síncrona para consumo de `commands.*` e publicação em `replies.*`
- Dead Letter Queue (DLQ) implementada em todas as células (`infra/messaging/dlq.go`)
- CQRS read model adicionado em `cell-pedidos` e `cell-estoque` (`infra/db/query_store.go`)

**Kubernetes (F2 parcial)**

- Manifests completos para deploy em produção: `k8s/base/namespace.yaml`, `k8s/core/shard-router.yaml`, `k8s/core/saga-hub.yaml`
- Deployments e HPAs para todos os shards (shard-1/2/3) com active/passive por célula (18 deployments total)

**IA Avançada (F5 parcial)**

- `agent-mcp/anomaly/detector.py`: detecção de anomalias com `IsolationForest` sobre métricas Prometheus (`shard_router_requests_total`, `data_sync_lag_seconds`, `saga_duration_seconds`, `circuit_breaker_state`)
- `agent-mcp/scaling/predictor.py`: predictive scaling baseado em histórico de métricas
- `agent-mcp/main.py` expandido com novas MCP tools para consulta de SAGAs e shards

**SRE e Observabilidade (F4 parcial)**

- Runbooks operacionais: `runbooks/circuit-breaker.md`, `runbooks/data-sync-lag.md`, `runbooks/shard-failover.md`
- `infra/monitoring/prometheus.yml` e dashboards Grafana atualizados com métricas de saga-hub e shard-router
- CI pipeline `fitness-functions.yml` adicionado ao GitHub Actions (executa a cada push em `main`)

**Infraestrutura**

- `docker-compose.yml` expandido com serviços `shard-router`, `saga-hub`, `data-sync`, databases dedicados por componente
- `demo.sh` expandido com cenários de demonstração para todos os novos componentes
- `teardown.sh`: script de limpeza completa — para e remove containers, volumes, rede `poc-eci` e cache Python; flag `--images` remove também as imagens buildadas
- `Makefile` atualizado com targets `fitness-check`, `k8s-apply`, `build-all`
- `PROMPT.md` (Master Prompt v3, 79KB): especificação arquitetural completa adicionada ao repositório

---

### v2.0.0 — 2026-05-02

**Release inicial**

- `cell-pedidos`, `cell-estoque`, `cell-notificacoes`: PBCs em Go 1.22 + Gin + Kafka + PostgreSQL
- `agent-mcp`: agente Python + Anthropic SDK + FastAPI com loop de monitoramento autônomo
- `fitness-functions/run_all.py`: FF1 (isolamento Go static), FF2 (health), FF3 (latência p99), FF4 (resiliência)
- `docker-compose.yml`: Traefik, Kafka, Prometheus, Grafana, Jaeger, Kafka UI
- `demo.sh`: script de demonstração manual da SAGA

---

*PoC v2 — Go + Kafka — baseada no artigo "Como Construir uma Empresa Componível Inteligente" — Rafael Sá Anselmo (2026)*
