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
│   │   ├── db/store.go
│   │   ├── db/query_store.go   # CQRS read model
│   │   ├── messaging/kafka.go
│   │   ├── messaging/producer.go
│   │   └── messaging/dlq.go    # Dead Letter Queue
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
├── infra/monitoring/
│   ├── prometheus.yml
│   ├── grafana-datasources.yml
│   └── grafana-dashboards.yml
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
└── PROMPT.md                   # Master Prompt v3 — especificação arquitetural completa
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
```

---

## Status de implementação por fase

| Fase | Descrição | Status |
|---|---|---|
| F0 — Fundação | shard-router, saga-hub, data-sync, PBCs Async Request-Reply | ✅ Completo |
| F1 — Resiliência | Circuit Breaker, Retry+backoff, Bulkhead, Timeout, DLQ | 🟡 Parcial (DLQ implementada) |
| F2 — Performance | Kubernetes manifests, Redis cache, CQRS read model | 🟡 Parcial (k8s + CQRS) |
| F3 — Segurança | JWT middleware, Rate limiting, SAST pipeline, Audit log | ⬜ Pendente |
| F4 — FinOps + SRE | SLO rules, Alert rules, Runbooks | 🟡 Parcial (Runbooks) |
| F5 — IA Avançada | Self-healing, Anomaly detection, Predictive scaling | 🟡 Parcial (anomaly + scaling) |

---

## Changelog

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
