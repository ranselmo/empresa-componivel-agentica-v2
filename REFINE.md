# REFINE — Software Design Document
## Plano de Refinamento do PoC ECI v2

**Versão:** 1.0  
**Base:** Revisão de código completa realizada em 2026-05-04  
**Repositório:** `empresa-componivel-agentica-v2`  
**Stack:** Go 1.22 · Python 3.12 · Kafka · PostgreSQL · Redis · Docker Compose · Kubernetes

---

## §0 — Contexto e Objetivo

Este documento é a especificação de execução para as 47 melhorias identificadas na revisão de código do PoC ECI v2. Os itens estão organizados em 6 fases ordenadas por dependência e severidade:

| Fase | Tema | Itens | Impacto |
|------|------|-------|---------|
| R1 | Bugs críticos e segurança | 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1, 4.2, 4.5 | Correção de comportamento incorreto e vulnerabilidades |
| R2 | Eliminação de duplicação | 1.1–1.6 | Módulo `shared`, 6 packages consolidados |
| R3 | Consistência arquitetural | 2.1–2.3, 4.3, 4.4, 6.1, 6.4, 8.4 | cell-notificacoes e cell-estoque no padrão de cell-pedidos |
| R4 | Observabilidade e runtime | 5.1–5.4, 7.1–7.5, 8.1–8.3 | Leaks, métricas corretas, Prometheus completo |
| R5 | CI/CD e Infraestrutura | 9.1–9.5, 10.1–10.6, 11.1–11.6 | Pipeline confiável, k8s completo, agent-mcp robusto |
| R6 | Testes | 12.1–12.3 | Cobertura mínima de domínio e contrato |

### Regra zero (herdada do PROMPT.md)
> Código que não compila não existe. Após cada arquivo alterado: `cd <componente> && go build ./...`

### Leis arquiteturais (não podem ser violadas)
- **L1:** PBCs são ilhas — zero imports entre PBCs
- **L2:** saga-hub é o único integrador
- **L3:** Toda requisição entra pelo shard-router
- **L4:** Células passivas recusam escrita (HTTP 503)
- **L5:** Scale Unit = 1 célula + 1 DB + 1 Redis + 1 consumer group

---

## §1 — Decisões de Design Globais

### D1 — Módulo `shared` como replace local
As utilidades comuns (resilience, telemetry) serão extraídas para um módulo Go local `shared/` referenciado via `replace` em cada `go.mod`. Isso respeita L1 (PBCs não se importam) porque `shared` não contém lógica de domínio — apenas infraestrutura pura.

```
shared/
  go.mod                    (module github.com/ranselmo/poc-eci/shared)
  telemetry/otel.go         (setupOTel unificado)
  resilience/
    breaker.go
    bulkhead.go
    retry.go
  middleware/
    passive.go              (passive-mode block)
    ratelimit.go
    timeout.go
  infra/
    auth/jwt.go
    audit/logger.go
    cache/redis.go
    monitoring/finops.go
```

Cada `go.mod` dos componentes que usarem `shared` adiciona:
```
require github.com/ranselmo/poc-eci/shared v0.0.0
replace github.com/ranselmo/poc-eci/shared => ../shared
```

### D2 — Módulo `contract` para tipos de mensagem Kafka
Os tipos `Command`/`Reply`/`BusinessEvent` redefinidos em cada cell serão extraídos para `contract/`. Este módulo não contém lógica de negócio — apenas structs e constantes de tópico. L1 não é violada pois PBCs nunca importam **uns aos outros**, apenas o contrato comum.

```
contract/
  go.mod
  types.go      (Command, Reply, BusinessEvent)
  topics.go     (constantes de tópico — duplicadas hoje entre saga-hub e cells)
```

### D3 — Refatoração de cell-notificacoes no padrão cell-pedidos
`cell-notificacoes/cmd/main.go` (407 linhas) será desmembrado em:
```
cell-notificacoes/
  domain/notificacao.go     (tipo Notificacao, construtor, validações)
  infra/db/store.go         (Store com pgxpool, cache, breaker)
  infra/messaging/
    kafka.go                (Consumer + tópicos)
    producer.go             (Producer + PublishReply)
    dlq.go                  (SendToDLQ — mesmo padrão das outras cells)
  cmd/main.go               (apenas bootstrap — < 100 linhas)
```

### D4 — Transação atômica para reserva de estoque
A reserva multi-item precisa de uma única transação PostgreSQL. O `Store.ReservarItens(ctx, itens)` será adicionado usando `pool.Begin()` → loop de SELECT FOR UPDATE + UPDATE → `tx.Commit()`.

### D5 — Commit manual no Kafka (at-least-once)
Todos os consumers mudarão para `"enable.auto.commit": false` com chamada explícita de `c.CommitMessage(msg)` após processamento bem-sucedido. Mensagens que vão para DLQ também fazem commit (para não reprocessar indefinidamente).

---

## §2 — FASE R1: Bugs Críticos e Segurança

### R1.1 — Reserva de estoque sem transação atômica
**Origem:** item 3.1 da revisão  
**Arquivo:** `cell-estoque/infra/db/store.go` (a ser criado em R3, mas a correção do bug é em R1)  
**Problema:** loop de `BuscarPorID` + `Salvar` por item sem transação — falha no item N deixa N-1 itens decrementados sem rollback.

**Solução:** Adicionar método `ReservarItens(ctx, itens []ReservaItem) error` que usa uma única transação:
```go
// Pseudocódigo da implementação
func (s *Store) ReservarItens(ctx context.Context, itens []ReservaItem) error {
    tx, err := s.pool.Begin(ctx)
    if err != nil { return err }
    defer tx.Rollback(ctx)

    for _, item := range itens {
        // SELECT quantidade_disponivel FROM produtos WHERE id=$1 FOR UPDATE
        // se quantidade < solicitada → return ErrEstoqueInsuficiente (faz Rollback via defer)
        // UPDATE produtos SET quantidade_disponivel = quantidade_disponivel - $1 WHERE id=$2
    }
    return tx.Commit(ctx)
}
```

**Verificação:** `go build ./...` em cell-estoque + teste manual de reserva com 2 itens onde o segundo tem estoque insuficiente.

---

### R1.2 — Compensação não libera estoque
**Origem:** item 3.2 da revisão  
**Arquivos:** `saga-hub/domain/saga.go`, `saga-hub/orchestrator/pedido.go`  
**Problema:** Fluxo de compensação emite `cancelar_pedido` mas não `liberar_estoque`. O método `Produto.Liberar()` existe no domínio mas nunca é invocado pela orquestração.

**Solução:**
1. Adicionar constante de tópico em `saga-hub/domain/saga.go`:
   ```go
   TopicCmdEstoqueLibertar = "commands.estoque.libertar"
   TopicReplyEstoqueLiberado = "replies.estoque.liberado"
   ```
2. Adicionar `StepLiberarEstoque SagaStep = "LIBERAR_ESTOQUE"` ao domínio.
3. Em `onEstoqueReply`, quando `reply.Status == "failure"`, o fluxo de compensação deve ser:
   - Emitir `cancelar_pedido` (já existe)
   - Após reply de cancelamento: emitir `liberar_estoque`
4. Em `onPedidoCancelado`, antes de mudar status para `FAILED`, emitir o comando de liberação de estoque.
5. Cell-estoque deve consumir `commands.estoque.libertar` e chamar `store.LiberarItens()` (transação análoga a `ReservarItens`).

**Nota:** Para a saga ser idempotente, `LiberarItens` deve usar `quantidade_disponivel = quantidade_disponivel + $1` com cláusula de guarda.

---

### R1.3 — `Pedido.Cancelar()` com erro silenciado
**Origem:** item 3.3 da revisão  
**Arquivo:** `cell-pedidos/api/handlers.go:89`  
**Problema:** `_ = pedido.Cancelar()` — erro de negócio ignorado.

**Solução:**
```go
if err := pedido.Cancelar(); err != nil {
    return messaging.Reply{}, fmt.Errorf("cancelar pedido: %w", err)
}
```
O erro propaga para o caller, que envia reply de falha ao saga-hub, que aciona compensação correta.

---

### R1.4 — `store.Save(saga)` com erro silenciado no orchestrator
**Origem:** item 3.4 da revisão  
**Arquivo:** `saga-hub/orchestrator/pedido.go`  
**Problema:** `_ = o.store.Save(ctx, saga)` em 5 pontos — falha de persistência é silenciada e a saga progride sem estado salvo.

**Solução:** Todos os `_ = o.store.Save(ctx, saga)` devem retornar o erro:
```go
if err := o.store.Save(ctx, saga); err != nil {
    return fmt.Errorf("persist saga step %s: %w", saga.CurrentStep, err)
}
```
O erro retornado faz o consumer tentar novamente (via retry do consumer loop). Após R1.8 (commit manual), a mensagem será reprocessada.

---

### R1.5 — `json.Unmarshal` sem verificação de erro
**Origem:** item 3.5 da revisão  
**Arquivo:** `cell-pedidos/infra/db/store.go:138,173`  
**Problema:** `json.Unmarshal(itensRaw, &raw)` sem `if err != nil`.

**Solução:** Adicionar verificação em ambos os sites:
```go
if err := json.Unmarshal(itensRaw, &raw); err != nil {
    return nil, fmt.Errorf("unmarshal itens: %w", err)
}
```

---

### R1.6 — `uuid.Parse` silenciado no endpoint de reposição
**Origem:** item 3.6 da revisão  
**Arquivo:** `cell-estoque/cmd/main.go:236`  
**Problema:** `id, _ := uuid.Parse(c.Param("id"))` — UUID inválido gera `uuid.Nil`, busca retorna 404 sem mensagem clara.

**Solução:**
```go
id, err := uuid.Parse(c.Param("id"))
if err != nil {
    c.JSON(http.StatusBadRequest, gin.H{"error": "id inválido"})
    return
}
```

---

### R1.7 — SQL Injection em `data-sync/infra/applier.go`
**Origem:** item 4.1 da revisão  
**Arquivo:** `data-sync/infra/applier.go`  
**Problema:** Nome de tabela e colunas interpolados diretamente via `fmt.Sprintf` — se um tópico Kafka malicioso for publicado, a estrutura SQL é injetada.

**Solução:** Criar uma allowlist de tabelas e colunas permitidas:
```go
var allowedTables = map[string]map[string]bool{
    "pedidos":      {"id": true, "cliente_id": true, "status": true, "valor_total": true, "itens": true, "criado_em": true, "atualizado_em": true},
    "produtos":     {"id": true, "nome": true, "quantidade_disponivel": true, "preco": true, "atualizado_em": true},
    "notificacoes": {"id": true, "destinatario_id": true, "tipo": true, "canal": true, "conteudo": true, "enviado_em": true},
}

func validateTable(table string) error {
    if _, ok := allowedTables[table]; !ok {
        return fmt.Errorf("tabela não permitida: %s", table)
    }
    return nil
}

func validateColumns(table string, cols map[string]any) error {
    allowed := allowedTables[table]
    for k := range cols {
        if !allowed[k] {
            return fmt.Errorf("coluna não permitida: %s.%s", table, k)
        }
    }
    return nil
}
```
Chamar `validateTable` e `validateColumns` no início de `applyInsert`, `applyUpdate`, `applyDelete`.

---

### R1.8 — Credenciais hardcoded nos defaults de DSN
**Origem:** item 4.2 da revisão  
**Arquivos:**
- `saga-hub/infra/db/store.go:24`
- `cell-pedidos/infra/db/store.go:29`
- `cell-notificacoes/cmd/main.go:70`

**Solução:** Remover os defaults com senha. Se `DATABASE_URL` não estiver definida, o serviço deve falhar de forma explícita:
```go
dsn := os.Getenv("DATABASE_URL")
if dsn == "" {
    return nil, fmt.Errorf("DATABASE_URL não definida")
}
```
Os defaults de dev são gerenciados pelo `docker-compose.yml` via variáveis de ambiente — não no código.

---

### R1.9 — `enable.auto.commit: true` (at-most-once)
**Origem:** item 4.5 da revisão  
**Arquivos:** `saga-hub/infra/messaging/consumer.go`, `cell-pedidos/infra/messaging/kafka.go`, `cell-estoque/infra/messaging/kafka.go`, `cell-notificacoes/cmd/main.go`, `data-sync/infra/applier.go`

**Solução:** Mudar para commit manual em todos os consumers:
```go
// Configuração
"enable.auto.commit": false,
"enable.auto.offset.store": false,

// Após processamento bem-sucedido (inclusive envio para DLQ):
if _, err := cs.c.CommitMessage(msg); err != nil {
    slog.Error("commit kafka offset", "err", err)
}
```
Mensagens que falham e NÃO foram para DLQ (ex: bulkhead reject) não fazem commit — serão reprocessadas após restart.

---

## §3 — FASE R2: Eliminação de Duplicação

### R2.1 — Criar módulo `shared/`
**Origem:** itens 1.1–1.6 da revisão

**Estrutura a criar:**
```
shared/
  go.mod
  telemetry/
    otel.go           (setupOTel parametrizado por serviceName)
  resilience/
    breaker.go        (idêntico ao atual — apenas mover)
    bulkhead.go
    retry.go
  middleware/
    passive.go        (bloco passive-mode extraído)
    ratelimit.go      (extraído das cells)
    timeout.go        (middleware de timeout de 5s extraído do main())
  infra/
    auth/jwt.go       (extraído das cells)
    audit/logger.go   (extraído das cells)
    cache/redis.go    (extraído das cells)
    monitoring/
      finops.go       (extraído das cells — métricas genéricas por label)
```

**`shared/go.mod`:**
```
module github.com/ranselmo/poc-eci/shared
go 1.22
require (
    github.com/gin-gonic/gin v1.10.0
    github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
    github.com/google/uuid v1.6.0
    github.com/lestrrat-go/jwx/v2 v2.0.21
    github.com/prometheus/client_golang v1.19.0
    github.com/redis/go-redis/v9 v9.5.1
    github.com/sony/gobreaker v0.5.0
    go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.49.0
    go.opentelemetry.io/otel v1.24.0
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
    go.opentelemetry.io/otel/sdk v1.24.0
    golang.org/x/time v0.5.0
)
```

**Em cada componente que usar shared, adicionar ao `go.mod`:**
```
require github.com/ranselmo/poc-eci/shared v0.0.0
replace github.com/ranselmo/poc-eci/shared => ../shared
```

---

### R2.2 — `telemetry/otel.go` — setupOTel unificado
**Arquivo:** `shared/telemetry/otel.go`

```go
package telemetry

import (
    "context"
    "log/slog"
    "os"
    // ... imports otel
)

func Setup(ctx context.Context, serviceName string) func() {
    ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if ep == "" {
        ep = "http://jaeger:4317"
    }
    // ... implementação atual (idêntica para todos)
}
```

Cada `cmd/main.go` passa de:
```go
func setupOTel(ctx context.Context) func() { /* 20 linhas */ }
defer setupOTel(ctx)()
```
Para:
```go
defer telemetry.Setup(ctx, "cell-pedidos")()
```

---

### R2.3 — `middleware/passive.go` — bloco passive-mode extraído
**Arquivo:** `shared/middleware/passive.go`

```go
package middleware

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

// BlockWrites registra handlers 503 para todos os métodos de escrita.
// Deve ser chamado quando CELL_ROLE=passive.
func BlockWrites(r *gin.Engine, shardID string) {
    for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
        r.Handle(method, "/*path", func(c *gin.Context) {
            c.JSON(http.StatusServiceUnavailable, gin.H{
                "error":     "cell is passive — writes forbidden",
                "cell_role": "passive",
                "shard_id":  shardID,
            })
        })
    }
}
```

Cada `cmd/main.go` substitui o bloco duplicado por:
```go
if cellRole == "passive" {
    middleware.BlockWrites(r, shardID)
    slog.Info("passive cell — write routes blocked", "shard", shardID)
} else {
    h.RegisterRoutes(r)
}
```

---

### R2.4 — `monitoring/finops.go` — métricas unificadas por label
**Arquivo:** `shared/infra/monitoring/finops.go`

O arquivo atual em cada cell declara métricas com os mesmos nomes (`cell_transaction_cost_cents` etc.). No módulo shared, as métricas são registradas uma única vez e discriminadas por label `component`:

```go
package monitoring

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

**Atenção:** Remover os arquivos `infra/monitoring/finops.go` de dentro de cada cell após migrar para shared. Manter apenas o `infra/monitoring/` na raiz do repo (que hoje está sem uso) e deletá-lo também.

---

### R2.5 — Remover `var _ = time.Now` em saga-hub
**Arquivo:** `saga-hub/infra/db/store.go:115`  
**Ação:** Deletar a linha. Verificar que `time` ainda é usado no mesmo arquivo (é — em `Retry`).

---

## §4 — FASE R3: Consistência Arquitetural

### R3.1 — Refatorar `cell-notificacoes` no padrão de `cell-pedidos`
**Origem:** itens 2.1, 2.3 da revisão

**Criar os seguintes arquivos:**

**`cell-notificacoes/domain/notificacao.go`:**
```go
package domain

import (
    "time"
    "github.com/google/uuid"
)

type CanalNotificacao string

const (
    CanalEmail CanalNotificacao = "email"
    CanalSMS   CanalNotificacao = "sms"
    CanalPush  CanalNotificacao = "push"
)

type Notificacao struct {
    ID             uuid.UUID
    DestinatarioID uuid.UUID
    Tipo           string
    Canal          CanalNotificacao
    Conteudo       string
    EnviadoEm      time.Time
}

func NewNotificacao(destinatarioID uuid.UUID, tipo string, canal CanalNotificacao, conteudo string) *Notificacao {
    return &Notificacao{
        ID: uuid.New(), DestinatarioID: destinatarioID,
        Tipo: tipo, Canal: canal, Conteudo: conteudo,
        EnviadoEm: time.Now().UTC(),
    }
}
```

**`cell-notificacoes/infra/db/store.go`:**
Extrair `Store`, `newStore`, `Salvar`, `Listar`, `Ping`, `Close` do `cmd/main.go` atual para este arquivo. Adicionar `Migrate()` como método separado (padrão das outras cells).

**`cell-notificacoes/infra/messaging/kafka.go`:**
Extrair `Command`, `CommandProcessor`, `Consumer`, `Run`, `replyTopicFor` do `cmd/main.go` para este arquivo. Seguir o padrão de `cell-pedidos/infra/messaging/kafka.go` com `CommandProcessor` interface.

**`cell-notificacoes/infra/messaging/producer.go`:**
Extrair `newProducer`, `publishReply` e constantes de tópico para este arquivo. Transformar em struct `Producer` com métodos, como em `cell-pedidos`.

**`cell-notificacoes/infra/messaging/dlq.go`:**
Extrair `SendToDLQ` (hoje via `messaging.SendToDLQ` — verificar onde está) como método do Producer.

**`cell-notificacoes/cmd/main.go` após refatoração:**
Deve ter menos de 100 linhas, responsável apenas por: inicializar componentes, montar rotas, e fazer graceful shutdown.

---

### R3.2 — Refatorar `cell-estoque` — extrair handlers do `main.go`
**Origem:** item 2.2 da revisão

**Criar `cell-estoque/api/handlers.go`:**
Extrair as rotas inline do `main()` de `cell-estoque/cmd/main.go`:
```
GET  /estoque/stats   → Handler.Stats()
GET  /estoque/        → Handler.Listar()
GET  /estoque/:id     → Handler.BuscarProduto()
PUT  /estoque/:id/repor → Handler.ReporEstoque()  [com jwtMW]
```

Seguir o padrão de `cell-pedidos/api/handlers.go`: struct `Handler` com `RegisterRoutes(r *gin.Engine)`.

**`cell-estoque/cmd/main.go` após refatoração:**
Deve ser bootstrap puro — instanciar `Handler`, chamar `h.RegisterRoutes(r)`.

---

### R3.3 — Mover lógica de comando do estoque para `api/handlers.go`
**Origem:** item 2.2 da revisão  
**Arquivo:** `cell-estoque/cmd/main.go` — `estoqueProcessor` struct e métodos `reservar`/`liberar`

`estoqueProcessor` deve ser movido para `cell-estoque/api/handlers.go` ou para um arquivo dedicado `cell-estoque/api/processor.go`, implementando a interface `messaging.CommandProcessor`.

---

### R3.4 — Separar `CorrelationID` de `SagaID` no domínio
**Origem:** item 6.1 da revisão  
**Arquivo:** `saga-hub/domain/saga.go:59`

**Problema:** `CorrelationID: id` (mesmo valor que `ID`) torna os dois campos redundantes.

**Solução:** O caller de `NewSaga` deve passar um `correlationID` externo (gerado pelo cliente ou pelo shard-router):
```go
func NewSaga(correlationID, clienteID uuid.UUID, shardID string, payload map[string]any) *Saga {
    id := uuid.New()
    now := time.Now().UTC()
    return &Saga{
        ID: id, CorrelationID: correlationID,
        // ...
    }
}
```

Atualizar `saga-hub/api/handlers.go` — o `IniciarRequest` recebe `correlation_id` opcional; se ausente, gera um novo UUID. Retornar o `correlation_id` na resposta para o cliente rastrear.

**Atenção:** Esta mudança afeta o `UNIQUE` constraint em `correlation_id` no banco — que deve ser mantido, pois agora correlation_id pode vir do cliente (idempotência real).

---

### R3.5 — `failSaga` deve notificar o cliente
**Origem:** item 6.2 da revisão  
**Arquivo:** `saga-hub/orchestrator/pedido.go`

Quando `criar_pedido` falha (primeiro step), `failSaga` deve emitir um evento `PedidoFalhou`:
```go
func (o *PedidoSaga) failSaga(ctx context.Context, saga *domain.Saga, reason string) error {
    saga.Status = domain.StatusFailed
    saga.UpdatedAt = time.Now().UTC()
    if err := o.store.Save(ctx, saga); err != nil { return err }
    sagaDone.WithLabelValues("pedido", "failed").Inc()
    o.prod.PublishBusinessEvent(domain.TopicEventPedidoFalhou, domain.BusinessEvent{
        EventID: uuid.New(), EventType: "PedidoFalhou",
        ShardID: saga.ShardID, OccurredAt: time.Now().UTC(),
        Payload: map[string]any{"reason": reason, "saga_id": saga.ID},
    })
    slog.Error("saga failed", "saga_id", saga.ID, "reason", reason)
    return nil
}
```
Adicionar `TopicEventPedidoFalhou = "events.pedidos.falhou"` em `domain/saga.go`.

---

### R3.6 — `PublishBusinessEvent` com circuit breaker
**Origem:** item 8.4 da revisão  
**Arquivo:** `saga-hub/infra/messaging/producer.go:67`

`PublishBusinessEvent` deve passar pelo breaker, igual a `PublishCommand`:
```go
func (pr *Producer) PublishBusinessEvent(topic string, ev domain.BusinessEvent) {
    b, err := json.Marshal(ev)
    if err != nil {
        slog.Error("marshal business event", "err", err)
        return
    }
    key := ev.EventID.String()
    if err := pr.breaker.Execute(func() error {
        return pr.p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Key:            []byte(key), Value: b,
        }, nil)
    }); err != nil {
        slog.Error("publish business event", "topic", topic, "err", err)
    }
}
```

---

### R3.7 — Unificar TTL de cache via env var
**Origem:** item 6.4 da revisão  
**Arquivos:** `cell-pedidos/infra/db/store.go`, `cell-notificacoes/cmd/main.go` (após R3.1: `infra/db/store.go`)

Ler TTL de `CACHE_TTL_SECONDS` (default 60) ao invés de hardcode:
```go
ttl := 60 * time.Second
if s := os.Getenv("CACHE_TTL_SECONDS"); s != "" {
    if n, err := strconv.Atoi(s); err == nil && n > 0 {
        ttl = time.Duration(n) * time.Second
    }
}
```

---

## §5 — FASE R4: Observabilidade e Runtime

### R4.1 — `ipLimiter` com TTL (evitar memory leak)
**Origem:** item 5.1 da revisão  
**Arquivo:** `shared/middleware/ratelimit.go` (após R2)

Substituir o mapa simples por uma estrutura com TTL usando `sync.Map` + goroutine de limpeza periódica:
```go
type ipEntry struct {
    limiter   *rate.Limiter
    lastSeen  time.Time
}

type ipLimiter struct {
    mu      sync.Mutex
    entries map[string]*ipEntry
}

func (il *ipLimiter) cleanup() {
    ticker := time.NewTicker(5 * time.Minute)
    for range ticker.C {
        il.mu.Lock()
        threshold := time.Now().Add(-10 * time.Minute)
        for ip, e := range il.entries {
            if e.lastSeen.Before(threshold) {
                delete(il.entries, ip)
            }
        }
        il.mu.Unlock()
    }
}
```
Inicializar `go il.cleanup()` em `RateLimit()`.

---

### R4.2 — `HealthWatcher` com semáforo para goroutines
**Origem:** item 5.2 da revisão  
**Arquivo:** `shard-router/infra/watcher.go`

Substituir `go w.check(ctx, cell)` ilimitado por um semáforo:
```go
type HealthWatcher struct {
    reg    *Registry
    client *http.Client
    sem    chan struct{}   // máximo de checks paralelos
}

func NewHealthWatcher(reg *Registry) *HealthWatcher {
    return &HealthWatcher{
        reg:    reg,
        client: &http.Client{Timeout: 2 * time.Second},
        sem:    make(chan struct{}, 10), // máximo 10 checks paralelos
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
                select {
                case w.sem <- struct{}{}:
                    go func(c CellEntry) {
                        defer func() { <-w.sem }()
                        w.check(ctx, c)
                    }(cell)
                default:
                    slog.Warn("health watcher: semaphore full, skipping check", "cell", cell.ID)
                }
            }
        }
    }
}
```

---

### R4.3 — `ReverseProxy` instanciado por célula (não por request)
**Origem:** item 5.3 da revisão  
**Arquivo:** `shard-router/api/handlers.go`

Criar um cache de `*httputil.ReverseProxy` por BaseURL:
```go
type Handler struct {
    reg      *infra.Registry
    breakers map[string]*resilience.Breaker
    proxies  map[string]*httputil.ReverseProxy // keyed por BaseURL
    proxyMu  sync.RWMutex
}

func (h *Handler) getProxy(cell *infra.CellEntry) *httputil.ReverseProxy {
    h.proxyMu.RLock()
    if p, ok := h.proxies[cell.BaseURL]; ok {
        h.proxyMu.RUnlock()
        return p
    }
    h.proxyMu.RUnlock()

    target, _ := url.Parse(cell.BaseURL)
    p := &httputil.ReverseProxy{
        Director: func(req *http.Request) {
            req.URL.Scheme = target.Scheme
            req.URL.Host = target.Host
            req.Host = target.Host
        },
    }
    h.proxyMu.Lock()
    h.proxies[cell.BaseURL] = p
    h.proxyMu.Unlock()
    return p
}
```
Remover o `sync.Mutex mu` do Handler que nunca é usado.

---

### R4.4 — Detecção de timeout Kafka por tipo de erro
**Origem:** item 8.1 da revisão  
**Arquivos:** todos os consumers Go

Substituir:
```go
if !strings.Contains(strings.ToLower(err.Error()), "timed out") {
```
Por:
```go
if kafkaErr, ok := err.(kafka.Error); !ok || kafkaErr.Code() != kafka.ErrTimedOut {
```

---

### R4.5 — `failCount` por mensagem, não global
**Origem:** item 8.2 da revisão  
**Arquivos:** `cell-pedidos/infra/messaging/kafka.go`, `cell-notificacoes/cmd/main.go` (após R3.1)

O `failCount` deve ser removido do loop externo. A lógica DLQ deve ser baseada em tentativas por mensagem individual, usando o header Kafka `x-retry-count` ou um mapa interno keyed por `msg.TopicPartition.Offset`:
```go
// Opção simples: usar offset como chave de contagem
failCounts := make(map[int64]int) // offset → tentativas

// Dentro do loop:
key := int64(msg.TopicPartition.Offset)
if err != nil {
    failCounts[key]++
    if failCounts[key] >= 3 {
        cs.prod.SendToDLQ(...)
        delete(failCounts, key)
        CommitMessage(msg)
    }
    // não commit — será reprocessado
    continue
}
delete(failCounts, key)
CommitMessage(msg)
```

---

### R4.6 — Prometheus scrape de células passivas
**Origem:** item 7.1 da revisão  
**Arquivo:** `infra/monitoring/prometheus.yml`

Adicionar targets passivos para cada PBC:
```yaml
- job_name: cell-pedidos
  static_configs:
    - targets:
        - 'cell-pedidos-s1-active:8000'
        - 'cell-pedidos-s1-passive:8000'
        - 'cell-pedidos-s2-active:8000'
        - 'cell-pedidos-s2-passive:8000'
        - 'cell-pedidos-s3-active:8000'
        - 'cell-pedidos-s3-passive:8000'
  relabel_configs:
    - source_labels: [__address__]
      regex: '.*-(active|passive):.*'
      target_label: role
      replacement: '$1'
```
Repetir para `cell-estoque` e `cell-notificacoes`.

---

### R4.7 — Montar regras SLO e Alert no docker-compose
**Origem:** item 7.4 da revisão  
**Arquivo:** `docker-compose.yml`

No serviço `prometheus`, adicionar volumes para os arquivos de regras:
```yaml
prometheus:
  volumes:
    - ./infra/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    - ./infra/monitoring/slo-rules.yml:/etc/prometheus/slo-rules.yml:ro
    - ./infra/monitoring/alert-rules.yml:/etc/prometheus/alert-rules.yml:ro
    - prometheus-data:/prometheus
```

---

### R4.8 — SLO de latência p99
**Origem:** item 7.2 da revisão  
**Arquivo:** `infra/monitoring/slo-rules.yml`

Adicionar recording rule de latência:
```yaml
- record: shard:latency_p99:rate5m
  expr: |
    histogram_quantile(0.99,
      sum(rate(shard_router_duration_seconds_bucket[5m])) by (shard, pbc, le)
    )
```

Adicionar alert correspondente em `alert-rules.yml`:
```yaml
- alert: LatencyP99High
  expr: shard:latency_p99:rate5m > 0.3
  for: 2m
  labels: { severity: warning }
  annotations:
    summary: "Latência p99 > 300ms: {{ $labels.shard }}/{{ $labels.pbc }} = {{ $value | humanizeDuration }}"
    runbook: "runbooks/circuit-breaker.md"
```

---

### R4.9 — `data-sync` com `healthz/ready`
**Origem:** item 7.5 da revisão  
**Arquivo:** `data-sync/cmd/main.go`

Adicionar endpoint `/healthz/ready` que verifica pelo menos uma conexão de pool ativa:
```go
mux.HandleFunc("/healthz/ready", func(w http.ResponseWriter, r *http.Request) {
    if app.Ready() {
        w.WriteHeader(200)
        w.Write([]byte(`{"status":"ok"}`))
    } else {
        w.WriteHeader(503)
        w.Write([]byte(`{"status":"no pools available"}`))
    }
})
```
Adicionar método `Ready() bool` no `Applier` que retorna `len(a.pools) > 0`.

---

### R4.10 — `data_sync_lag_seconds` implementado
**Origem:** item 6.3 da revisão  
**Arquivo:** `data-sync/infra/applier.go`

Adicionar métrica de lag baseada no campo `ts_ms` do envelope Debezium:
```go
var syncLag = promauto.NewGaugeVec(
    prometheus.GaugeOpts{Name: "data_sync_lag_seconds"},
    []string{"shard", "pbc"},
)

// Em apply(), após parse do debeziumMsg:
if tsMs, ok := dm.Payload["ts_ms"].(float64); ok {
    lag := float64(time.Now().UnixMilli())/1000 - tsMs/1000
    syncLag.WithLabelValues(shard, pbc).Set(lag)
}
```
O estrutura do `debeziumMsg` deve ser expandida para incluir `TsMs int64` no `Payload`.

---

## §6 — FASE R5: CI/CD e Infraestrutura

### R5.1 — Cache de módulos Go no CI
**Origem:** item 9.1 da revisão  
**Arquivo:** `.github/workflows/fitness-functions.yml`

Adicionar step de cache antes dos steps Go:
```yaml
- uses: actions/cache@v4
  with:
    path: |
      ~/.cache/go-build
      ~/go/pkg/mod
    key: ${{ runner.os }}-go-${{ hashFiles('**/go.sum') }}
    restore-keys: |
      ${{ runner.os }}-go-
```

---

### R5.2 — Fixar versões de ferramentas SAST no CI
**Origem:** item 9.2 da revisão  
**Arquivo:** `.github/workflows/fitness-functions.yml`

Substituir `go install ...@latest` por versões fixas:
```yaml
- name: govulncheck
  uses: golang/govulncheck-action@v1
  with:
    go-version: "1.22"
    go-package: ./...
    work-dir: ${{ matrix.cell }}

- name: gosec
  uses: securego/gosec@v2.21.0
  with:
    args: -severity medium ./...
```
Ou, como alternativa ao `gosec`, usar `golangci-lint` (ver R5.4).

---

### R5.3 — `go test ./...` no CI
**Origem:** item 9.3 da revisão  
**Arquivo:** `.github/workflows/fitness-functions.yml`

Adicionar job `unit-tests` após R6 (quando os testes existirem):
```yaml
unit-tests:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      cell: [shard-router, saga-hub, cell-pedidos, cell-estoque, cell-notificacoes, data-sync]
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version: "1.22" }
    - uses: actions/cache@v4
      with:
        path: ~/.cache/go-build
        key: ${{ runner.os }}-go-${{ hashFiles('**/go.sum') }}
    - run: cd ${{ matrix.cell }} && go test -race -count=1 ./...
```

---

### R5.4 — `golangci-lint` no CI
**Origem:** item 9.4 da revisão  
**Arquivo:** `.github/workflows/fitness-functions.yml`

```yaml
lint:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      cell: [shard-router, saga-hub, cell-pedidos, cell-estoque, cell-notificacoes, data-sync]
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version: "1.22" }
    - uses: golangci/golangci-lint-action@v6
      with:
        version: v1.59.0
        working-directory: ${{ matrix.cell }}
        args: --timeout=5m
```

Criar `.golangci.yml` na raiz com linters relevantes:
```yaml
linters:
  enable:
    - errcheck
    - staticcheck
    - unused
    - gosimple
    - govet
    - ineffassign
    - typecheck
```

---

### R5.5 — Tag de imagem versionada nos k8s manifests
**Origem:** item 10.1 da revisão  
**Arquivos:** todos os `k8s/shards/*/deployment.yaml` e `k8s/core/*.yaml`

Substituir `image: poc-eci/<component>:latest` por `image: poc-eci/<component>:${VERSION}` usando Kustomize ou Helm para injeção de tag. Para o PoC, documentar no `Makefile` o padrão de tag:
```makefile
VERSION ?= $(shell git describe --tags --always --dirty)
.PHONY: build-images
build-images:
    docker build -t poc-eci/cell-pedidos:$(VERSION) ./cell-pedidos
    # ... outros componentes
```

---

### R5.6 — `data-sync` com Deployment k8s
**Origem:** item 10.5 da revisão  
**Criar:** `k8s/core/data-sync.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-sync
  namespace: poc-eci
spec:
  replicas: 1
  selector:
    matchLabels: { app: data-sync }
  template:
    metadata:
      labels: { app: data-sync }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9191"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: data-sync
          image: poc-eci/data-sync:latest
          ports: [{ containerPort: 9191 }]
          env:
            - { name: KAFKA_BROKERS, value: "kafka:29092" }
            # PASSIVE_DSN_* via secretKeyRef
          livenessProbe:
            httpGet: { path: /healthz/live, port: 9191 }
            initialDelaySeconds: 15
          readinessProbe:
            httpGet: { path: /healthz/ready, port: 9191 }
            initialDelaySeconds: 20
```

---

### R5.7 — `PodDisruptionBudget` para células ativas
**Origem:** item 10.3 da revisão

Para cada shard × PBC ativo, criar um PDB que garanta pelo menos 1 pod disponível durante manutenção. Criar `k8s/shards/shard-1/cell-pedidos-active/pdb.yaml`:
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cell-pedidos-s1-active-pdb
  namespace: poc-eci
spec:
  minAvailable: 1
  selector:
    matchLabels: { app: cell-pedidos, shard: shard-1, role: active }
```
Repetir para os 9 deployments ativos (3 shards × 3 PBCs).

---

### R5.8 — `NetworkPolicy` para isolar PBCs em k8s
**Origem:** item 10.4 da revisão  
**Criar:** `k8s/base/network-policy.yaml`

```yaml
# cell-pedidos só recebe tráfego do shard-router e do Prometheus
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: cell-pedidos-ingress
  namespace: poc-eci
spec:
  podSelector:
    matchLabels: { pbc: pedidos }
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: { matchLabels: { app: shard-router } }
        - podSelector: { matchLabels: { app: prometheus } }
```
Repetir para `estoque` e `notificacoes`. Isso enforça L1 na camada de rede.

---

### R5.9 — JWT com log de aviso em modo dev
**Origem:** item 4.3 da revisão  
**Arquivo:** `shared/infra/auth/jwt.go` (após R2)

```go
func Middleware() gin.HandlerFunc {
    jwksURL := os.Getenv("JWKS_URL")
    if jwksURL == "" {
        slog.Warn("JWKS_URL not set — JWT validation DISABLED (dev mode only)")
        return func(c *gin.Context) { c.Next() }
    }
    // ... resto da implementação
}
```

---

### R5.10 — `reiniciar_celula` com allowlist
**Origem:** item 4.4 da revisão  
**Arquivo:** `agent-mcp/main.py`

```python
ALLOWED_CONTAINERS = {
    f"poc-eci-cell-{pbc}-s{shard}-{role}-1"
    for pbc in ["pedidos", "estoque", "notif"]
    for shard in [1, 2, 3]
    for role in ["active", "passive"]
}

async def executar_shard_tool(name: str, inputs: dict) -> str:
    if name == "reiniciar_celula":
        container = inputs["container_name"]
        if container not in ALLOWED_CONTAINERS:
            return json.dumps({"error": f"container '{container}' não permitido"})
        result = subprocess.run(...)
```

---

### R5.11 — `subprocess.run` → `asyncio.create_subprocess_exec`
**Origem:** item 11.1 da revisão  
**Arquivo:** `agent-mcp/main.py`

```python
elif name == "reiniciar_celula":
    container = inputs["container_name"]
    if container not in ALLOWED_CONTAINERS:
        return json.dumps({"error": f"container não permitido: {container}"})
    proc = await asyncio.create_subprocess_exec(
        "docker", "restart", container,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    return json.dumps({
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
        "returncode": proc.returncode,
    })
```

---

### R5.12 — Modelo Claude via env var no agent-mcp
**Origem:** item 11.2 da revisão  
**Arquivo:** `agent-mcp/main.py`

```python
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

# Em executar_agente():
response = await client_ai.messages.create(
    model=ANTHROPIC_MODEL,
    # ...
)
```

---

### R5.13 — `monitor_log` como `deque` com tipo anotado
**Origem:** item 11.4 da revisão  
**Arquivo:** `agent-mcp/main.py`

```python
from collections import deque
from typing import Any

monitor_log: deque[dict[str, Any]] = deque(maxlen=100)

# Remover:
# if len(monitor_log) > 100:
#     monitor_log.pop(0)
# A deque gerencia o limite automaticamente.
```

---

### R5.14 — Kafka em modo KRaft (sem ZooKeeper)
**Origem:** item 10.2 da revisão  
**Arquivo:** `docker-compose.yml`

Substituir `zookeeper` + `kafka` (Confluent com ZooKeeper) por imagem KRaft:
```yaml
kafka:
  image: confluentinc/cp-kafka:7.6.0
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"   # ver R5.15
    CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"      # gerado com kafka-storage random-uuid
```
Remover completamente o serviço `zookeeper`.

---

### R5.15 — Criação explícita de tópicos Kafka
**Origem:** item 8.3 da revisão  
**Criar:** `infra/kafka/create-topics.sh`

```bash
#!/bin/bash
KAFKA_BROKER=${KAFKA_BROKER:-kafka:29092}
create() {
  kafka-topics --bootstrap-server $KAFKA_BROKER \
    --create --if-not-exists \
    --topic "$1" --partitions "${2:-3}" --replication-factor 1 \
    --config retention.ms=86400000
}

# Commands
create commands.pedidos.criar 3
create commands.pedidos.cancelar 3
create commands.estoque.reservar 3
create commands.estoque.libertar 3
create commands.notificacoes.enviar 3
# Replies
create replies.pedidos.criado 3
create replies.pedidos.cancelado 3
create replies.estoque.reservado 3
create replies.estoque.insuficiente 3
create replies.estoque.liberado 3
create replies.notificacoes.enviada 3
# Events
create events.pedidos.confirmado 3
create events.pedidos.cancelado 3
create events.pedidos.falhou 3
create events.notificacoes.enviada 3
# Audit / DLQ
create audit.events 1
create dlq.pedidos.commands.pedidos.criar 1
create dlq.estoque.commands.estoque.reservar 1
create dlq.notificacoes.commands.notificacoes.enviar 1
# CDC
for shard in shard-1 shard-2 shard-3; do
  for table in pedidos.pedidos estoque.produtos notificacoes.notificacoes; do
    create "cdc.$shard.$table" 1
  done
done
```

Adicionar ao `docker-compose.yml` um serviço `kafka-init` que executa este script após Kafka estar healthy:
```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.6.0
  networks: [poc-eci]
  depends_on:
    kafka:
      condition: service_healthy
  entrypoint: ["/bin/bash", "/scripts/create-topics.sh"]
  volumes:
    - ./infra/kafka/create-topics.sh:/scripts/create-topics.sh:ro
  restart: on-failure
```

---

## §7 — FASE R6: Testes

### R6.1 — Testes unitários de domínio
**Origem:** item 12.1 da revisão

Criar os seguintes arquivos de teste:

**`cell-pedidos/domain/pedido_test.go`:**
```go
package domain_test

// Casos a cobrir:
// TestNewPedido_Valido — pedido com itens válidos é criado com status PENDENTE
// TestNewPedido_SemItens — retorna erro
// TestNewPedido_QuantidadeZero — retorna erro
// TestNewPedido_PrecoNegativo — retorna erro
// TestPedido_ValorTotal — soma correta de itens
// TestPedido_Confirmar — PENDENTE → CONFIRMADO
// TestPedido_Confirmar_NaoPendente — retorna erro se já CONFIRMADO
// TestPedido_Cancelar — PENDENTE → CANCELADO
// TestPedido_Cancelar_Confirmado — retorna erro
```

**`cell-estoque/domain/estoque_test.go`:**
```go
// TestProduto_Reservar_Suficiente — retorna true, decrementa quantidade
// TestProduto_Reservar_Insuficiente — retorna false, não altera quantidade
// TestProduto_Liberar — incrementa quantidade corretamente
// TestProduto_Repor — incrementa e atualiza timestamp
```

**`shard-router/domain/routing_test.go`:**
```go
// TestRoute_Determinismo — mesma key sempre → mesmo shard
// TestRoute_Distribuicao — 300 keys distintas distribuem entre os 3 shards
// TestRoute_Formato — resultado sempre "shard-1", "shard-2" ou "shard-3"
```

**`saga-hub/orchestrator/pedido_test.go`:**
```go
// TestHandleReply_PedidoCriado_Success — avança para StepReservarEstoque
// TestHandleReply_PedidoCriado_Failure — chama failSaga
// TestHandleReply_EstoqueReply_Failure — inicia compensação
// TestHandleReply_NotificacaoReply_Success — saga → COMPLETED
// TestHandleReply_PedidoCancelado — saga → FAILED
// (usar mocks de store e producer via interfaces)
```

**`shared/resilience/retry_test.go`:**
```go
// TestRetry_SuccedeNaPrimeira — zero retries adicionais
// TestRetry_SuccedeNaTerceira — tenta 3x, sucede na 3a
// TestRetry_EsgotaTentativas — retorna último erro após maxAttempts
// TestRetry_RespeitaCtxCancelado — para quando ctx é cancelado
```

**`shared/resilience/bulkhead_test.go`:**
```go
// TestBulkhead_AceitaDentroCapacidade
// TestBulkhead_RejeitaAcimaCapacidade
// TestBulkhead_LiberaSlotAposExecucao
```

---

### R6.2 — Corrigir `BOUNDARY_RULES` no FF1
**Origem:** item 12.2 da revisão  
**Arquivo:** `fitness-functions/run_all.py`

Remover `BOUNDARY_RULES` (verifica strings como `"estoque123"` que nunca existem). Manter apenas `FORBIDDEN_IMPORTS` que verifica os imports reais. Simplificar `check_boundary`:

```python
def check_boundary(cell: str) -> list[str]:
    cell_dir = ROOT / cell
    violations = []
    for go_file in cell_dir.rglob("*.go"):
        content = go_file.read_text(errors="ignore")
        for imp in FORBIDDEN_IMPORTS.get(cell, []):
            if f'"github.com/ranselmo/poc-eci/{imp}' in content:
                violations.append(
                    f"  [{cell}] {go_file.relative_to(ROOT)}: import proibido '{imp}'"
                )
    return violations
```

---

### R6.3 — FF2 Contract Test com JWT
**Origem:** item 12.3 da revisão  
**Arquivo:** `fitness-functions/run_all.py`

O contrato de `POST /pedidos/` deve detectar se autenticação está ativa e enviar token válido (ou pular o teste com mensagem clara):

```python
JWT_TOKEN = os.getenv("FF2_JWT_TOKEN", "")

# No contrato POST /pedidos/:
{
    "cell": "pedidos", "method": "POST", "path": "/pedidos/",
    "name": "POST /pedidos cria com campos obrigatórios",
    "headers": {
        "X-Client-ID": "ff2-test-cliente",
        **({"Authorization": f"Bearer {JWT_TOKEN}"} if JWT_TOKEN else {}),
    },
    "expected_status": 201 if JWT_TOKEN or not os.getenv("JWKS_URL") else 401,
    # ...
}
```

---

## §8 — Critérios Globais de Aceitação

Após a execução de todas as fases, os seguintes critérios devem ser satisfeitos:

### Critérios de Build
```bash
# Todos os componentes Go compilam sem erro
for c in shared contract shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  (cd "$c" && go build ./...) && echo "PASS: $c" || echo "FAIL: $c"
done

# Python sem erros de import
cd agent-mcp && python -c "import main, anomaly.detector, scaling.predictor" && echo "PASS: agent-mcp"
```

### Critérios de Teste
```bash
# Todos os testes unitários passam com -race
for c in shared contract shard-router saga-hub cell-pedidos cell-estoque cell-notificacoes; do
  (cd "$c" && go test -race -count=1 ./...) && echo "PASS: $c tests" || echo "FAIL: $c tests"
done
```

### Critérios de Segurança
```bash
# govulncheck sem vulnerabilidades conhecidas
for c in shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  (cd "$c" && govulncheck ./...) && echo "PASS: $c vuln" || echo "FAIL: $c vuln"
done

# Sem credenciais hardcoded nos fontes Go
! grep -rn "123@" --include="*.go" . && echo "PASS: no hardcoded creds" || echo "FAIL: found hardcoded creds"
```

### Critérios de Arquitetura (L1–L5)
```bash
# L1: PBCs não importam uns aos outros
./check.sh   # deve passar os checks estáticos

# L4: célula passiva bloqueia escrita
# (requer --full com stack up)
./check.sh --full
```

### Critérios de Observabilidade
```bash
# SLO e Alert rules válidas
docker run --rm --entrypoint promtool \
  -v "$(pwd)/infra/monitoring:/conf" \
  prom/prometheus:v2.48.0 check rules /conf/slo-rules.yml

docker run --rm --entrypoint promtool \
  -v "$(pwd)/infra/monitoring:/conf" \
  prom/prometheus:v2.48.0 check rules /conf/alert-rules.yml

# data_sync_lag_seconds existe na métrica exportada
# (requer stack up: curl http://localhost:9191/metrics | grep data_sync_lag_seconds)
```

### Critérios das Fitness Functions
```bash
# FF1: boundary isolation
# FF2: contract tests (com JWT_TOKEN configurado)
# FF3: p99 < 300ms
# FF4: blast radius isolado
python3 fitness-functions/run_all.py
# Saída esperada: "SUITE APROVADA — Arquitetura em conformidade"
```

---

## §9 — Ordem de Execução Recomendada

```
R1 (bugs críticos)
  └─ R1.1 transação estoque
  └─ R1.2 compensação libera estoque
  └─ R1.3 cancelar pedido propaga erro
  └─ R1.4 save saga não silencia erro
  └─ R1.5 json.Unmarshal verificado
  └─ R1.6 uuid.Parse verificado
  └─ R1.7 SQL injection allowlist
  └─ R1.8 credenciais removidas dos defaults
  └─ R1.9 kafka at-least-once (commit manual)
      │
      ▼
R2 (módulo shared)
  └─ R2.1 criar shared/go.mod e estrutura
  └─ R2.2 telemetry/otel.go
  └─ R2.3 middleware/passive.go
  └─ R2.4 monitoring/finops.go unificado
  └─ R2.5 remover var _ = time.Now
  └─ [atualizar go.mod de cada componente com replace]
  └─ [remover packages duplicados de cada cell]
      │
      ▼
R3 (consistência arquitetural)
  └─ R3.1 refatorar cell-notificacoes
  └─ R3.2 extrair handlers cell-estoque
  └─ R3.3 mover estoqueProcessor
  └─ R3.4 CorrelationID separado do SagaID
  └─ R3.5 failSaga notifica
  └─ R3.6 PublishBusinessEvent com breaker
  └─ R3.7 TTL de cache via env var
      │
      ▼
R4 (observabilidade e runtime)
  └─ R4.1 ipLimiter com TTL
  └─ R4.2 HealthWatcher com semáforo
  └─ R4.3 ReverseProxy por célula
  └─ R4.4 kafka timeout por tipo de erro
  └─ R4.5 failCount por mensagem
  └─ R4.6 prometheus scrape passivas
  └─ R4.7 montar slo/alert rules no compose
  └─ R4.8 SLO de latência p99
  └─ R4.9 data-sync healthz/ready
  └─ R4.10 data_sync_lag_seconds implementado
      │
      ▼
R5 (CI/CD e infra)
  └─ R5.1 cache de módulos no CI
  └─ R5.2 versões fixas de ferramentas SAST
  └─ R5.3 go test no CI (após R6)
  └─ R5.4 golangci-lint no CI
  └─ R5.5 tag versionada nas imagens k8s
  └─ R5.6 data-sync Deployment k8s
  └─ R5.7 PodDisruptionBudget
  └─ R5.8 NetworkPolicy
  └─ R5.9 JWT log de aviso em dev
  └─ R5.10 allowlist reiniciar_celula
  └─ R5.11 asyncio subprocess
  └─ R5.12 modelo via env var
  └─ R5.13 monitor_log como deque
  └─ R5.14 Kafka KRaft (sem ZooKeeper)
  └─ R5.15 criação explícita de tópicos
      │
      ▼
R6 (testes)
  └─ R6.1 testes unitários de domínio
  └─ R6.2 corrigir BOUNDARY_RULES no FF1
  └─ R6.3 FF2 com JWT
```

---

## §10 — Notas de Implementação

### Sobre o módulo `shared` e as leis arquiteturais
O módulo `shared` contém **exclusivamente** infraestrutura técnica: circuit breaker, rate limiter, telemetria, cache, JWT. Ele não contém:
- Lógica de domínio de nenhum PBC
- Comunicação entre PBCs
- Referências a Kafka topics específicos de negócio

A Lei L1 permanece intacta: PBCs não importam uns aos outros. `shared` é uma biblioteca de infra, não um PBC.

### Sobre R1.9 (commit manual Kafka) e DLQ
Após R1.9, uma mensagem que vai para DLQ **deve** ter seu offset commitado — caso contrário, ao reiniciar o consumer, a mensagem seria reprocessada, enviada para DLQ novamente, e entraria em loop infinito. O fluxo correto é:
1. Processar mensagem → falha
2. `failCount[offset]++`
3. Se `failCount >= 3`: SendToDLQ → CommitMessage → delete failCounts[offset]
4. Se `failCount < 3`: não commit (reprocessar na próxima tentativa)

### Sobre R3.4 (CorrelationID separado)
Esta mudança requer migração de schema: `ALTER TABLE sagas DROP CONSTRAINT sagas_correlation_id_key; ALTER TABLE sagas ADD CONSTRAINT sagas_correlation_id_key UNIQUE (correlation_id);` já existe — o constraint está correto. O que muda é que `correlation_id` passa a ser diferente de `id`, tornando o índice semanticamente útil.

### Sobre R5.14 (Kafka KRaft)
O `CLUSTER_ID` deve ser gerado uma única vez:
```bash
docker run --rm confluentinc/cp-kafka:7.6.0 kafka-storage random-uuid
```
E fixado no `docker-compose.yml`. Não regenerar a cada `docker compose up` — o volume do Kafka ficaria inconsistente.

---

*Fim do documento REFINE.md — 47 itens, 6 fases, critérios de aceitação definidos.*
