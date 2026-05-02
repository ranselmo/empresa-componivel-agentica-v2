# PoC — Empresa Componível Inteligente v2 (Go + Kafka)

Implementação prática do framework de 4 pilares descrito no artigo  
**"Como Construir uma Empresa Componível Inteligente"** — Rafael Sá Anselmo.

Esta é a **versão 2** da PoC, com stack tecnológica alternativa:  
células escritas em **Go**, comunicação via **Apache Kafka**.

---

## Stack Tecnológica

Esta seção explica cada tecnologia escolhida, sua função específica na solução e por que foi selecionada para esta PoC.

### Linguagem das células: Go 1.22

**O que é:** Go é uma linguagem compilada, estaticamente tipada, desenvolvida pelo Google, com foco em performance, simplicidade e concorrência nativa.

**Por que na PoC:** As células de negócio (`cell-pedidos`, `cell-estoque`, `cell-notificacoes`) são implementadas em Go por três razões alinhadas ao framework. Primeira: o binário compilado resulta em imagens Docker de apenas ~15MB contra ~300MB do Python, o que acelera o deploy — diretamente ligado ao princípio de deployment de baixo risco do Pilar 1. Segunda: o modelo de concorrência de Go (goroutines + channels) permite que cada célula rode o consumer Kafka e o servidor HTTP no mesmo processo sem bloqueio, sem a complexidade do `asyncio`. Terceira: a tipagem estática funciona como uma fitness function em tempo de compilação — o build falha se um evento de domínio tiver campo faltando ou tipo errado, antes mesmo de rodar qualquer teste.

**Onde aparece:** `cell-pedidos/`, `cell-estoque/`, `cell-notificacoes/` — todo código `.go`.

---

### Event Bus: Apache Kafka 7.6 (Confluent)

**O que é:** Kafka é uma plataforma distribuída de streaming de eventos, projetada para alta vazão, durabilidade e replay de mensagens. Diferente de filas tradicionais, os eventos ficam persistidos no log por um período configurável, e múltiplos consumidores podem ler de forma independente.

**Por que na PoC:** O Kafka substitui o RabbitMQ da v1 e reforça dois princípios do framework de forma mais explícita. O princípio de isolamento de célula fica mais nítido porque cada célula tem seu próprio consumer group, lê em seu próprio ritmo e não bloqueia outras células se ficar lenta. O princípio de resiliência fica mais robusto porque, se uma célula cai e volta, ela recomeça do offset onde parou — nenhum evento é perdido, ao contrário de filas que podem descartar mensagens expiradas. Os tópicos (`dominio.pedido.criado`, `dominio.estoque.reservado`, etc.) formam o contrato de evento de domínio entre células, tornando o SAGA explícito e auditável.

**Onde aparece:** `docker-compose.yml` (serviços `zookeeper` e `kafka`), `infra/messaging/kafka.go` em cada célula, Kafka UI em `localhost:8090`.

---

### Driver Kafka em Go: confluent-kafka-go v2

**O que é:** Biblioteca oficial da Confluent para Go, wrapper do `librdkafka` — a implementação C de alto desempenho do protocolo Kafka.

**Por que na PoC:** É a opção de menor latência e maior compatibilidade com o ecossistema Confluent. O `CGO_ENABLED=1` no Dockerfile é necessário por isso — o driver linka com `librdkafka` via cgo. A troca de performance vale: o produtor com `acks=all` garante que nenhum evento de domínio é perdido em falha de broker.

**Onde aparece:** `go.mod` de cada célula, `infra/messaging/kafka.go`.

---

### Framework HTTP das células: Gin v1.10

**O que é:** Gin é o framework web mais utilizado em Go. Oferece roteamento de alta performance baseado em radix tree, middlewares componíveis e binding/validação de JSON via struct tags.

**Por que na PoC:** As APIs públicas das células (os contratos de PBC) são expostas via Gin. A integração com OpenTelemetry via `otelgin.Middleware` adiciona rastreamento automático a cada request sem modificar os handlers — isso implementa o conceito de observabilidade embutida da Engenharia de Plataforma (Pilar 2) sem poluir a lógica de negócio.

**Onde aparece:** `api/handlers.go` (cell-pedidos), `cmd/main.go` (cell-estoque, cell-notificacoes).

---

### Banco de dados das células: PostgreSQL 16 + pgx v5

**O que é:** PostgreSQL é o banco relacional open-source mais avançado disponível. `pgx` é o driver Go nativo de mais alta performance para PostgreSQL, com suporte a pool de conexões, prepared statements e tipos PostgreSQL avançados (JSONB, UUID, TIMESTAMPTZ).

**Por que na PoC:** Cada célula tem seu próprio banco PostgreSQL isolado — banco de pedidos, banco de estoque, banco de notificações — em portas diferentes (5433, 5434, 5435). Isso implementa fisicamente o boundary context do Pilar 1: não existe forma de uma célula acessar os dados de outra além de chamar sua API pública. O campo `itens` dos pedidos usa JSONB para armazenar a lista de itens sem precisar de tabela separada, mantendo o schema simples para a PoC.

**Onde aparece:** `infra/db/store.go` em cada célula, serviços `db-pedidos`, `db-estoque`, `db-notificacoes` no `docker-compose.yml`.

---

### API Gateway: Traefik v3

**O que é:** Traefik é um reverse proxy e API gateway moderno, com discovery automático via Docker labels. Ao contrário do Nginx, não requer reload de configuração — detecta novos containers automaticamente.

**Por que na PoC:** Implementa o entry point único para todas as células (Pilar 2 — Engenharia de Plataforma). O roteamento é feito por prefixo de path: `/pedidos` vai para `cell-pedidos`, `/estoque` vai para `cell-estoque`. As células não precisam saber umas das outras — só o gateway sabe o mapa de rotas. Traefik também expõe métricas Prometheus nativamente, sem configuração extra.

**Onde aparece:** Serviço `traefik` no `docker-compose.yml`, labels `traefik.*` em cada célula, dashboard em `localhost:8080`.

---

### Observabilidade — Distributed Tracing: OpenTelemetry + Jaeger

**O que é:** OpenTelemetry (OTel) é o padrão aberto de observabilidade, agnóstico de vendor, que unifica traces, métricas e logs. Jaeger é um backend de tracing distribuído open-source, originalmente desenvolvido pelo Uber.

**Por que na PoC:** Quando um `POST /pedidos` entra no sistema, ele percorre múltiplas células via eventos Kafka. Sem tracing distribuído, é impossível saber onde um pedido travou ou qual célula está lenta. O `otelgin.Middleware` no Gin propaga automaticamente o `trace_id` entre serviços via HTTP headers, e o exporter OTLP envia os spans para o Jaeger. Isso implementa a camada de observabilidade do Building Block descrito no Pilar 1 — toda célula vem com observabilidade embutida.

**Onde aparece:** `setupOTel()` em cada `cmd/main.go`, exporter OTLP para `jaeger:4317`, UI em `localhost:16686`.

---

### Observabilidade — Métricas: Prometheus + Grafana

**O que é:** Prometheus é o sistema de coleta de métricas pull-based mais utilizado no ecossistema cloud-native. Grafana é a plataforma de dashboards que consome as métricas do Prometheus.

**Por que na PoC:** Cada célula Go expõe um endpoint `/metrics` com contadores de requests HTTP, latências por percentil e uso de goroutines — automaticamente, via `prometheus/client_golang`. O Prometheus scrapa esses endpoints a cada 15 segundos. As fitness functions de latência (FF3) consultam o Prometheus via API para validar se o p99 dos endpoints está dentro do limite de 200ms. Isso fecha o ciclo do Pilar 3: a fitness function usa métricas reais de produção, não simuladas.

**Onde aparece:** Serviços `prometheus` e `grafana` no `docker-compose.yml`, endpoint `/metrics` em cada célula, configuração em `infra/monitoring/prometheus.yml`.

---

### Agente de IA: Python 3.12 + Anthropic SDK + FastAPI

**O que é:** O agente MCP é o único componente em Python na v2. Usa o SDK oficial da Anthropic para Python, que implementa o loop de function calling (tools) para agentes autônomos. FastAPI expõe a interface HTTP do agente.

**Por que Python aqui, e não Go:** O SDK da Anthropic com suporte completo a streaming, function calling e multi-turn conversations tem implementação de referência em Python. Reescrever em Go adicionaria complexidade sem benefício para a PoC. Isso ilustra um ponto importante do framework: células são políglotas — cada uma usa a linguagem mais adequada ao seu propósito. O agente acessa as células Go via HTTP, completamente agnóstico à linguagem delas.

**Por que na PoC:** Implementa o Pilar 4 completo. O agente expõe as capacidades das células Go como ferramentas MCP (`criar_pedido`, `consultar_estoque`, `verificar_saude_sistema`, etc.) e as oferece ao Claude como contexto. O loop de monitoramento autônomo chama `verificar_saude_sistema()` a cada 60 segundos e raciocina sobre o resultado sem intervenção humana — demonstrando a "Célula Inteligente" do framework.

**Onde aparece:** `agent-mcp/main.py`, serviço `agent-mcp` no `docker-compose.yml`, UI em `localhost:9000/docs`.

---

### Fitness Functions: Python 3.12 + httpx

**O que é:** Scripts Python que executam verificações automatizadas da arquitetura. `httpx` é uma biblioteca HTTP assíncrona para Python, com interface similar ao `requests`.

**Por que Python para as FFs:** As fitness functions são ferramentas de CI/CD, não de produção. Python permite escrever verificações expressivas em poucas linhas, sem necessidade de compilar. A FF1 analisa o código-fonte Go com manipulação de strings simples. A FF2/FF3 disparam requests HTTP contra as células Go. A FF4 executa subprocessos `docker stop/start`. Python é a melhor ferramenta para esse tipo de script de automação.

**Onde aparece:** `fitness-functions/run_all.py`, `.github/workflows/fitness-functions.yml`.

---

### Kafka UI: Provectus Kafka UI

**O que é:** Interface web open-source para inspecionar clusters Kafka — tópicos, partições, consumer groups, offsets e mensagens individuais.

**Por que na PoC:** Torna os eventos de domínio visíveis durante a demo. É possível abrir o Kafka UI em `localhost:8090`, navegar até o tópico `dominio.pedido.criado`, clicar em uma mensagem e ver exatamente o JSON do evento `PedidoCriado` que a célula de Pedidos publicou. Isso torna o fluxo SAGA observável e didático — essencial para validar o Pilar 1 visualmente.

**Onde aparece:** Serviço `kafka-ui` no `docker-compose.yml`, UI em `localhost:8090`.

---

### Tabela resumo da stack

| Tecnologia | Versão | Papel na solução | Pilar |
|---|---|---|---|
| Go | 1.22 | Linguagem das células de negócio | 1 |
| Apache Kafka | 7.6 (Confluent) | Event Bus para SAGA inter-célula | 1 |
| confluent-kafka-go | v2.3 | Driver Kafka para Go (alta performance) | 1 |
| PostgreSQL | 16 | Banco isolado por célula (boundary físico) | 1 |
| pgx | v5.5 | Driver Go nativo para PostgreSQL | 1 |
| Gin | v1.10 | Framework HTTP das células Go | 2 |
| Traefik | v3.0 | API Gateway — entry point único | 2 |
| OpenTelemetry | v1.24 | Tracing distribuído entre células | 2 |
| Jaeger | 1.52 | Backend de tracing distribuído | 2 |
| Prometheus | 2.48 | Coleta de métricas de cada célula | 2 |
| Grafana | 10.2 | Dashboards de métricas | 2 |
| Python + httpx | 3.12 | Fitness functions no CI/CD | 3 |
| GitHub Actions | — | Pipeline CI/CD com FFs integradas | 3 |
| Anthropic Claude API | claude-sonnet-4-6 | LLM do agente autônomo | 4 |
| Anthropic SDK Python | 0.28 | Function calling / MCP tools | 4 |
| FastAPI | 0.111 | Interface HTTP do agente | 4 |
| Kafka UI | latest | Inspeção visual de eventos Kafka | — |
| Docker Compose | v2 | Orquestração local da PoC | — |

---

## Diferenças em relação à v1 (Python + RabbitMQ)

| Aspecto | v1 (Python + RabbitMQ) | v2 (Go + Kafka) |
|---|---|---|
| Linguagem das células | Python 3.12 + FastAPI | Go 1.22 + Gin |
| Event Bus | RabbitMQ 3.12 | Apache Kafka 7.6 |
| Tamanho da imagem Docker | ~300MB por célula | ~15MB por célula |
| Modelo de concorrência | asyncio (cooperative) | goroutines (preemptive) |
| Replay de eventos | Não (fila descarta) | Sim (log retido 24h) |
| Visibilidade de eventos | RabbitMQ Management UI | Kafka UI |
| Consumer groups | Subscriptions por fila | Consumer groups por tópico |
| Tipagem de eventos | Pydantic (runtime) | Structs Go (compile-time) |
| Agente de IA | Python (SDK nativo) | Python (SDK nativo) |

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
# Criar pedido (Notebook Pro — 10 unidades)
curl -X POST http://localhost/pedidos/ \
  -H "Content-Type: application/json" \
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

### Visualizar eventos Kafka

1. Acesse http://localhost:8090
2. Clique em **Topics**
3. Navegue pelos tópicos `dominio.pedido.*` e `dominio.estoque.*`
4. Clique em **Messages** para ver os eventos publicados em tempo real

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
poc-eci-go/
├── cell-pedidos/
│   ├── domain/
│   │   ├── pedido.go       # Entidade + regras de negócio (Go puro)
│   │   └── events.go       # Contratos de eventos (structs tipadas)
│   ├── infra/
│   │   ├── db/store.go     # PostgreSQL com pgx
│   │   └── messaging/kafka.go  # Producer + Consumer Kafka
│   ├── api/handlers.go     # Gin HTTP handlers
│   ├── cmd/main.go         # Wire-up + OTel + Prometheus
│   ├── go.mod
│   └── Dockerfile          # Multi-stage build Go
│
├── cell-estoque/           # Mesma estrutura
├── cell-notificacoes/      # cmd/main.go único (célula mais simples)
│
├── agent-mcp/
│   ├── main.py             # Python + Anthropic SDK + FastAPI
│   ├── requirements.txt
│   └── Dockerfile
│
├── fitness-functions/
│   └── run_all.py          # FF1 (Go static), FF2, FF3, FF4
│
├── infra/monitoring/
│   ├── prometheus.yml
│   └── grafana-datasources.yml
│
├── .github/workflows/
│   └── fitness-functions.yml
│
├── docker-compose.yml
├── .env.example
└── README.md
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

# Rebuild de uma célula Go após mudança
docker compose build cell-pedidos && docker compose restart cell-pedidos

# Limpar tudo
docker compose down -v
```

---

*PoC v2 — Go + Kafka — baseada no artigo "Como Construir uma Empresa Componível Inteligente" — Rafael Sá Anselmo (2025)*
