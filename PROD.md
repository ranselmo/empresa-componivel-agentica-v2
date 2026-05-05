# PROD.md — Checklist de Produção

Prompt para Claude Code implementar os itens que faltam para produção.
Leia CLAUDE.md antes de iniciar. Execute em ordem P0 → P1 → P2.

---

## Contexto do projeto

- 6 módulos Go (`github.com/ranselmo/poc-eci/<comp>`) + `shared/` + Python `agent-mcp/`
- 3 shards × 3 PBCs × 2 roles = 18 cells; 27 PostgreSQL DBs; Kafka KRaft
- k8s manifests em `k8s/`; docker-compose local em `docker-compose.yml`
- Todos os secrets estão hoje em plaintext — nenhum está criptografado

---

## P0 — Bloqueadores (implementar antes de qualquer deploy)

### P0.1 — Debezium connectors automáticos

**Problema:** O serviço Debezium sobe mas nenhum connector é registrado. O `data-sync` nunca recebe CDC.

**Criar** `infra/debezium/register-connectors.sh`:
```bash
#!/bin/bash
DEBEZIUM_URL=${DEBEZIUM_URL:-http://debezium:8083}
register() {
  curl -sf -X POST "$DEBEZIUM_URL/connectors" \
    -H "Content-Type: application/json" -d "$1"
}
# Repetir para cada shard × PBC (9 connectors total)
# Padrão: nome=cdc-<shard>-<pbc>, database.hostname=db-<pbc>-<shard>-active
# table.include.list=public.<tabela_principal_do_pbc>
# transforms: route com RegexRouter para topic cdc.<shard>.<pbc>.<tabela>
```

**Adicionar** ao `docker-compose.yml` serviço `debezium-init`:
```yaml
debezium-init:
  image: curlimages/curl:8.6.0
  networks: [poc-eci]
  depends_on:
    debezium: { condition: service_healthy }
  entrypoint: ["/bin/sh", "/scripts/register-connectors.sh"]
  volumes:
    - ./infra/debezium/register-connectors.sh:/scripts/register-connectors.sh:ro
  restart: on-failure
```

**Adicionar** healthcheck ao serviço `debezium` existente:
```yaml
healthcheck:
  test: ["CMD", "curl", "-sf", "http://localhost:8083/connectors"]
  interval: 15s; timeout: 10s; retries: 10; start_period: 60s
```

**Criar** `k8s/core/debezium-init-job.yaml` (Job k8s que executa o script pós-deploy).

---

### P0.2 — Database migrations (golang-migrate)

**Problema:** Schema criado por seed scripts ad-hoc sem versionamento ou rollback.

**Instalar** `golang-migrate` como dependência de dev (não de runtime):
```bash
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

**Criar** estrutura de migrations para cada PBC:
```
cell-pedidos/migrations/
  000001_init.up.sql    — CREATE TABLE pedidos, itens (schema atual)
  000001_init.down.sql  — DROP TABLE pedidos
cell-estoque/migrations/
  000001_init.up.sql    — CREATE TABLE produtos
cell-notificacoes/migrations/
  000001_init.up.sql    — CREATE TABLE notificacoes
saga-hub/migrations/
  000001_init.up.sql    — CREATE TABLE sagas
```

**Modificar** cada `cmd/main.go` para rodar migrations na inicialização:
```go
import "github.com/golang-migrate/migrate/v4"
import _ "github.com/golang-migrate/migrate/v4/database/postgres"
import _ "github.com/golang-migrate/migrate/v4/source/file"

m, err := migrate.New("file://migrations", os.Getenv("DATABASE_URL"))
if err != nil { slog.Error("migrate new", "err", err); os.Exit(1) }
if err := m.Up(); err != nil && err != migrate.ErrNoChange {
    slog.Error("migrate up", "err", err); os.Exit(1)
}
```

**Adicionar** `golang-migrate` ao `go.mod` de cada componente que tem DB.

---

### P0.3 — Alertmanager configurado

**Problema:** Serviço `alertmanager` sobe sem config — alertas disparam mas não chegam a ninguém.

**Criar** `infra/monitoring/alertmanager.yml`:
```yaml
global:
  resolve_timeout: 5m

route:
  receiver: default
  group_by: [alertname, shard, pbc]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - match: { severity: critical }
      receiver: critical
      repeat_interval: 1h

receivers:
  - name: default
    # Substituir pelo canal real: slack_configs, email_configs, webhook_configs
    webhook_configs:
      - url: ${ALERT_WEBHOOK_URL}
        send_resolved: true
  - name: critical
    webhook_configs:
      - url: ${ALERT_WEBHOOK_URL}
        send_resolved: true
```

**Montar** no `docker-compose.yml` (serviço `alertmanager` já existe):
```yaml
volumes:
  - ./infra/monitoring/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
command: ["--config.file=/etc/alertmanager/alertmanager.yml"]
environment:
  ALERT_WEBHOOK_URL: ${ALERT_WEBHOOK_URL:-http://localhost/webhook-placeholder}
```

**Adicionar** ao `infra/monitoring/prometheus.yml`:
```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

---

### P0.4 — Secrets management

**Problema:** Senhas e API keys em plaintext no docker-compose e nos manifests.

**Para docker-compose (dev/staging):**

Criar `.env.example`:
```
POSTGRES_PASSWORD_PEDIDOS=
POSTGRES_PASSWORD_ESTOQUE=
POSTGRES_PASSWORD_NOTIFICACOES=
POSTGRES_PASSWORD_SAGA=
ANTHROPIC_API_KEY=
ALERT_WEBHOOK_URL=
```

Substituir no `docker-compose.yml` todos os valores hardcoded por `${VAR}`.
Adicionar `.env` ao `.gitignore` (já deve estar).

**Para k8s (produção):**

Criar `k8s/base/secrets-template.yaml` com instruções (não commitar valores):
```yaml
# NÃO COMMITAR COM VALORES REAIS
# Criar via: kubectl create secret generic <nome> --from-literal=key=value -n poc-eci
# Ou via External Secrets Operator apontando para Vault/AWS SSM
apiVersion: v1
kind: Secret
metadata: { name: cell-pedidos-s1-active, namespace: poc-eci }
type: Opaque
stringData:
  database-url: "postgres://pedidos:<SENHA>@db-pedidos-s1-active:5432/pedidos?sslmode=require"
```

Adicionar `k8s/base/secrets-template.yaml` ao `.gitignore`.
Criar `k8s/base/external-secrets.yaml` (opcional, para External Secrets Operator):
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata: { name: cell-pedidos-s1-active, namespace: poc-eci }
spec:
  refreshInterval: 1h
  secretStoreRef: { name: vault-backend, kind: ClusterSecretStore }
  target: { name: cell-pedidos-s1-active }
  data:
    - secretKey: database-url
      remoteRef: { key: poc-eci/cell-pedidos-s1-active, property: database-url }
```

---

## P1 — Crítico operacional

### P1.1 — Log aggregation (Loki + Promtail)

**Adicionar** ao `docker-compose.yml`:
```yaml
loki:
  image: grafana/loki:2.9.0
  networks: [poc-eci]
  ports: ["3100:3100"]
  command: -config.file=/etc/loki/local-config.yaml

promtail:
  image: grafana/promtail:2.9.0
  networks: [poc-eci]
  volumes:
    - /var/lib/docker/containers:/var/lib/docker/containers:ro
    - /var/run/docker.sock:/var/run/docker.sock
    - ./infra/monitoring/promtail.yml:/etc/promtail/config.yml:ro
```

**Criar** `infra/monitoring/promtail.yml`:
```yaml
server: { http_listen_port: 9080 }
positions: { filename: /tmp/positions.yaml }
clients:
  - url: http://loki:3100/loki/api/v1/push
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
        filters:
          - name: label
            values: ["com.docker.compose.project=poc-eci"]
    relabel_configs:
      - source_labels: [__meta_docker_container_name]
        target_label: container
```

**Adicionar** Loki como datasource no Grafana (`infra/monitoring/grafana-datasources.yml`):
```yaml
- name: Loki
  type: loki
  url: http://loki:3100
```

---

### P1.2 — Grafana dashboards reais

**Criar** `infra/monitoring/dashboards/eci-overview.json` com os seguintes painéis (usar Grafana JSON model):

1. **SAGA throughput**: `rate(saga_completed_total[5m])` por `outcome` (success/compensated/failed)
2. **SAGA latência p99**: `histogram_quantile(0.99, rate(saga_duration_seconds_bucket[5m]))`
3. **Disponibilidade por shard/PBC**: `shard:availability:rate5m * 100`
4. **Error budget burn**: `shard:error_budget_burn` — alerta visual quando > 1
5. **CDC lag**: `data_sync_lag_seconds` por shard/pbc
6. **Circuit breakers**: `circuit_breaker_state` — highlight quando estado=2 (open)
7. **Request rate**: `rate(shard_router_requests_total[1m])` por shard
8. **Células saudáveis**: gauge com `up` por job

**Montar** no Grafana via `infra/monitoring/grafana-dashboards.yml` (provisioning já existe, só falta o arquivo JSON).

---

### P1.3 — Backup PostgreSQL

**Criar** `infra/backup/pg-backup.sh`:
```bash
#!/bin/bash
# Executar como CronJob k8s ou cron local
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=${BACKUP_DIR:-/backups}
for db in pedidos-s1-active pedidos-s2-active pedidos-s3-active \
           estoque-s1-active estoque-s2-active estoque-s3-active \
           notif-s1-active notif-s2-active notif-s3-active saga; do
  pg_dump "${DATABASE_URL_PREFIX}-${db}" | gzip > "$BACKUP_DIR/${db}_${TIMESTAMP}.sql.gz"
done
# Manter últimos 7 dias
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +7 -delete
```

**Criar** `k8s/core/pg-backup-cronjob.yaml`:
```yaml
apiVersion: batch/v1
kind: CronJob
metadata: { name: pg-backup, namespace: poc-eci }
spec:
  schedule: "0 2 * * *"   # 02:00 UTC diariamente
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: pg-backup
              image: postgres:16-alpine
              command: ["/bin/sh", "/scripts/pg-backup.sh"]
              envFrom:
                - secretRef: { name: pg-backup-credentials }
              volumeMounts:
                - name: scripts
                  mountPath: /scripts
                - name: backup-storage
                  mountPath: /backups
          volumes:
            - name: scripts
              configMap: { name: pg-backup-scripts }
            - name: backup-storage
              persistentVolumeClaim: { claimName: backup-pvc }
          restartPolicy: OnFailure
```

---

### P1.4 — Ingress k8s com TLS

**Criar** `k8s/base/ingress.yaml`:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: poc-eci-ingress
  namespace: poc-eci
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/proxy-body-size: "1m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "30"
spec:
  ingressClassName: nginx
  tls:
    - hosts: [api.poc-eci.example.com]
      secretName: poc-eci-tls
  rules:
    - host: api.poc-eci.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: shard-router, port: { number: 8080 } }
```

**Criar** `k8s/base/cert-issuer.yaml` (cert-manager ClusterIssuer para Let's Encrypt).

---

### P1.5 — Kafka multi-broker (RF≥3)

**Atualizar** `docker-compose.yml` para ambiente de staging com 3 brokers KRaft:
```yaml
# Adicionar kafka-2 e kafka-3 com KAFKA_NODE_ID: 2 e 3
# KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093
```

**Atualizar** `infra/kafka/create-topics.sh`: mudar `--replication-factor 1` para `--replication-factor 3` nos tópicos de commands/replies/events. Manter RF=1 apenas para `__consumer_offsets` e topics de sistema.

**Atualizar** `docker-compose.yml` — adicionar `volumes` ao serviço `kafka` para persistência:
```yaml
volumes:
  - kafka-data:/var/lib/kafka/data
# Em volumes globais: kafka-data:
```

---

## P2 — Importante, pode evoluir

### P2.1 — Idempotência em POST /pedidos/

**Problema:** Retry do cliente cria pedidos duplicados.

**Implementar** em `cell-pedidos/api/handlers.go`:
1. Ler header `Idempotency-Key` (UUID obrigatório em POST)
2. Verificar no Redis: `GET idempotency:<key>` — se existe, retornar resposta cacheada (201)
3. Processar normalmente; ao salvar, fazer `SET idempotency:<key> <response_json> EX 86400`
4. Retornar 400 se header ausente em POST

Chave Redis: `idempotency:<X-Client-ID>:<Idempotency-Key>` (escopo por cliente).

---

### P2.2 — Paginação cursor-based

**Implementar** em `cell-pedidos/infra/db/query_store.go` e `cell-estoque`:
```go
// GET /pedidos/?after=<uuid>&limit=<int>
func (s *QueryStore) Listar(ctx context.Context, after uuid.UUID, limit int) ([]Pedido, error) {
    // SELECT * FROM pedidos WHERE id > $1 ORDER BY id LIMIT $2
}
```

Limit padrão: 20, máximo: 100. Resposta inclui `next_cursor` se há mais registros.

---

### P2.3 — Testes de integração

**Criar** para cada componente com I/O real um arquivo `integration_test.go` com build tag:
```go
//go:build integration
```

Executar com: `go test -tags integration -race ./...`

**Componentes prioritários:**
- `cell-pedidos/infra/db/store_integration_test.go` — testa Salvar/BuscarPorID contra PostgreSQL real (usar `testcontainers-go`)
- `cell-estoque/infra/db/store_integration_test.go` — testa `ReservarItens` com transação concorrente (2 goroutines)
- `saga-hub/orchestrator/pedido_integration_test.go` — SAGA completa contra Kafka real

**Dependência:**
```bash
go get github.com/testcontainers/testcontainers-go
```

**Adicionar** job no CI (`.github/workflows/fitness-functions.yml`):
```yaml
integration-test:
  runs-on: ubuntu-latest
  services:
    postgres:
      image: postgres:16-alpine
      env: { POSTGRES_PASSWORD: test }
    kafka: ...
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-go@v5
      with: { go-version: "1.22" }
    - run: cd cell-pedidos && go test -tags integration -race -count=1 ./...
```

---

### P2.4 — Kustomize overlays staging/prod

**Estrutura:**
```
k8s/
  base/           — manifests atuais (sem valores de ambiente)
  overlays/
    staging/
      kustomization.yaml   — patches: replicas=1, resources menores
    production/
      kustomization.yaml   — patches: replicas=3, HPA ativo, TLS
```

**`k8s/overlays/production/kustomization.yaml`:**
```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
patches:
  - path: replica-count.yaml   # replicas: 3 para shard-router e saga-hub
  - path: resource-limits.yaml # aumentar limits para prod
images:
  - name: poc-eci/cell-pedidos
    newTag: ${VERSION}         # injetado pelo CI via kustomize edit set image
```

---

### P2.5 — API versioning

**Adicionar** prefixo `/v1` em todos os routers Gin:

Em `cell-pedidos/cmd/main.go` (e demais células):
```go
v1 := r.Group("/v1")
// Mover todas as rotas para v1
// Manter rotas sem prefixo redirecionando para /v1 (301) por 1 release de deprecação
```

Em `shard-router`: propagar prefixo no strip de path antes de fazer proxy.

---

### P2.6 — GDPR — exclusão de dados por cliente

**Criar** endpoint em `cell-pedidos/api/handlers.go`:
```
DELETE /v1/clientes/:cliente_id/dados
```

Implementação:
1. Verificar autorização (role `admin` no JWT)
2. Deletar todos os registros do `cliente_id` na tabela `pedidos`
3. Publicar evento `audit.events` com `action=GDPR_DELETE`
4. Retornar 204

Repetir para `cell-estoque` (dados de reservas) e `cell-notificacoes`.

**Nota:** o shard do cliente é determinístico (`sha256(cliente_id) % 3`), então o delete precisa ir apenas para um shard.

---

## Critérios de aceitação globais

```bash
# P0 verificável localmente
docker compose up -d
# Debezium connectors registrados
curl -sf http://localhost:8083/connectors | python3 -c "import sys,json; c=json.load(sys.stdin); print(f'{len(c)} connectors: {c}')"
# Esperar: 9 connectors

# Migrations rodaram
docker compose exec db-pedidos-s1-active psql -U pedidos -d pedidos \
  -c "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 3;"

# Alertmanager configurado
curl -sf http://localhost:9093/api/v2/status | python3 -c \
  "import sys,json; s=json.load(sys.stdin); print(s.get('config',{}).get('original','')[:200])"

# P1 verificável localmente
# Loki recebendo logs
curl -sf "http://localhost:3100/loki/api/v1/query?query={container=~\"poc-eci.*\"}&limit=5"

# P2 verificável
# Idempotência
KEY=$(python3 -c "import uuid; print(uuid.uuid4())")
curl -sf -X POST http://localhost:8080/v1/pedidos/ \
  -H "Idempotency-Key: $KEY" -H "X-Client-ID: test-client" \
  -H "Content-Type: application/json" \
  -d '{"cliente_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","itens":[{"produto_id":"11111111-1111-1111-1111-111111111111","quantidade":1,"preco_unitario":10}]}'
# Segunda chamada com mesmo KEY deve retornar idêntico sem criar novo pedido
```
