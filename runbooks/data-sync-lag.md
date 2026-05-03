# Runbook: DataSyncLagHigh

## Trigger: `data_sync_lag_seconds > 5`

## Diagnóstico (execute em ordem)

1. `docker logs data-sync --tail 50 | grep -E "error|lag"`
2. `curl -sf http://localhost:9191/metrics | grep data_sync`
3. `docker logs kafka --tail 20 | grep -E "error|partition"`
4. Verificar tópicos CDC: `docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group data-sync --describe`

## Ações corretivas

### Cenário A: data-sync sobrecarregado (lag crescente)

- Verificar CPU/mem: `docker stats data-sync`
- Se alto: `docker-compose up -d --scale data-sync=2` (adicionar réplica)
- Garantir que consumer groups são únicos por réplica

### Cenário B: Partição Kafka com lag acumulado

- `docker exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --group data-sync --reset-offsets --to-latest --all-topics --execute`
- **ATENÇÃO**: isso descarta mensagens não processadas — só usar se DB passivo puder ser reconstruído via snapshot

### Cenário C: DB passivo indisponível

- `docker restart db-<pbc>-s<N>-passive`
- `docker restart data-sync`

## Verificação de resolução

```bash
curl -sf http://localhost:9191/metrics | grep data_sync_lag_seconds
# Deve ser < 1.0
```

## Escalação

Após 15min de lag > 30s: acionar time de dados — potencial perda de consistência nos shards passivos.
