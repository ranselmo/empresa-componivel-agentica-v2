# Runbook: ActiveCellDown / BothCellsDown

## Trigger: `shard_router_cell_health{role="active"} == 0`

## Diagnóstico (execute em ordem)

1. `curl -sf http://localhost:8080/router/cells | jq .`
2. `docker ps --filter "name=cell-" --format "{{.Names}}\t{{.Status}}"`
3. `docker logs cell-pedidos-s1-active --tail 50`
4. `curl -sf http://localhost:8080/healthz/ready`

## Ações corretivas

### Cenário A: Container crashou (OOM ou panic)

- `docker restart cell-<pbc>-s<N>-active`
- Verificar memory limits: `docker stats cell-<pbc>-s<N>-active`
- Se OOM: aumentar `mem_limit` no docker-compose.yml e reiniciar

### Cenário B: DB não responsivo

- `docker logs db-<pbc>-s<N>-active --tail 20`
- `docker restart db-<pbc>-s<N>-active`
- O shard-router deve ter feito failover automático para passive — verificar com `curl /router/cells`

### Cenário C: Kafka desconectado

- `docker logs kafka --tail 20`
- `docker restart kafka`
- Aguardar healthcheck: `docker inspect kafka | jq '.[0].State.Health'`

## Verificação de resolução

```bash
curl -sf http://localhost:8080/router/cells | jq '.[] | select(.role=="active") | .healthy'
# Deve retornar true para todas as células ativas
```

## Escalação

Após 5min sem resolução: acionar on-call via PagerDuty — runbook de escalação em `runbooks/escalation.md`
