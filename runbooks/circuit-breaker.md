# Runbook: CircuitBreakerOpen

## Trigger: `circuit_breaker_state == 2`

## Diagnóstico (execute em ordem)

1. `curl -sf http://localhost:9095/api/v1/query?query=circuit_breaker_state | jq '.data.result'`
2. Identificar componente e breaker do label: `component`, `breaker`
3. `docker logs <component> --tail 50 | grep -E "circuit|breaker|error"`
4. Verificar se dependência (DB/Kafka) está respondendo: `docker ps | grep <dependency>`

## Ações corretivas

### Cenário A: DB com alta latência ou fora do ar

- `docker restart db-<pbc>-s<N>-<role>`
- O circuit breaker fechará automaticamente após 30s com <= 3 requests de teste
- Monitorar: `curl http://localhost:9095/api/v1/query?query=circuit_breaker_state`

### Cenário B: Kafka indisponível ou partições orfãs

- `docker restart kafka`
- Aguardar `kafka healthcheck` passar
- O breaker tentará fechar automaticamente (MaxRequests=3, Timeout=30s)

### Cenário C: Cascata de erros (múltiplos breakers abertos)

- Verificar ordem de reinicialização: infra primeiro (Kafka, DBs), depois cells
- `docker-compose restart kafka zookeeper`
- Aguardar 60s e verificar: `curl http://localhost:9095/api/v1/query?query=circuit_breaker_state`

## Verificação de resolução

```bash
curl -sf "http://localhost:9095/api/v1/query?query=circuit_breaker_state%3D%3D2" | \
  jq '.data.result | length'
# Deve retornar 0 (nenhum breaker aberto)
```

## Escalação

Após 10min com breaker aberto sem recuperação automática: investigar root cause no DB/Kafka logs. Escalar para infra on-call se for problema de infraestrutura.
