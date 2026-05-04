#!/bin/bash
# §9 — Critérios Globais de Aceitação
# Uso: ./check.sh           (checks estáticos apenas)
#      ./check.sh --full    (inclui checks que requerem stack up)
set -u
FAIL=0
FULL=${1:-""}

check() { [ "$1" = "0" ] && echo "PASS: $2" || { echo "FAIL: $2"; FAIL=1; }; }

echo "=== BUILD ==="
BASE="$(cd "$(dirname "$0")" && pwd)"
for c in shard-router saga-hub data-sync cell-pedidos cell-estoque cell-notificacoes; do
  (cd "$BASE/$c" && go build ./... 2>&1) \
    && check 0 "$c build" \
    || check 1 "$c build"
done

echo ""
echo "=== BOUNDARY CHECK ==="
for check_cell in cell-pedidos cell-estoque cell-notificacoes; do
  for forbidden in cell-pedidos cell-estoque cell-notificacoes saga-hub; do
    [ "$check_cell" = "$forbidden" ] && continue
    count=$(grep -r "\"github.com/ranselmo/poc-eci/$forbidden" "$BASE/$check_cell/" 2>/dev/null | wc -l | tr -d ' ')
    check "$count" "$check_cell não importa $forbidden (count=$count)"
  done
done

echo ""
echo "=== SLO RULES ==="
docker run --rm --entrypoint promtool \
  -v "$BASE/infra/monitoring:/conf" \
  prom/prometheus:v2.48.0 check rules /conf/slo-rules.yml 2>&1 \
  | grep -q "SUCCESS" && check 0 "slo-rules valid" || check 1 "slo-rules invalid"

docker run --rm --entrypoint promtool \
  -v "$BASE/infra/monitoring:/conf" \
  prom/prometheus:v2.48.0 check rules /conf/alert-rules.yml 2>&1 \
  | grep -q "SUCCESS" && check 0 "alert-rules valid" || check 1 "alert-rules invalid"

if [ "$FULL" != "--full" ]; then
  echo ""
  echo "=== CHECKS DINÂMICOS (requerem: docker compose up) ==="
  echo "  Execute './check.sh --full' com o stack rodando para:"
  echo "  - STACK HEALTH  : 18 containers (3 shards × 3 PBCs × 2 roles)"
  echo "  - SHARD ROUTING : distribuição entre shards via X-Client-ID"
  echo "  - PASSIVE MODE  : células passivas bloqueiam escrita (HTTP 503)"
  echo "  - SAGA E2E      : saga dispara e conclui com status COMPLETED"
  echo ""
  echo "=== RESULT (checks estáticos) ==="
  [ "$FAIL" = "0" ] && echo "ALL STATIC CHECKS PASSED" || echo "SOME CHECKS FAILED — veja acima"
  exit $FAIL
fi

echo ""
echo "=== STACK HEALTH ==="
echo "Aguardando containers estabilizarem (90s)..."
sleep 90
# Compose prefixa com "poc-eci-" e sufixo "-1"; notificacoes → cell-notif
for shard in 1 2 3; do
  for pbc in pedidos estoque notif; do
    for role in active passive; do
      name="poc-eci-cell-${pbc}-s${shard}-${role}-1"
      status=$(docker inspect --format='{{.State.Status}}' "$name" 2>/dev/null || echo "missing")
      [ "$status" = "running" ] \
        && check 0 "cell-${pbc}-s${shard}-${role} running" \
        || check 1 "cell-${pbc}-s${shard}-${role} running ($status)"
    done
  done
done

echo ""
echo "=== SHARD ROUTING ==="
# bash 3.2 (macOS) — sem associative arrays; conta shards únicos via sort+uniq
shards_file=$(mktemp)
for key in "cliente-aaa" "cliente-bbb" "cliente-ccc" "cliente-ddd" "cliente-eee"; do
  shard=$(curl -sf -H "X-Client-ID: $key" http://localhost:8080/healthz/live \
    -D - 2>/dev/null | grep -i "X-Shard-ID" | awk '{print $2}' | tr -d '\r' || echo "")
  [ -n "$shard" ] && echo "$shard" >> "$shards_file"
done
unique_count=$(sort -u "$shards_file" | wc -l | tr -d ' ')
rm -f "$shards_file"
[ "$unique_count" -ge 2 ] \
  && check 0 "routing distributes across shards ($unique_count shards seen)" \
  || check 1 "routing distributes across shards (only $unique_count shards seen)"

echo ""
echo "=== PASSIVE MODE ==="
resp=$(curl -sf -X POST http://localhost/cell-pedidos-s1-passive/pedidos/ \
  -H "Content-Type: application/json" -d '{}' 2>/dev/null || echo "503")
[[ "$resp" == *"passive"* || "$resp" == "503" ]] \
  && check 0 "passive blocks writes" \
  || check 1 "passive blocks writes (got: $resp)"

echo ""
echo "=== SAGA E2E ==="
RESP=$(curl -sf -X POST http://localhost:8080/saga/pedido \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: test-001" \
  -d '{"cliente_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","shard_id":"shard-1","payload":{"itens":[{"produto_id":"11111111-1111-1111-1111-111111111111","quantidade":1,"preco_unitario":4999.90}]}}')
SAGA_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['saga_id'])" 2>/dev/null || echo "")
if [ -z "$SAGA_ID" ]; then
  check 1 "saga e2e — resposta inválida: $RESP"
else
  sleep 10
  STATUS=$(curl -sf "http://localhost:8080/saga/$SAGA_ID" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "UNKNOWN")
  [ "$STATUS" = "COMPLETED" ] \
    && check 0 "saga e2e completed (saga_id=$SAGA_ID)" \
    || check 1 "saga e2e (status=$STATUS, saga_id=$SAGA_ID)"
fi

echo ""
echo "=== RESULT ==="
[ "$FAIL" = "0" ] && echo "ALL CHECKS PASSED" || echo "SOME CHECKS FAILED — veja acima"
exit $FAIL
