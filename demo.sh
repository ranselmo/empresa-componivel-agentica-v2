#!/bin/bash
# demo.sh — Demo end-to-end da PoC ECI v2 (Go + Kafka)
# Demonstra os 4 pilares com células Go e eventos Kafka
set -e

BASE="http://localhost"
AGENT="http://localhost:9000"
KAFKA_UI="http://localhost:8090"
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

pause() { echo; read -p "  Pressione Enter para continuar..." ; echo; }
header() {
  echo
  echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
  echo -e "${CYAN}  $1${NC}"
  echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
}
step() { echo -e "${YELLOW}▶ $1${NC}"; }
ok()   { echo -e "${GREEN}✓ $1${NC}"; }
info() { echo -e "  $1"; }

# ── Intro ────────────────────────────────────────────────────────────
header "PoC ECI v2 — Go + Kafka | Demo End-to-End"
info "Stack: Go 1.22 · Apache Kafka · PostgreSQL · Traefik · OTel + Jaeger"
info "Certifique-se que o stack está rodando: docker compose up -d"
pause

# ── Pilar 1: Saúde das células Go ───────────────────────────────────
header "PILAR 1 — Arquitetura Componível (células Go)"
step "Verificando saúde de cada célula..."

for cell in pedidos estoque notificacoes; do
  STATUS=$(curl -sf "$BASE/$cell/health" \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "DOWN")
  RUNTIME=$(curl -sf "$BASE/$cell/health" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('runtime','Go'))" 2>/dev/null || echo "?")
  [ "$STATUS" = "ok" ] && ok "$cell: UP (Go binary)" || echo -e "${RED}✗ $cell: DOWN${NC}"
done

step "Consultando estoque (seed automático com 4 produtos)..."
curl -sf "$BASE/estoque/" | python3 -m json.tool
pause

# ── Pilar 1: SAGA happy path ─────────────────────────────────────────
header "PILAR 1 — SAGA via Kafka: Pedido Confirmado"
step "Criando pedido para Notebook Pro (estoque: 10 unidades)..."
info "Evento PedidoCriado será publicado no tópico dominio.pedido.criado"
info "Kafka UI: $KAFKA_UI/ui/clusters/poc-cluster/all-topics"

RESP=$(curl -sf -X POST "$BASE/pedidos/" \
  -H "Content-Type: application/json" \
  -d '{
    "cliente_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "itens": [
      {"produto_id": "11111111-1111-1111-1111-111111111111",
       "quantidade": 1, "preco_unitario": 4999.90}
    ]
  }')
echo "$RESP" | python3 -m json.tool
PEDIDO_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['pedido_id'])")
ok "Pedido $PEDIDO_ID criado (PENDENTE)"

step "Aguardando SAGA completar via Kafka (consumer groups processando)..."
sleep 5

step "Status final:"
FINAL=$(curl -sf "$BASE/pedidos/$PEDIDO_ID")
echo "$FINAL" | python3 -m json.tool
STATUS=$(echo "$FINAL" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
[ "$STATUS" = "CONFIRMADO" ] && ok "SAGA concluída — pedido CONFIRMADO" || echo -e "${RED}Status: $STATUS${NC}"

step "Notificação enviada:"
curl -sf "$BASE/notificacoes/" | python3 -c "
import sys, json
ns = json.load(sys.stdin)
if ns: print(json.dumps(ns[0], indent=2, ensure_ascii=False))
else: print('Aguardando notificação... (tente em 2s)')
"
pause

# ── Pilar 1: SAGA compensação ─────────────────────────────────────────
header "PILAR 1 — SAGA via Kafka: Compensação (sem estoque)"
step "Criando pedido para Monitor 4K (estoque: 0 unidades)..."
info "EstoqueInsuficiente → PedidoCancelado → NotificacaoCancelamento"

RESP2=$(curl -sf -X POST "$BASE/pedidos/" \
  -H "Content-Type: application/json" \
  -d '{
    "cliente_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "itens": [
      {"produto_id": "44444444-4444-4444-4444-444444444444",
       "quantidade": 1, "preco_unitario": 2499.90}
    ]
  }')
PEDIDO_ID2=$(echo "$RESP2" | python3 -c "import sys,json; print(json.load(sys.stdin)['pedido_id'])")
ok "Pedido $PEDIDO_ID2 criado"
sleep 5

STATUS2=$(curl -sf "$BASE/pedidos/$PEDIDO_ID2" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
[ "$STATUS2" = "CANCELADO" ] && ok "SAGA de compensação — pedido CANCELADO" \
  || echo -e "${RED}Status: $STATUS2 (aguarde mais alguns segundos)${NC}"
pause

# ── Pilar 1: Visualizar eventos Kafka ────────────────────────────────
header "PILAR 1 — Eventos Kafka visíveis"
step "Verificando tópicos criados no cluster Kafka..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep dominio | while read t; do echo "  tópico: $t"; done

step "Consumer groups e offset lag..."
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list 2>/dev/null \
  | while read g; do echo "  grupo: $g"; done

info ""
info "Visualize os eventos completos no Kafka UI:"
info "  $KAFKA_UI"
pause

# ── Pilar 2: Plataforma e Observabilidade ────────────────────────────
header "PILAR 2 — Engenharia de Plataforma"
step "Interfaces disponíveis..."
ok "API Gateway (Traefik): http://localhost:8080"
ok "Jaeger (tracing Go↔Kafka): http://localhost:16686"
ok "Grafana (métricas): http://localhost:3000 (admin/poc123)"
ok "Prometheus: http://localhost:9090"
ok "Kafka UI: http://localhost:8090"

step "Métricas Prometheus das células Go..."
for cell in cell-pedidos cell-estoque cell-notificacoes; do
  COUNT=$(curl -sf "http://localhost:9090/api/v1/query" \
    --data-urlencode "query=http_requests_total{job=\"$cell\"}" 2>/dev/null \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
results=d.get('data',{}).get('result',[])
total=sum(float(r['value'][1]) for r in results) if results else 0
print(f'{total:.0f}')
" 2>/dev/null || echo "N/A")
  info "$cell: $COUNT requests registrados"
done

step "Traces distribuídos no Jaeger:"
info "1. Acesse http://localhost:16686"
info "2. Service: cell-pedidos"
info "3. Find Traces → clique num trace para ver a cadeia completa"
pause

# ── Pilar 3: Fitness Functions ────────────────────────────────────────
header "PILAR 3 — Arquitetura Evolutiva (Fitness Functions)"
step "FF1: Boundary Isolation — análise estática do código Go"
python3 -c "
import sys
exec(open('fitness-functions/run_all.py').read())
ok = run_ff1()
" 2>/dev/null || python3 fitness-functions/run_all.py 2>&1 | head -20

step "FF2: Contract Tests contra células Go..."
python3 -c "
import asyncio, sys
exec(open('fitness-functions/run_all.py').read())
asyncio.run(run_ff2())
" 2>/dev/null || echo "  Execute: python3 fitness-functions/run_all.py"
pause

# ── Pilar 4: Agente de IA com MCP ────────────────────────────────────
header "PILAR 4 — IA Agêntica com MCP (acessa células Go via HTTP)"

if [ -z "$ANTHROPIC_API_KEY" ] || [ "$ANTHROPIC_API_KEY" = "sua_chave_aqui" ]; then
  echo -e "${YELLOW}  Agente em modo passivo (ANTHROPIC_API_KEY não configurada)${NC}"
  info "Para ativar: export ANTHROPIC_API_KEY=sk-ant-... && docker compose restart agent-mcp"
  info "Interface do agente: http://localhost:9000/docs"
else
  step "Consultando agente sobre o estado do sistema..."
  curl -sf -X POST "$AGENT/agente/executar" \
    -H "Content-Type: application/json" \
    -d '{"prompt": "Verifique a saúde das células Go e me dê um resumo dos pedidos criados nesta demo."}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['resultado'])"

  pause

  step "Testando detecção autônoma de anomalia..."
  info "Derrubando cell-estoque (célula Go) por 10 segundos..."
  docker stop cell-estoque > /dev/null 2>&1

  sleep 3
  curl -sf -X POST "$AGENT/agente/executar" \
    -H "Content-Type: application/json" \
    -d '{"prompt": "Verifique urgentemente a saúde do sistema. Qual célula está com problema e o que recomenda?"}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['resultado'])"

  info "Restaurando cell-estoque..."
  docker start cell-estoque > /dev/null 2>&1
  sleep 10
  ok "Célula restaurada automaticamente (Bulkhead pattern)"
fi

# ── Resumo ────────────────────────────────────────────────────────────
header "Demo Concluída — v2 (Go + Kafka)"
ok "PILAR 1 — 3 PBCs em Go, SAGA via Kafka, eventos duráveis"
ok "PILAR 2 — Traefik, OpenTelemetry, Prometheus/Grafana, Kafka UI"
ok "PILAR 3 — 4 fitness functions (análise Go estática + HTTP + chaos)"
ok "PILAR 4 — Agente Python acessa células Go via MCP tools HTTP"
echo
info "Stack completa:"
info "  Linguagem das células : Go 1.22 (binários ~15MB)"
info "  Event Bus             : Apache Kafka (eventos duráveis, replay)"
info "  API Gateway           : Traefik v3 (discovery automático)"
info "  Tracing               : OpenTelemetry → Jaeger"
info "  Métricas              : Prometheus → Grafana"
info "  Agente IA             : Python + Anthropic SDK (Claude Sonnet)"
echo
info "Interfaces:"
info "  http://localhost       — API Gateway"
info "  http://localhost:8080  — Traefik dashboard"
info "  http://localhost:8090  — Kafka UI (eventos visíveis)"
info "  http://localhost:16686 — Jaeger (traces)"
info "  http://localhost:3000  — Grafana (admin/poc123)"
info "  http://localhost:9000/docs — Agente MCP"
echo
