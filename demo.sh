#!/usr/bin/env bash
# demo.sh — ECI v2 (Go + Kafka) — Demo End-to-End completo
# Executa os 4 pilares: Componível | Plataforma | Evolutiva | IA Agêntica
set -uo pipefail

# ── Endpoints ────────────────────────────────────────────────────────────────
ROUTER="http://localhost:8080"      # shard-router (entrada única — L3)
PROMETHEUS="http://localhost:9095"
GRAFANA="http://localhost:3000"
JAEGER="http://localhost:16686"
KAFKA_UI="http://localhost:8090"
AGENT="http://localhost:9000"

# ── Cores ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
RED='\033[0;31m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

header() {
  echo
  echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
  printf "${CYAN}  %-52s${NC}\n" "$1"
  echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
}
step()  { echo -e "${YELLOW}▶ $1${NC}"; }
ok()    { echo -e "  ${GREEN}✓${NC} $1"; }
err()   { echo -e "  ${RED}✗${NC} $1"; }
info()  { echo -e "  ${DIM}$1${NC}"; }
nl()    { echo; }

# Aguarda endpoint ficar acessível
wait_healthy() {
  local url="$1" label="$2" max="${3:-120}"
  printf "  Aguardando %-38s" "$label"
  for i in $(seq 1 "$max"); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "OK (${i}s)"
      return 0
    fi
    sleep 1
    [ $((i % 15)) -eq 0 ] && printf "%ds..." "$i"
  done
  echo "TIMEOUT"
  return 1
}

# Polling de status de uma SAGA (imprime pontinhos de progresso)
poll_saga() {
  local saga_id="$1" max="${2:-30}"
  local status=""
  for i in $(seq 1 "$max"); do
    status=$(curl -sf "$ROUTER/saga/$saga_id" \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
    if [ "$status" = "COMPLETED" ] || [ "$status" = "FAILED" ]; then
      echo "$status"
      return 0
    fi
    sleep 1
    printf "."
  done
  echo "${status:-TIMEOUT}"
}

# ── Verificação de pré-requisitos ─────────────────────────────────────────────
if ! docker info >/dev/null 2>&1; then
  err "Docker não está rodando. Inicie o Docker Desktop e tente novamente."
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  err "python3 não encontrado. Instale Python 3.9+."
  exit 1
fi

# Instala httpx se necessário (requerido pelas fitness functions)
if ! python3 -c "import httpx" 2>/dev/null; then
  info "Instalando httpx para fitness functions..."
  pip3 install httpx --break-system-packages -q 2>/dev/null \
    || pip3 install httpx --user -q 2>/dev/null || true
fi

# ── Banner ────────────────────────────────────────────────────────────────────
clear
echo -e "${CYAN}${BOLD}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║   PoC — Empresa Componível Inteligente (ECI v2)     ║"
echo "  ║   Go 1.22 · Kafka KRaft · 3 Shards · Agent MCP     ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"
info "Autor: Rafael Sá Anselmo"
info "Stack: Go 1.22 · Kafka KRaft · PostgreSQL · Redis · Prometheus · Jaeger · Claude"
info "v2.14.0 — REFINE.md fases R1–R6 completas (47 melhorias implementadas)"
nl

# ── START STACK ───────────────────────────────────────────────────────────────
header "SETUP — Iniciando Stack Completa"

step "Subindo containers com docker compose up -d --build..."
info "(primeira execução: ~3-5 min para build das imagens Go)"
nl
docker compose up -d --build 2>&1 | grep -E '(Building|built|Started|Healthy|Error|error)' | head -20 || true
nl

# Aguarda shard-router (só fica healthy quando Kafka + todos os DBs estão ok)
wait_healthy "$ROUTER/healthz/live" "shard-router:8080/healthz/live ..." 200 || {
  err "Shard-router não ficou saudável após 200s."
  docker compose ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null | head -25
  nl
  info "Para diagnóstico: docker compose logs shard-router"
  exit 1
}

# Aguarda data-sync estar pronto (pools passivos conectados)
wait_healthy "http://localhost:9191/healthz/ready" "data-sync:9191/healthz/ready ..." 60 || \
  info "data-sync não ficou pronto — CDC passivo pode estar indisponível"

# Aguarda pelo menos 9 células registradas como saudáveis
step "Aguardando health watcher registrar células ativas (até 60s)..."
healthy_cells=0
for i in $(seq 1 60); do
  healthy_cells=$(curl -sf "$ROUTER/router/cells" \
    | python3 -c "
import sys, json
cells = json.load(sys.stdin).get('cells', [])
print(sum(1 for c in cells if c['Healthy']))
" 2>/dev/null || echo "0")
  if [ "${healthy_cells:-0}" -ge 9 ]; then break; fi
  sleep 1
  [ $((i % 15)) -eq 0 ] && info "${healthy_cells:-0} células saudáveis até agora (aguardando 9)..."
done
ok "Stack pronta — ${healthy_cells:-0} células ativas"
nl

# ═══════════════════════════════════════════════════════════════════════════════
# PILAR 1 — ARQUITETURA COMPONÍVEL
# ═══════════════════════════════════════════════════════════════════════════════
header "PILAR 1 — Arquitetura Componível (Consistent Hashing + 3 Shards)"

# Topologia
step "Topologia registrada no shard-router:"
curl -sf "$ROUTER/router/cells" | python3 -c "
import sys, json
cells = sorted(json.load(sys.stdin).get('cells', []),
               key=lambda x: (x['ShardID'], x['PBC'], x['Role']))
total     = len(cells)
saudaveis = sum(1 for c in cells if c['Healthy'])
print(f'  {saudaveis}/{total} células  |  3 shards × 3 PBCs × 2 papéis (active/passive)')
print()
print(f'  {\"ID\":<38} {\"Shard\":<8} {\"PBC\":<15} {\"Papel\":<8} Status')
print(f'  {\"-\"*38} {\"-\"*8} {\"-\"*15} {\"-\"*8} ------')
for c in cells:
    mark = '✓ UP' if c['Healthy'] else '✗ DOWN'
    print(f'  {c[\"ID\"]:<38} {c[\"ShardID\"]:<8} {c[\"PBC\"]:<15} {c[\"Role\"]:<8} {mark}')
" 2>/dev/null
nl

# Catálogo
step "Catálogo de produtos (seed automático em cada cell-estoque):"
curl -sf "$ROUTER/estoque/" | python3 -c "
import sys, json
for p in json.load(sys.stdin):
    flag = '✓ OK         ' if p['quantidade_disponivel'] > 0 else '✗ SEM ESTOQUE'
    print(f'  {flag}  {p[\"nome\"]:<25}  qty={p[\"quantidade_disponivel\"]:3d}  R\$ {p[\"preco\"]}')
" 2>/dev/null
nl

# ── SAGA Happy Path ───────────────────────────────────────────────────────────
header "PILAR 1 — SAGA Orquestrada via Kafka (Happy Path)"
info "Fluxo: POST /saga/pedido → saga-hub → commands.pedidos.criar"
info "       → replies.pedidos.criado → commands.estoque.reservar"
info "       → replies.estoque.reservado → commands.notificacoes.enviar"
info "       → replies.notificacoes.enviada → status=COMPLETED"
nl

step "Iniciando SAGA: Notebook Pro (estoque=10) — espera COMPLETED..."
SAGA1=$(curl -sf -X POST "$ROUTER/saga/pedido" \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: cliente-alpha-001" \
  -d '{
    "cliente_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "shard_id":   "shard-1",
    "payload": {
      "cliente_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      "itens": [{"produto_id": "11111111-1111-1111-1111-111111111111",
                 "quantidade": 1, "preco_unitario": 4999.90}]
    }
  }' 2>/dev/null || echo '{}')

SAGA1_ID=$(echo "$SAGA1" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('saga_id',''))" 2>/dev/null || echo "")

if [ -z "$SAGA1_ID" ]; then
  err "Falha ao iniciar SAGA — resposta: $SAGA1"
else
  ok "SAGA iniciada — id=$SAGA1_ID"
  printf "  Aguardando Kafka roundtrip (pedido→estoque→notificacao)"
  STATUS1=$(poll_saga "$SAGA1_ID" 120)
  echo
  [ "$STATUS1" = "COMPLETED" ] \
    && ok "SAGA status=COMPLETED ✓" \
    || err "SAGA status=$STATUS1 (esperava COMPLETED)"

  # Detalhes da SAGA concluída
  curl -sf "$ROUTER/saga/$SAGA1_ID" | python3 -c "
import sys, json
s = json.load(sys.stdin)
print(f'  saga_id={s[\"saga_id\"]}')
print(f'  correlation_id={s.get(\"correlation_id\", \"(campo na saga interna)\")}')
print(f'  step={s[\"current_step\"]}  shard={s[\"shard_id\"]}')
" 2>/dev/null
fi
nl

# ── SAGA Compensação ──────────────────────────────────────────────────────────
header "PILAR 1 — SAGA via Kafka (Compensação Completa por Estoque Insuficiente)"
info "Fluxo: commands.estoque.reservar → EstoqueInsuficiente → saga-hub"
info "       → commands.pedidos.cancelar → replies.pedidos.cancelado"
info "       → commands.estoque.liberar  → replies.estoque.liberado   (R1.2)"
info "       → commands.notificacoes.enviar → status=FAILED"
nl

step "Iniciando SAGA: Monitor 4K (estoque=0) — espera FAILED..."
SAGA2=$(curl -sf -X POST "$ROUTER/saga/pedido" \
  -H "Content-Type: application/json" \
  -H "X-Client-ID: cliente-beta-002" \
  -d '{
    "cliente_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "shard_id":   "shard-1",
    "payload": {
      "cliente_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
      "itens": [{"produto_id": "44444444-4444-4444-4444-444444444444",
                 "quantidade": 1, "preco_unitario": 2499.90}]
    }
  }' 2>/dev/null || echo '{}')

SAGA2_ID=$(echo "$SAGA2" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('saga_id',''))" 2>/dev/null || echo "")

if [ -z "$SAGA2_ID" ]; then
  err "Falha ao iniciar SAGA — resposta: $SAGA2"
else
  ok "SAGA iniciada — id=$SAGA2_ID"
  printf "  Aguardando compensação Kafka (cancelar → liberar_estoque → notificar)"
  STATUS2=$(poll_saga "$SAGA2_ID" 120)
  echo
  [ "$STATUS2" = "FAILED" ] \
    && ok "Compensação completa — status=FAILED ✓ (estoque reservado liberado antes de falhar)" \
    || err "SAGA status=$STATUS2 (esperava FAILED)"
fi
nl

# ── Notificações e Pedidos ────────────────────────────────────────────────────
step "Notificações geradas pelas SAGAs:"
curl -sf -H "X-Client-ID: cliente-alpha-001" "$ROUTER/notificacoes/" | python3 -c "
import sys, json
ns = json.load(sys.stdin)
if not ns:
    print('  (consumer ainda processando)')
else:
    for n in ns[:5]:
        print(f'  tipo={n.get(\"tipo\",\"?\"):<22}  canal={n.get(\"canal\",\"?\"):<6}  dest={str(n.get(\"destinatario_id\",\"\"))[:8]}...')
print(f'  Total: {len(ns)} notificação(ões) neste shard')
" 2>/dev/null
nl

step "Stats de pedidos (CQRS read model):"
curl -sf -H "X-Client-ID: cliente-alpha-001" "$ROUTER/pedidos/stats" | python3 -c "
import sys, json
s = json.load(sys.stdin)
print(f'  total={s.get(\"total\",0)}  pendentes={s.get(\"pendentes\",0)}  confirmados={s.get(\"confirmados\",0)}  cancelados={s.get(\"cancelados\",0)}')
" 2>/dev/null
nl

# ── Kafka Topics ──────────────────────────────────────────────────────────────
header "PILAR 1 — Tópicos Kafka (KRaft — criação explícita via kafka-init)"
step "Tópicos criados no cluster poc-eci:"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:29092 --list 2>/dev/null \
  | sort | while IFS= read -r t; do
    [ -z "$t" ] && continue
    case "$t" in
      commands.*) printf "  \033[33m→ cmd\033[0m  %s\n" "$t" ;;
      replies.*)  printf "  \033[32m← rep\033[0m  %s\n" "$t" ;;
      events.*)   printf "  \033[36m◉ evt\033[0m  %s\n" "$t" ;;
      cdc.*)      printf "  \033[35m↓ cdc\033[0m  %s\n" "$t" ;;
      audit.*)    printf "  \033[34m✎ aud\033[0m  %s\n" "$t" ;;
      dlq.*)      printf "  \033[31m☓ dlq\033[0m  %s\n" "$t" ;;
      *)          printf "  · sys  %s\n" "$t" ;;
    esac
  done
nl

step "Consumer groups registrados:"
docker compose exec -T kafka kafka-consumer-groups \
  --bootstrap-server kafka:29092 --list 2>/dev/null \
  | grep -v "^$" | head -20 | while IFS= read -r g; do echo "  · $g"; done
nl

# ═══════════════════════════════════════════════════════════════════════════════
# PILAR 2 — ENGENHARIA DE PLATAFORMA
# ═══════════════════════════════════════════════════════════════════════════════
header "PILAR 2 — Engenharia de Plataforma"

step "Interfaces de observabilidade disponíveis:"
ok "Shard Router (API única):   $ROUTER/router/cells"
ok "Kafka UI (eventos vis.):    $KAFKA_UI"
ok "Jaeger (traces Go↔Kafka):  $JAEGER"
ok "Prometheus (métricas):      $PROMETHEUS"
ok "Grafana (dashboards):       $GRAFANA  (admin / poc123)"
ok "Agent MCP (Swagger UI):     $AGENT/docs"
nl

step "Health consolidado pelo shard-router:"
curl -sf "$ROUTER/router/cells" | python3 -c "
import sys, json
cells     = json.load(sys.stdin).get('cells', [])
saudaveis = sum(1 for c in cells if c['Healthy'])
total     = len(cells)
print(f'  {saudaveis}/{total} células saudáveis')
for pbc in ['pedidos', 'estoque', 'notificacoes']:
    ativos   = [c for c in cells if c['PBC'] == pbc and c['Role'] == 'active'  and c['Healthy']]
    passivos = [c for c in cells if c['PBC'] == pbc and c['Role'] == 'passive' and c['Healthy']]
    print(f'  {pbc:<15} active={len(ativos)}/3  passive={len(passivos)}/3')
" 2>/dev/null
nl

step "data-sync (CDC ativo→passivo) — readiness:"
DATA_SYNC_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9191/healthz/ready" 2>/dev/null || echo "000")
if [ "$DATA_SYNC_STATUS" = "200" ]; then
  ok "data-sync pronto — pools passivos conectados (HTTP 200)"
elif [ "$DATA_SYNC_STATUS" = "503" ]; then
  err "data-sync degradado — nenhum pool passivo conectado (HTTP 503)"
else
  info "data-sync não acessível na porta 9191 (status=$DATA_SYNC_STATUS)"
fi
nl

step "Contadores Prometheus (via scrape das células Go):"
python3 3>/dev/null <<'PYEOF'
import urllib.request, json, urllib.parse, sys

PROMETHEUS = "http://localhost:9095"
queries = [
    ("shard_router_requests_total",                 "Requests via shard-router"),
    ("shard_router_failover_total",                 "Failovers ativo → passivo"),
    ("saga_started_total",                          "SAGAs iniciadas"),
    ("saga_completed_total",                        "SAGAs completadas/compensadas"),
    ("data_sync_applied_total",                     "CDC events aplicados (data-sync)"),
    ("data_sync_lag_seconds",                       "Lag CDC atual (segundos)"),
    ("circuit_breaker_state",                       "Circuit breakers abertos (estado=2)"),
]
for q, label in queries:
    try:
        url = f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(q)}"
        with urllib.request.urlopen(url, timeout=4) as r:
            data = json.loads(r.read())
        results = data.get("data", {}).get("result", [])
        total   = sum(float(r["value"][1]) for r in results) if results else 0
        print(f"  {label:<45} = {total:.0f}")
    except Exception:
        print(f"  {label:<45} = (ainda scraping)")
PYEOF
nl

step "SLO — Disponibilidade (shard:availability:rate5m):"
python3 3>/dev/null <<'PYEOF'
import urllib.request, json, urllib.parse

PROMETHEUS = "http://localhost:9095"
try:
    url = f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote('shard:availability:rate5m')}"
    with urllib.request.urlopen(url, timeout=4) as r:
        data = json.loads(r.read())
    results = data.get("data", {}).get("result", [])
    if not results:
        print("  (aguardando dados suficientes para calcular SLO)")
    else:
        for r in results:
            shard = r["metric"].get("shard", "?")
            pbc   = r["metric"].get("pbc", "?")
            raw   = float(r["value"][1])
            if raw != raw:  # NaN — sem tráfego suficiente
                print(f"  · {shard}/{pbc:<15} disponibilidade=n/a  (sem tráfego ainda)")
            else:
                val  = raw * 100
                flag = "✓" if val >= 99.9 else "✗"
                print(f"  {flag} {shard}/{pbc:<15} disponibilidade={val:.3f}%  (SLO: 99.9%)")
except Exception:
    print("  (Prometheus não acessível ou slo-rules ainda não avaliadas)")
PYEOF
nl

# ═══════════════════════════════════════════════════════════════════════════════
# PILAR 3 — ARQUITETURA EVOLUTIVA (FITNESS FUNCTIONS)
# ═══════════════════════════════════════════════════════════════════════════════
header "PILAR 3 — Arquitetura Evolutiva (Fitness Functions FF1–FF4)"

step "Executando suite completa..."
nl
if python3 fitness-functions/run_all.py 2>&1; then
  nl; ok "Suite de fitness functions: APROVADA"
else
  nl; err "Suite de fitness functions: reprovada (veja detalhes acima)"
fi
nl

# ═══════════════════════════════════════════════════════════════════════════════
# PILAR 4 — IA AGÊNTICA COM MCP
# ═══════════════════════════════════════════════════════════════════════════════
header "PILAR 4 — IA Agêntica com MCP (Claude + Anthropic SDK)"

if wait_healthy "$AGENT/health" "agent-mcp:9000/health ..." 20 2>/dev/null; then
  step "Tools MCP disponíveis (12 tools: 7 cell + 5 shard-aware):"
  curl -sf "$AGENT/agente/tools" 2>/dev/null | python3 -c "
import sys, json
tools = json.load(sys.stdin).get('tools', [])
mcp   = [t for t in tools if t['name'] not in {'listar_status_shards','verificar_saga','iniciar_saga_pedido','reiniciar_celula','consultar_prometheus'}]
shard = [t for t in tools if t['name'] in {'listar_status_shards','verificar_saga','iniciar_saga_pedido','reiniciar_celula','consultar_prometheus'}]
print(f'  MCP tools ({len(mcp)}):')
for t in mcp:
    print(f'    · {t[\"name\"]:<28} {t[\"description\"][:50]}')
print(f'  Shard-aware tools ({len(shard)}):')
for t in shard:
    print(f'    · {t[\"name\"]:<28} {t[\"description\"][:50]}')
" 2>/dev/null
  nl

  if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    MODEL="${ANTHROPIC_MODEL:-claude-sonnet-4-6}"
    step "Consultando agente Claude ($MODEL) — diagnosticar estado do sistema..."
    AGENT_RESP=$(curl -sf -X POST "$AGENT/agente/executar" \
      -H "Content-Type: application/json" \
      -d '{"prompt":"Verifique a saúde das células via listar_status_shards e resuma os pedidos criados. Seja breve."}' \
      2>/dev/null || echo '{"resultado":"(timeout)"}')
    echo "$AGENT_RESP" | python3 -c \
      "import sys,json; print(json.load(sys.stdin).get('resultado',''))" 2>/dev/null
    nl

    step "Previsão de scaling (EMA + slope forecast):"
    curl -sf "$AGENT/agente/scaling/previsao" 2>/dev/null | python3 -c "
import sys, json
for p in json.load(sys.stdin).get('previsoes', []):
    print(f'  {p[\"cell\"]:<15} rps={p[\"current_rps\"]:.2f}  forecast_5m={p[\"predicted_rps_5m\"]:.2f}  replicas={p[\"recommended_replicas\"]}')
" 2>/dev/null
    nl

    step "Anomaly detection (IsolationForest):"
    curl -sf "$AGENT/agente/anomalias" 2>/dev/null | python3 -c "
import sys, json
r = json.load(sys.stdin)
if r.get('reason') == 'insufficient data':
    print('  Coletando histórico — rode novamente após 10+ ciclos de monitoramento (10 min)')
else:
    flag = '⚠ ANOMALIA DETECTADA' if r.get('anomaly') else '✓ Normal'
    print(f'  {flag}  score={r.get(\"score\",\"N/A\")}')
" 2>/dev/null
  else
    info "ANTHROPIC_API_KEY não configurada — agente em modo passivo"
    info ""
    info "Para ativar o agente IA:"
    info "  export ANTHROPIC_API_KEY=sk-ant-..."
    info "  export ANTHROPIC_MODEL=claude-sonnet-4-6  # opcional"
    info "  docker compose restart agent-mcp"
    info "  ./demo.sh"
    info ""
    info "Swagger UI disponível em: $AGENT/docs"
  fi
else
  err "Agent MCP não respondeu em 20s. Verifique: docker compose logs agent-mcp"
fi
nl

# ═══════════════════════════════════════════════════════════════════════════════
# RESUMO FINAL
# ═══════════════════════════════════════════════════════════════════════════════
header "Demo Concluída — ECI v2 (v2.14.0)"

ok "PILAR 1 — 3 PBCs em Go | 3 shards × (active+passive) | SAGA orquestrada via Kafka"
ok "PILAR 2 — Shard Router (consistent hash) | OTel→Jaeger | Prometheus/Grafana | SLO"
ok "PILAR 3 — FF1 Boundary | FF2 Contract | FF3 p99 Latência | FF4 Chaos Bulkhead"
ok "PILAR 4 — Claude + 12 MCP tools | IsolationForest | EMA Predictor | ANTHROPIC_MODEL"
nl

echo -e "  ${BOLD}Leis Arquiteturais:${NC}"
info "  L1: PBCs são ilhas — sem imports/HTTP cross-PBC          (verificado: FF1)"
info "  L2: saga-hub é o único integrador entre PBCs             (Kafka orquestration)"
info "  L3: toda requisição entra pelo shard-router:8080         (consistent hash)"
info "  L4: células passivas recusam escrita HTTP 503            (CELL_ROLE=passive)"
info "  L5: Scale Unit = 1 célula + 1 DB + 1 Redis + 1 group    (docker-compose)"
nl

echo -e "  ${BOLD}Qualidade (REFINE.md — 47 melhorias):${NC}"
info "  R1: bugs críticos corrigidos — transação atômica, compensação SAGA, at-least-once"
info "  R2: módulo shared/ — elimina 18 pacotes duplicados entre os 6 componentes Go"
info "  R3: consistência arquitetural — cell-notificacoes, CorrelationID separado"
info "  R4: observabilidade — watcher semáforo, proxy cache, SLO latência, data-sync lag"
info "  R5: CI/CD — Kafka KRaft, PDB, NetworkPolicy, tópicos explícitos, versões fixas"
info "  R6: testes — 25 casos unitários de domínio e resilience, FF1/FF2 corrigidos"
nl

echo -e "  ${BOLD}Interfaces:${NC}"
printf "  %-12s %s\n" "Router:"     "$ROUTER/router/cells"
printf "  %-12s %s\n" "Kafka UI:"   "$KAFKA_UI"
printf "  %-12s %s\n" "Jaeger:"     "$JAEGER"
printf "  %-12s %s\n" "Grafana:"    "$GRAFANA  (admin/poc123)"
printf "  %-12s %s\n" "Prometheus:" "$PROMETHEUS"
printf "  %-12s %s\n" "Agent MCP:"  "$AGENT/docs"
nl
echo -e "  ${BOLD}Comandos úteis:${NC}"
info "  make kafka-topics"
info "  make kafka-lag"
info "  docker compose logs -f cell-pedidos-s1-active"
info "  curl $ROUTER/router/cells | python3 -m json.tool"
nl
