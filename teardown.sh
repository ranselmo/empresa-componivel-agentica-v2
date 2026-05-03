#!/usr/bin/env bash
# teardown.sh — ECI v2 — Para e remove todos os recursos provisionados pela demo
#
# Uso:
#   ./teardown.sh           # remove containers, volumes e rede
#   ./teardown.sh --images  # idem + apaga imagens Go/Python buildadas (~15MB × 7)
set -uo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
RED='\033[0;31m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

step() { echo -e "${YELLOW}▶ $1${NC}"; }
ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
err()  { echo -e "  ${RED}✗${NC} $1"; }
info() { echo -e "  ${DIM}$1${NC}"; }

REMOVE_IMAGES=false
for arg in "$@"; do
  [ "$arg" = "--images" ] && REMOVE_IMAGES=true
done

echo
echo -e "${CYAN}${BOLD}"
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║   ECI v2 — Teardown                                 ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

if ! docker info >/dev/null 2>&1; then
  err "Docker não está rodando — nada a remover."
  exit 0
fi

# ── 1. Containers + volumes nomeados + rede ────────────────────────────────────
step "Parando e removendo containers, volumes e rede (poc-eci)..."
info "docker compose down -v --remove-orphans"
if docker compose down -v --remove-orphans 2>&1 | grep -E '(Stopping|Removing|Removed|removed|stopped|error)' | sed 's/^/  /'; then
  ok "Containers, volumes nomeados (grafana-data, prometheus-data) e rede removidos"
else
  ok "Nenhum container ativo encontrado"
fi
echo

# ── 2. Imagens buildadas (opcional) ───────────────────────────────────────────
if [ "$REMOVE_IMAGES" = true ]; then
  step "Removendo imagens buildadas do projeto..."
  IMAGES=(
    "poc-eci-shard-router"
    "poc-eci-saga-hub"
    "poc-eci-data-sync"
    "poc-eci-cell-pedidos"
    "poc-eci-cell-estoque"
    "poc-eci-cell-notificacoes"
    "poc-eci-agent-mcp"
  )
  removed=0
  for img in "${IMAGES[@]}"; do
    if docker image inspect "$img" >/dev/null 2>&1; then
      docker rmi "$img" >/dev/null 2>&1 && ok "$img removida" && ((removed++)) || err "falha ao remover $img"
    else
      info "$img não encontrada (já removida ou nunca buildada)"
    fi
  done
  [ "$removed" -gt 0 ] && ok "$removed imagem(ns) removida(s)" || info "Nenhuma imagem removida"
  echo
fi

# ── 3. Cache Python do agent-mcp ──────────────────────────────────────────────
step "Removendo cache Python gerado pelo agent-mcp..."
find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
find . -name '*.pyc' -delete 2>/dev/null || true
ok "Cache Python removido"
echo

# ── 4. Verificação final ───────────────────────────────────────────────────────
step "Verificando se restam recursos poc-eci..."
remaining=$(docker ps -a --filter "label=com.docker.compose.project=poc-eci" --format "{{.Names}}" 2>/dev/null)
if [ -z "$remaining" ]; then
  ok "Nenhum container poc-eci remanescente"
else
  err "Containers ainda presentes:"
  echo "$remaining" | sed 's/^/    /'
fi

volumes_left=$(docker volume ls --filter "label=com.docker.compose.project=poc-eci" --format "{{.Name}}" 2>/dev/null)
if [ -z "$volumes_left" ]; then
  ok "Nenhum volume poc-eci remanescente"
else
  err "Volumes ainda presentes: $volumes_left"
fi

network_left=$(docker network ls --filter "name=poc-eci" --format "{{.Name}}" 2>/dev/null)
if [ -z "$network_left" ]; then
  ok "Rede poc-eci removida"
else
  err "Rede ainda presente: $network_left"
fi

echo
echo -e "  ${BOLD}Recursos liberados:${NC}"
info "  · ~50+ containers (18 células × shard × role + infra)"
info "  · 20 bancos PostgreSQL (3 shards × 3 PBCs × 2 roles + saga)"
info "  · 9 instâncias Redis"
info "  · Kafka + Zookeeper + Debezium"
info "  · Volumes grafana-data e prometheus-data"
info "  · Rede Docker poc-eci"
if [ "$REMOVE_IMAGES" = true ]; then
  info "  · 7 imagens Docker buildadas (~15MB cada)"
fi
echo
info "Para reiniciar a demo: ./demo.sh"
if [ "$REMOVE_IMAGES" = false ]; then
  info "Para remover também as imagens buildadas: ./teardown.sh --images"
fi
echo
