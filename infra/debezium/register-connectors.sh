#!/bin/bash
set -e
DEBEZIUM_URL=${DEBEZIUM_URL:-http://debezium:8083}

register() {
  local name="$1"
  local payload="$2"
  echo "  Registering connector: $name"
  local status
  status=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$DEBEZIUM_URL/connectors" \
    -H "Content-Type: application/json" -d "$payload" 2>/dev/null) || true
  if [ "$status" = "201" ] || [ "$status" = "409" ]; then
    echo "    OK ($status)"
  else
    echo "    WARN: got HTTP $status for $name"
  fi
}

echo "Waiting for Debezium REST API..."
until curl -sf "$DEBEZIUM_URL/connectors" > /dev/null 2>&1; do
  sleep 3
done
echo "Debezium ready. Registering 9 connectors (3 shards × 3 PBCs)..."

for shard in s1 s2 s3; do
  # ── cell-pedidos ──────────────────────────────────────────────
  register "cdc-${shard}-pedidos" "{
    \"name\": \"cdc-${shard}-pedidos\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"db-pedidos-${shard}-active\",
      \"database.port\": \"5432\",
      \"database.user\": \"pedidos\",
      \"database.password\": \"pedidos123\",
      \"database.dbname\": \"pedidos\",
      \"database.server.name\": \"db-pedidos-${shard}-active\",
      \"table.include.list\": \"public.pedidos\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_${shard}_pedidos\",
      \"publication.name\": \"dbz_pub_${shard}_pedidos\",
      \"transforms\": \"route\",
      \"transforms.route.type\": \"org.apache.kafka.connect.transforms.ReplaceField\$Key\",
      \"topic.prefix\": \"cdc.${shard}.pedidos\"
    }
  }"

  # ── cell-estoque ──────────────────────────────────────────────
  register "cdc-${shard}-estoque" "{
    \"name\": \"cdc-${shard}-estoque\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"db-estoque-${shard}-active\",
      \"database.port\": \"5432\",
      \"database.user\": \"estoque\",
      \"database.password\": \"estoque123\",
      \"database.dbname\": \"estoque\",
      \"database.server.name\": \"db-estoque-${shard}-active\",
      \"table.include.list\": \"public.produtos\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_${shard}_estoque\",
      \"publication.name\": \"dbz_pub_${shard}_estoque\",
      \"topic.prefix\": \"cdc.${shard}.estoque\"
    }
  }"

  # ── cell-notificacoes ─────────────────────────────────────────
  register "cdc-${shard}-notificacoes" "{
    \"name\": \"cdc-${shard}-notificacoes\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
      \"database.hostname\": \"db-notif-${shard}-active\",
      \"database.port\": \"5432\",
      \"database.user\": \"notificacoes\",
      \"database.password\": \"notif123\",
      \"database.dbname\": \"notificacoes\",
      \"database.server.name\": \"db-notif-${shard}-active\",
      \"table.include.list\": \"public.notificacoes\",
      \"plugin.name\": \"pgoutput\",
      \"slot.name\": \"debezium_${shard}_notif\",
      \"publication.name\": \"dbz_pub_${shard}_notif\",
      \"topic.prefix\": \"cdc.${shard}.notificacoes\"
    }
  }"
done

echo ""
echo "Done. Registered connectors:"
curl -sf "$DEBEZIUM_URL/connectors" | tr -d '[]"' | tr ',' '\n' | sed 's/^/  /'
