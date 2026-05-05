#!/bin/bash
# PostgreSQL backup — all active DBs
# Usage: DATABASE_URL_PREFIX=postgres://user:pass@host BACKUP_DIR=/backups ./pg-backup.sh
set -euo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR=${BACKUP_DIR:-/backups}
RETAIN_DAYS=${RETAIN_DAYS:-7}

mkdir -p "$BACKUP_DIR"

backup_db() {
  local label="$1"
  local dsn="$2"
  local file="$BACKUP_DIR/${label}_${TIMESTAMP}.sql.gz"
  echo "  backing up $label..."
  pg_dump "$dsn" | gzip > "$file"
  echo "  done: $file ($(du -sh "$file" | cut -f1))"
}

echo "=== PostgreSQL Backup $(date) ==="

# Active DBs per shard × PBC
for shard in s1 s2 s3; do
  backup_db "pedidos-${shard}-active"      "${PEDIDOS_DSN_PREFIX:-postgres://pedidos:pedidos123@db-pedidos-${shard}-active:5432/pedidos}"
  backup_db "estoque-${shard}-active"      "${ESTOQUE_DSN_PREFIX:-postgres://estoque:estoque123@db-estoque-${shard}-active:5432/estoque}"
  backup_db "notificacoes-${shard}-active" "${NOTIF_DSN_PREFIX:-postgres://notificacoes:notif123@db-notif-${shard}-active:5432/notificacoes}"
done
backup_db "saga" "${SAGA_DSN:-postgres://saga:saga123@db-saga:5432/saga}"

# Retain only last N days
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +"$RETAIN_DAYS" -delete
echo "Pruned backups older than ${RETAIN_DAYS} days."

echo "=== Backup complete ==="
