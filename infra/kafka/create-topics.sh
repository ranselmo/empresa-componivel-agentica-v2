#!/bin/bash
set -e
KAFKA_BROKER=${KAFKA_BROKER:-kafka:29092}

create() {
  kafka-topics --bootstrap-server "$KAFKA_BROKER" \
    --create --if-not-exists \
    --topic "$1" --partitions "${2:-3}" --replication-factor 1 \
    --config retention.ms=86400000
}

# Commands
create commands.pedidos.criar    3
create commands.pedidos.cancelar 3
create commands.estoque.reservar 3
create commands.estoque.liberar  3
create commands.notificacoes.enviar 3

# Replies
create replies.pedidos.criado    3
create replies.pedidos.cancelado 3
create replies.estoque.reservado    3
create replies.estoque.insuficiente 3
create replies.estoque.liberado     3
create replies.notificacoes.enviada 3

# Events
create events.pedidos.confirmado 3
create events.pedidos.cancelado  3
create events.pedidos.falhou     3
create events.notificacoes.enviada 3

# Audit / DLQ
create audit.events 1
create dlq.pedidos.commands.pedidos.criar          1
create dlq.estoque.commands.estoque.reservar       1
create dlq.notificacoes.commands.notificacoes.enviar 1

# CDC topics (per shard × table)
for shard in shard-1 shard-2 shard-3; do
  for table in pedidos.pedidos estoque.produtos notificacoes.notificacoes; do
    create "cdc.$shard.$table" 1
  done
done

echo "All topics created."
