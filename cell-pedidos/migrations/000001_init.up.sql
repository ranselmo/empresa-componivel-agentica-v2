CREATE TABLE IF NOT EXISTS pedidos (
    id            UUID PRIMARY KEY,
    cliente_id    UUID NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDENTE',
    valor_total   NUMERIC(12,2) NOT NULL,
    itens         JSONB NOT NULL,
    criado_em     TIMESTAMPTZ NOT NULL,
    atualizado_em TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pedidos_cliente ON pedidos(cliente_id);
CREATE INDEX IF NOT EXISTS idx_pedidos_status  ON pedidos(status);
CREATE INDEX IF NOT EXISTS idx_pedidos_criado  ON pedidos(criado_em DESC);
