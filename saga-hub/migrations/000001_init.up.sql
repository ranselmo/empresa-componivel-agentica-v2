CREATE TABLE IF NOT EXISTS sagas (
    id             UUID PRIMARY KEY,
    correlation_id UUID NOT NULL UNIQUE,
    status         TEXT NOT NULL,
    current_step   TEXT NOT NULL,
    cliente_id     UUID NOT NULL,
    shard_id       TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sagas_status         ON sagas(status);
CREATE INDEX IF NOT EXISTS idx_sagas_correlation_id ON sagas(correlation_id);
CREATE INDEX IF NOT EXISTS idx_sagas_created_at     ON sagas(created_at DESC);
