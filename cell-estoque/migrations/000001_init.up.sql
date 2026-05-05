CREATE TABLE IF NOT EXISTS produtos (
    id                    UUID PRIMARY KEY,
    nome                  TEXT NOT NULL,
    quantidade_disponivel INT  NOT NULL DEFAULT 0,
    preco                 NUMERIC(12,2) NOT NULL,
    atualizado_em         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_produtos_nome ON produtos(nome);
