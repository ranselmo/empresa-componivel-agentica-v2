CREATE TABLE IF NOT EXISTS notificacoes (
    id              UUID PRIMARY KEY,
    destinatario_id UUID NOT NULL,
    tipo            TEXT NOT NULL,
    canal           TEXT NOT NULL,
    conteudo        TEXT NOT NULL,
    enviado_em      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_notif_destinatario ON notificacoes(destinatario_id);
CREATE INDEX IF NOT EXISTS idx_notif_enviado      ON notificacoes(enviado_em DESC);
