CREATE TABLE IF NOT EXISTS cotacoes (
    id          SERIAL PRIMARY KEY,
    moeda       VARCHAR(10)    NOT NULL,
    valor_brl   NUMERIC(10,4)  NOT NULL,
    coletado_em TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_log (
    id           SERIAL PRIMARY KEY,
    dag_id       VARCHAR(100),
    status       VARCHAR(20),
    mensagem     TEXT,
    executado_em TIMESTAMP DEFAULT NOW()
);
