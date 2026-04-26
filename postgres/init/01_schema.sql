-- ── Transactions (raw) ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    id               TEXT        PRIMARY KEY,
    user_id          TEXT        NOT NULL,
    amount           NUMERIC(14,2),
    currency         TEXT,
    merchant         TEXT,
    country          TEXT,
    card_type        TEXT,
    card_last4       TEXT,
    ip_address       TEXT,
    channel          TEXT,                       -- api | payment | cdc
    timestamp        TIMESTAMPTZ,
    ingested_at      TIMESTAMPTZ DEFAULT now()
);

-- ── Fraud decisions ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_decisions (
    id               SERIAL      PRIMARY KEY,
    transaction_id   TEXT        NOT NULL,
    fraud_score      NUMERIC(5,4),              -- 0.0000 → 1.0000
    is_fraud         BOOLEAN     NOT NULL,
    decision_ms      INT,                        -- latency in ms
    model_version    TEXT,
    features         JSONB,
    triggered_rules  TEXT[],
    decided_at       TIMESTAMPTZ DEFAULT now()
);

-- ── Feature store (offline) ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_features (
    user_id          TEXT        PRIMARY KEY,
    tx_count_1h      INT         DEFAULT 0,
    tx_count_24h     INT         DEFAULT 0,
    amount_sum_1h    NUMERIC(14,2) DEFAULT 0,
    amount_sum_24h   NUMERIC(14,2) DEFAULT 0,
    avg_amount       NUMERIC(14,2) DEFAULT 0,
    distinct_countries_24h INT   DEFAULT 0,
    last_tx_at       TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ DEFAULT now()
);

-- ── Alerts ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id               SERIAL      PRIMARY KEY,
    transaction_id   TEXT        NOT NULL,
    user_id          TEXT        NOT NULL,
    fraud_score      NUMERIC(5,4),
    alert_type       TEXT,
    message          TEXT,
    acknowledged     BOOLEAN     DEFAULT FALSE,
    created_at       TIMESTAMPTZ DEFAULT now()
);

-- ── DLQ ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dlq_messages (
    id               SERIAL      PRIMARY KEY,
    topic            TEXT,
    payload          TEXT,
    error_type       TEXT,
    error_message    TEXT,
    created_at       TIMESTAMPTZ DEFAULT now()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fraud_tx      ON fraud_decisions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_fraud_score   ON fraud_decisions(fraud_score DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_decided ON fraud_decisions(decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_tx_user       ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_tx_ts         ON transactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_tx     ON alerts(transaction_id);
CREATE INDEX IF NOT EXISTS idx_alerts_ack    ON alerts(acknowledged);
