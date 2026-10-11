-- Per-agent ledger. Agents are identified by integer id only: this table holds
-- no names, no personal data.
CREATE TABLE IF NOT EXISTS agent_accounts (
    agent_id           INTEGER PRIMARY KEY,
    cash_ticks         BIGINT      NOT NULL DEFAULT 0,
    position           BIGINT      NOT NULL DEFAULT 0,
    realized_pnl_ticks BIGINT      NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
