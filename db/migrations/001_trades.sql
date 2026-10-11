-- Executed trade fills. Prices and quantities stay in integer ticks/shares,
-- matching the engine exactly; no floating point is stored.
CREATE TABLE IF NOT EXISTS trades (
    id          BIGSERIAL PRIMARY KEY,
    trade_id    BIGINT      NOT NULL,
    price_ticks BIGINT      NOT NULL,
    qty         BIGINT      NOT NULL,
    taker_side  SMALLINT    NOT NULL,  -- 0 = buyer aggressed, 1 = seller
    ts          TIMESTAMPTZ NOT NULL
);

-- BRIN, not BTREE: this table is append-only in time order and grows fast.
CREATE INDEX IF NOT EXISTS trades_ts_brin ON trades USING BRIN (ts);
