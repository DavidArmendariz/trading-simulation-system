-- OHLCV bars aggregated by the gateway. Partial buckets are upserted
-- repeatedly as they fill, so volume and trade counts are added, not replaced.
CREATE TABLE IF NOT EXISTS candles (
    bucket   TIMESTAMPTZ NOT NULL,
    interval TEXT        NOT NULL DEFAULT '1s',
    open     BIGINT      NOT NULL,
    high     BIGINT      NOT NULL,
    low      BIGINT      NOT NULL,
    close    BIGINT      NOT NULL,
    volume   BIGINT      NOT NULL DEFAULT 0,
    trades   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, interval)
);

CREATE INDEX IF NOT EXISTS candles_interval_bucket ON candles (interval, bucket DESC);
