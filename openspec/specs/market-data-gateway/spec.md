# market-data-gateway Specification

## Purpose
Go gateway (`gateway/`): REST control plane, WebSocket fan-out to the dashboard, and the only writer to PostgreSQL (trades, OHLCV candles, schema).

## Requirements

### Requirement: Simulation control API
The gateway SHALL expose `POST /api/v1/simulation/{start,stop,config,reset}` and `GET /api/v1/simulation/status`, and SHALL relay commands on the Redis `simulation:control` channel.

#### Scenario: Start
- **WHEN** a client posts `{"agent_count": 2000, "target_tps": 5000}` to `/api/v1/simulation/start`
- **THEN** a START command with those values is published on `simulation:control`

### Requirement: Stale worker rates expire
The gateway SHALL ignore a worker's reported rate once it is older than 5 seconds.

#### Scenario: Pool stopped
- **WHEN** workers stop reporting
- **THEN** reported throughput drops to zero within 5 seconds

### Requirement: Batched trade persistence
The gateway SHALL persist every trade with `CopyFrom` batches flushed every 250ms or at 1,000 rows, whichever comes first. Prices and quantities SHALL be stored as integer `BIGINT`.

#### Scenario: Burst of trades
- **WHEN** 1,000 trades arrive within 250ms
- **THEN** they are written in one batch without waiting for the timer

### Requirement: OHLCV candles
The gateway SHALL aggregate trades into 1-second candles, upserting partial buckets so volume and trade counts accumulate. `GET /api/v1/candles` SHALL return recent candles, default limit 500, clamped to 1..5000.

#### Scenario: Limit out of range
- **WHEN** a client requests `/api/v1/candles?limit=10000`
- **THEN** at most 5,000 candles are returned

### Requirement: WebSocket fan-out with conflation
`GET /ws` SHALL stream book, trades, stats, and agents messages. Each client SHALL have a 1-slot conflated book channel and a small buffered event channel; a slow client SHALL receive the newest book and lose events rather than grow a queue.

#### Scenario: Slow browser
- **WHEN** a client cannot keep up with 60Hz snapshots
- **THEN** it receives the latest snapshot when it next reads, and memory per client stays bounded

### Requirement: Schema migrations on boot
The gateway SHALL apply SQL files from `db/migrations/` in lexical order at startup before serving traffic. It SHALL record each applied file in a `schema_migrations` table and SHALL NOT re-apply a recorded file. Each file and its record row SHALL commit in one transaction.

#### Scenario: Fresh database
- **WHEN** the gateway boots against an empty database
- **THEN** all migrations are applied, recorded in `schema_migrations`, and the tables exist

#### Scenario: Restart
- **WHEN** the gateway boots and every migration is already recorded
- **THEN** no migration SQL runs

#### Scenario: Failed migration
- **WHEN** a migration file fails partway
- **THEN** its changes roll back, it is not recorded, and the gateway exits with an error
