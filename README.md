# Distributed Hybrid Market Simulation

A containerized continuous double auction: 2,000+ Python agents stream limit orders over ZeroMQ into a Rust matching engine, which publishes trades and L2 depth through Redis to a Go gateway, a PostgreSQL ledger, and a React trading terminal.

Measured on a laptop: **20,125 orders/sec** into the engine, 4,827 trades/sec printed, zero dropped orders, engine at 86% of one core and 38MB resident.

## Run it

```bash
cp .env.example .env
make up                 # build and start all six services
open http://localhost:13000
```

Then press **Start Simulation** in the dashboard, or:

```bash
make start              # curl POST /api/v1/simulation/start
make status
make stop
```

`make logs`, `make psql`, `make redis-cli`, `make stats`, `make test-engine`, `make clean`.

## Ports

| Service | Container | Host |
|---|---|---|
| PostgreSQL | 5432 | **15432** |
| Redis | 6379 | **16379** |
| Go gateway (REST + WS) | 8080 | **18080** |
| React dashboard | 5173 | **13000** |

## Architecture

```
React dashboard  ──WS/REST──>  Go gateway  ──>  PostgreSQL (trades, 1s OHLCV)
                                   ^
                                   │ Redis Pub/Sub
                                   v
Python agent pool  ──ZeroMQ IPC──>  Rust matching engine
```

**Rust engine** (`engine/`) owns the book: `BTreeMap<Ticks, Level>` for sorted price levels, `VecDeque<Order>` inside each for FIFO time priority. Four threads talk over bounded crossbeam channels (ZMQ intake → matching → publisher, plus a Redis control listener), and the book itself is single-owner, so the hot path holds no locks.

**Python agents** (`agents/`) shard across worker processes because CPython is GIL-bound. Each worker holds one PUSH socket, one Redis subscription feeding a shared `MarketState`, and one asyncio task per trader. Mix is 70% noise, 20% momentum, 10% mean reversion.

**Go gateway** (`gateway/`) is the only writer to PostgreSQL: batched `CopyFrom` every 250ms or 1,000 rows, plus a 1-second OHLCV aggregator. It also runs the WebSocket hub and the REST control plane.

## Design decisions worth knowing

**Integer ticks everywhere.** Prices are `u64` cents, quantities `u64` shares, end to end: engine, ZeroMQ wire, Redis, and database. Only React divides by 100 for display. No float ever touches a price.

**Snapshots are coalesced, trades are not.** The matching thread emits a top-15 depth snapshot on a 60Hz tick and batches every trade printed since the last tick. Publishing per event would saturate Redis and the browser, and the UI cannot use more than frame rate. Every message carries a monotonic `seq`; the dashboard counts gaps.

**Backpressure drops the right thing.** A full inbound channel drops orders and counts them rather than stalling the ZMQ socket, which would stall every agent. A full publisher channel drops the stale snapshot but keeps the trades, because the gateway persists them. A slow browser gets the newest book and loses frames instead of growing a queue.

**The book has a size cap.** Agents never cancel (they never learn their order ids), so resting orders would grow without bound. `MAX_RESTING` (default 200,000) expires the oldest quotes, which keeps engine memory flat during long runs. Each level carries a running total so building a snapshot is O(depth), not O(orders).

**Target TPS is a closed loop.** Momentum and mean-reversion agents decline to act most of the time, so a fixed wake rate lands ~30% under target. Each worker measures its own realized rate every second and scales agent wake rates until the pool delivers what the slider asked for, backing off if the send queue builds.

**MessagePack in, JSON out.** Agent → engine is MessagePack with single-character keys. Engine → Redis → browser is JSON, because the gateway forwards those payloads nearly verbatim.

## API

```
POST /api/v1/simulation/start    {"agent_count": 2000, "target_tps": 5000}
POST /api/v1/simulation/stop
POST /api/v1/simulation/config   {"agent_count": 800, "target_tps": 20000}
POST /api/v1/simulation/reset    {"initial_price_ticks": 10000}
GET  /api/v1/simulation/status
GET  /api/v1/candles?limit=600
GET  /ws                          WebSocket: book | trades | stats | agents
GET  /healthz
```

Redis channels: `market:book`, `market:trades`, `market:stats` (engine), `agents:stats`, `agents:status` (pool), `simulation:control` (gateway).

## Tuning

`AGENT_WORKERS` is the throughput ceiling: the agent pool saturates ~4 cores at 20k TPS while the engine is still at 86% of one. Raise workers before anything else. `SIM_SNAPSHOT_HZ` trades UI smoothness for Redis and browser load; `SIM_MAX_RESTING` trades book depth for engine memory.

## Tests

```bash
make test-engine     # 15 order book tests: FIFO priority, price priority,
                     # partial fills, sweeps, IOC, cancels, eviction, snapshots
```
