# CLAUDE.md

Distributed market simulator: Python agents -> ZeroMQ IPC -> Rust matching engine -> Redis Pub/Sub -> Go gateway (Postgres + WebSocket) -> React dashboard. All six services run via `docker compose`. README.md has run commands, ports, API, and tuning knobs; read it first.

## Commands

- `make up` / `make down` / `make clean` (drops volumes) / `make logs`
- `make start` / `make stop` / `make status` (simulation control via gateway REST)
- `make test-engine` (Rust order book tests, run locally with cargo)
- Host ports are offset to avoid clashes: Postgres 15432, Redis 16379, gateway 18080, UI 13000.

## Invariants (do not break)

1. **Integer ticks end to end.** Prices are `u64` cents, quantities `u64` shares in engine, wire, Redis, and DB (`BIGINT`). Only the React UI divides by 100. Never introduce floats for price or qty.
2. **Book is single-owner, lock-free.** Only the `matching` thread touches `OrderBook`. Other threads (ZMQ intake, Redis control, publisher, stats) talk to it over bounded crossbeam channels. Do not add `Arc<Mutex<OrderBook>>`.
3. **Gateway is the only Postgres writer.** Engine and agents never touch the DB.
4. **Wire formats:** agent -> engine is MessagePack with single-character keys (`engine/src/types.rs` `InboundMsg`). Engine -> Redis -> browser is JSON, forwarded nearly verbatim by the gateway.
5. **Every published message carries a monotonic `seq`.** The UI counts gaps; keep it when adding message types.

## Design decisions and why

- **Snapshots coalesced, trades not.** Engine emits top-N depth on a 60Hz tick and batches all trades since the last tick. Per-event publishing saturates Redis and the browser.
- **Backpressure drops the right thing.** Full inbound channel: drop orders and count them (never stall ZMQ, that stalls every agent). Full publisher channel: drop stale snapshot, keep trades (gateway persists them). Slow WS client: 1-slot conflated channel, gets newest book.
- **Book size cap.** Agents never cancel (they never learn engine order ids), so resting orders grew ~15MB/min. `MAX_RESTING` (200k) evicts oldest via an `arrivals` queue.
- **Each price `Level` keeps a running `total` qty** so `l2_snapshot` is O(depth), not O(orders). Summing VecDeques at 60Hz was the dominant cost.
- **Closed-loop TPS pacer in agents.** Momentum and mean-reversion agents skip most wake-ups, so fixed wake rates landed ~30% under target. Each worker runs a per-second P-controller on its realized rate.
- **Agents shard across processes** (CPython GIL). `multiprocessing` uses `spawn`, not `fork`: each child builds its own event loop and ZMQ context. Mix is 70% noise, 20% momentum, 10% mean reversion.
- **CONFIG_UPDATE restarts the agent pool** instead of live re-sharding. Restart is sub-second and has fewer failure modes.
- **Gateway expires per-worker rate reports after 5s**, otherwise a stopped pool keeps showing stale throughput.
- **Persistence:** trades batched via `CopyFrom` every 250ms or 1,000 rows. `trades.ts` uses a BRIN index (append-only, time-ordered). 1s OHLCV candles are upserted repeatedly as buckets fill, so volume and trade counts add, not replace.
- **Redis runs with no persistence** (`--save "" --appendonly no`). It is a bus, not a store.
- **Agent pool is the throughput bottleneck**, not the engine (measured 20,125 orders/s: engine 86% of one core, agents ~411% across 4 workers). Raise `AGENT_WORKERS` first.

## Gotchas

- **Borrow checker in `OrderBook::submit`:** lift the price `Level` out of the `BTreeMap` with `.remove()`, fill against it, re-insert if non-empty. Holding `&mut self.asks` while touching `self.index` or `self.next_trade_id` does not compile.
- **Go image:** `golang:1.23-alpine` fails `go mod tidy` because gin test deps need go >= 1.25. Use `golang:1.25-alpine` with `GOTOOLCHAIN=auto`.
- **IPC socket:** engine unlinks the stale `ipc://` file on boot, so `docker compose restart engine` alone works and agents reconnect.
- **DB migrations** in `db/migrations/` are mounted into the gateway and applied on boot in lexical order (`gateway/internal/store/postgres.go`). Applied files are recorded in `schema_migrations` and never re-run, so never edit an applied file: add a new one with the next numeric prefix.

## OpenSpec

Specs live in `openspec/specs/` (order-matching, agent-simulation, market-data-gateway). For behavior changes use `/opsx:propose`, then `/opsx:apply`, then `/opsx:archive`. CLI comes from nvm Node 24: `/usr/local/bin/node` has no npm, so run `export PATH="$HOME/.nvm/versions/node/v24.14.0/bin:$PATH"` first.

## Decision log

Append new decisions here: date, decision, reason. Keep each to one or two lines.

- 2026-09-01: Initial architecture built and verified (all decisions above).
- 2026-10-11: Adopted OpenSpec with baseline specs for the three backend services.
- 2026-10-11: Gateway tracks applied migrations in `schema_migrations`; each file commits in one transaction with its record. Re-running every file on boot only worked while all files stayed idempotent.
