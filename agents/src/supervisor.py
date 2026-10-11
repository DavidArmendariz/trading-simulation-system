"""Agent pool supervisor.

Owns no trading logic. It listens on `simulation:control` for the admin panel's
START / STOP / CONFIG_UPDATE commands and shards the requested agent count and
Target TPS across worker processes.

CONFIG_UPDATE while running is handled by a clean restart of the pool: at these
scales re-sharding live is more failure modes than it is worth, and a restart
costs well under a second.
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
import os
import signal
import time

import redis.asyncio as aioredis

from worker import worker_entry

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [supervisor] %(message)s",
)
log = logging.getLogger("supervisor")

CONTROL_CHANNEL = "simulation:control"
STATE_KEY = "sim:state"
STATUS_CHANNEL = "agents:status"


def env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except ValueError:
        return default


class Pool:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.workers: list[mp.Process] = []
        self.agent_count = env_int("AGENT_COUNT", 2000)
        self.target_tps = env_int("TARGET_TPS", 5000)
        self.worker_count = max(1, env_int("AGENT_WORKERS", 4))
        # spawn, not fork: the child builds its own event loop and ZMQ context.
        self.mp_ctx = mp.get_context("spawn")

    @property
    def running(self) -> bool:
        return any(p.is_alive() for p in self.workers)

    def start(self) -> None:
        if self.running:
            log.info("start ignored, pool already running")
            return

        shards = self.worker_count
        per_worker = max(1, self.agent_count // shards)
        tps_share = self.target_tps / shards
        log.info(
            "starting %d workers, %d agents each, %.0f TPS each",
            shards, per_worker, tps_share,
        )

        for i in range(shards):
            # Last shard absorbs the remainder so totals match exactly.
            count = per_worker if i < shards - 1 else self.agent_count - per_worker * (shards - 1)
            process = self.mp_ctx.Process(
                target=worker_entry,
                args=(i, i * per_worker + 1, max(1, count), tps_share, self.cfg),
                daemon=True,
                name=f"agent-worker-{i}",
            )
            process.start()
            self.workers.append(process)

    def stop(self) -> None:
        if not self.workers:
            return
        log.info("stopping %d workers", len(self.workers))
        for process in self.workers:
            if process.is_alive():
                process.terminate()
        deadline = time.monotonic() + 5.0
        for process in self.workers:
            process.join(timeout=max(0.1, deadline - time.monotonic()))
            if process.is_alive():
                log.warning("worker %s did not exit, killing", process.name)
                process.kill()
                process.join(timeout=2.0)
        self.workers = []

    def reconfigure(self, payload: dict) -> None:
        if "agent_count" in payload:
            self.agent_count = max(1, min(int(payload["agent_count"]), 20_000))
        if "target_tps" in payload:
            self.target_tps = max(1, min(int(payload["target_tps"]), 200_000))
        if "workers" in payload:
            self.worker_count = max(1, min(int(payload["workers"]), 32))
        log.info(
            "config: agents=%d tps=%d workers=%d",
            self.agent_count, self.target_tps, self.worker_count,
        )
        if self.running:
            self.stop()
            self.start()


async def heartbeat(client: aioredis.Redis, pool: Pool, stop: asyncio.Event) -> None:
    while not stop.is_set():
        payload = {
            "ts": int(time.time() * 1000),
            "running": pool.running,
            "workers": len(pool.workers),
            "agent_count": pool.agent_count,
            "target_tps": pool.target_tps,
        }
        try:
            await client.publish(STATUS_CHANNEL, json.dumps(payload))
            await client.set(STATE_KEY + ":agents", json.dumps(payload))
        except Exception as exc:  # noqa: BLE001
            log.warning("heartbeat failed: %s", exc)
        await asyncio.sleep(2.0)


async def resume_previous_state(client: aioredis.Redis, pool: Pool) -> None:
    """A restarted supervisor should match whatever the operator last asked for."""
    try:
        raw = await client.get(STATE_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("cannot read %s: %s", STATE_KEY, exc)
        return
    if not raw:
        return
    try:
        state = json.loads(raw)
    except json.JSONDecodeError:
        return
    pool.reconfigure(state)
    if state.get("running"):
        log.info("resuming previous RUNNING state")
        pool.start()


async def main() -> None:
    cfg = {
        "redis_url": os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"),
        "ipc_endpoint": os.environ.get("IPC_ENDPOINT", "ipc:///tmp/ipc/orders.ipc"),
        "initial_price_ticks": env_int("INITIAL_PRICE_TICKS", 10_000),
    }
    pool = Pool(cfg)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    client = aioredis.from_url(cfg["redis_url"], decode_responses=True)
    for attempt in range(30):
        try:
            await client.ping()
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("Redis not ready (%s), retry %d", exc, attempt + 1)
            await asyncio.sleep(1.0)

    await resume_previous_state(client, pool)
    beat = asyncio.create_task(heartbeat(client, pool, stop))

    pubsub = client.pubsub(ignore_subscribe_messages=True)
    await pubsub.subscribe(CONTROL_CHANNEL)
    log.info("listening on %s", CONTROL_CHANNEL)

    try:
        while not stop.is_set():
            message = await pubsub.get_message(timeout=1.0)
            if message is None:
                continue
            try:
                control = json.loads(message["data"])
            except json.JSONDecodeError:
                log.warning("unparseable control message: %r", message["data"])
                continue

            cmd = str(control.get("cmd", "")).upper()
            payload = control.get("payload") or {}
            log.info("control: %s %s", cmd, payload)

            if cmd == "START":
                if payload:
                    pool.reconfigure(payload)
                pool.start()
            elif cmd == "STOP":
                pool.stop()
            elif cmd == "CONFIG_UPDATE":
                pool.reconfigure(payload)
            elif cmd == "RESET":
                pass  # the engine owns book state
    finally:
        stop.set()
        beat.cancel()
        pool.stop()
        await pubsub.aclose()
        await client.aclose()
        log.info("supervisor stopped")


if __name__ == "__main__":
    asyncio.run(main())
