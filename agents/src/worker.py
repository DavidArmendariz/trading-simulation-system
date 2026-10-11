"""One agent worker process.

Python is GIL-bound, so 2,000 agents are sharded across several of these rather
than crammed into one interpreter. Each process owns:

  * one ZeroMQ PUSH socket into the engine's Unix domain socket,
  * one Redis subscription feeding a shared in-process `MarketState`,
  * N asyncio tasks, one per simulated trader.

Sends are non-blocking on a plain (non-async) socket with a large send high
water mark: at these rates the per-message coroutine overhead of zmq.asyncio
costs more than it buys, and we would rather drop and count than stall an agent.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time

import msgpack
import zmq

from market_state import MarketState
from ratelimit import TokenBucket, poisson_delay
from strategies.mean_reversion import MeanReversionTrader
from strategies.momentum import MomentumTrader
from strategies.noise import NoiseTrader

log = logging.getLogger("worker")

MIX = (
    (0.70, NoiseTrader),
    (0.20, MomentumTrader),
    (0.10, MeanReversionTrader),
)


def build_population(first_agent_id: int, count: int, rate_per_agent: float) -> list:
    """Deterministic 70/20/10 split so every run has the same composition."""
    agents = []
    noise_n = int(count * MIX[0][0])
    momentum_n = int(count * MIX[1][0])
    for i in range(count):
        agent_id = first_agent_id + i
        if i < noise_n:
            agents.append(NoiseTrader(agent_id, rate_per_agent))
        elif i < noise_n + momentum_n:
            agents.append(MomentumTrader(agent_id, rate_per_agent))
        else:
            agents.append(MeanReversionTrader(agent_id, rate_per_agent))
    random.shuffle(agents)
    return agents


class Pacer:
    """Closed loop on realized throughput.

    Momentum and mean-reversion agents decline to act most of the time, so a
    fixed per-agent wake rate lands well under Target TPS. This scales every
    agent's wake rate until the pool actually generates what the operator asked
    for, and backs off if the send queue starts building.
    """

    __slots__ = ("multiplier",)

    def __init__(self):
        self.multiplier = 1.0


async def pace_control(pacer: Pacer, counters: "Counters", queue: asyncio.Queue,
                       target: float, stop: asyncio.Event) -> None:
    previous = 0
    while not stop.is_set():
        await asyncio.sleep(1.0)
        generated = counters.generated
        rate = generated - previous
        previous = generated

        if queue.qsize() > 5_000:
            # Sender is the constraint; generating harder only grows the queue.
            pacer.multiplier = max(0.1, pacer.multiplier * 0.8)
            continue

        desired = pacer.multiplier * (target / rate) if rate > 0 else pacer.multiplier * 1.5
        # Damped so the loop settles instead of oscillating.
        pacer.multiplier = min(20.0, max(0.1, 0.5 * pacer.multiplier + 0.5 * desired))


class Counters:
    __slots__ = ("generated", "sent", "dropped_queue", "dropped_socket")

    def __init__(self):
        self.generated = 0
        self.sent = 0
        self.dropped_queue = 0
        self.dropped_socket = 0


async def market_subscriber(redis_url: str, state: MarketState, stop: asyncio.Event) -> None:
    import redis.asyncio as aioredis

    while not stop.is_set():
        try:
            client = aioredis.from_url(redis_url, decode_responses=True)
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe("market:book", "market:trades")
            async for message in pubsub.listen():
                if stop.is_set():
                    break
                payload = json.loads(message["data"])
                if message["channel"] == "market:book":
                    state.on_book(payload)
                else:
                    state.on_trades(payload.get("trades", []))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reconnect on any transport error
            log.warning("market subscriber lost: %s", exc)
            await asyncio.sleep(1.0)


async def agent_loop(agent, state: MarketState, queue: asyncio.Queue, counters: Counters,
                     pacer: Pacer, stop: asyncio.Event) -> None:
    # Stagger the herd so the first second is not one giant burst.
    await asyncio.sleep(random.random())
    while not stop.is_set():
        await asyncio.sleep(poisson_delay(agent.rate * pacer.multiplier))
        order = agent.decide(state)
        if order is None:
            continue
        counters.generated += 1
        try:
            queue.put_nowait(order)
        except asyncio.QueueFull:
            counters.dropped_queue += 1


async def sender(socket, queue: asyncio.Queue, bucket: TokenBucket, counters: Counters,
                 stop: asyncio.Event) -> None:
    packb = msgpack.packb
    while not stop.is_set():
        order = await queue.get()
        await bucket.acquire()
        try:
            socket.send(packb(order, use_bin_type=True), zmq.NOBLOCK)
            counters.sent += 1
        except zmq.Again:
            # Engine not up yet, or its receive queue is full.
            counters.dropped_socket += 1


async def reporter(redis_url: str, worker_id: int, counters: Counters, agent_count: int,
                   stop: asyncio.Event) -> None:
    import redis.asyncio as aioredis

    client = aioredis.from_url(redis_url, decode_responses=True)
    previous = 0
    while not stop.is_set():
        await asyncio.sleep(1.0)
        sent = counters.sent
        payload = {
            "worker_id": worker_id,
            "ts": int(time.time() * 1000),
            "agents": agent_count,
            "generated": counters.generated,
            "sent": sent,
            "sent_per_sec": sent - previous,
            "dropped_queue": counters.dropped_queue,
            "dropped_socket": counters.dropped_socket,
        }
        previous = sent
        try:
            await client.publish("agents:stats", json.dumps(payload))
        except Exception as exc:  # noqa: BLE001
            log.warning("stats publish failed: %s", exc)


async def _run(worker_id: int, first_agent_id: int, agent_count: int, tps_share: float,
               cfg: dict) -> None:
    ctx = zmq.Context.instance()
    socket = ctx.socket(zmq.PUSH)
    socket.setsockopt(zmq.SNDHWM, 1_000_000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(cfg["ipc_endpoint"])
    log.info("worker %d connected to %s", worker_id, cfg["ipc_endpoint"])

    state = MarketState(cfg["initial_price_ticks"])
    counters = Counters()
    queue: asyncio.Queue = asyncio.Queue(maxsize=200_000)
    bucket = TokenBucket(tps_share)
    pacer = Pacer()
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    rate_per_agent = tps_share / max(agent_count, 1)
    population = build_population(first_agent_id, agent_count, rate_per_agent)

    tasks = [
        asyncio.create_task(market_subscriber(cfg["redis_url"], state, stop)),
        asyncio.create_task(sender(socket, queue, bucket, counters, stop)),
        asyncio.create_task(reporter(cfg["redis_url"], worker_id, counters, agent_count, stop)),
        asyncio.create_task(pace_control(pacer, counters, queue, tps_share, stop)),
    ]
    tasks += [
        asyncio.create_task(agent_loop(a, state, queue, counters, pacer, stop))
        for a in population
    ]

    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    socket.close(0)
    ctx.term()
    log.info("worker %d stopped (sent=%d)", worker_id, counters.sent)


def worker_entry(worker_id: int, first_agent_id: int, agent_count: int, tps_share: float,
                 cfg: dict) -> None:
    """Process entry point. Must stay importable at module level for spawn()."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s [worker-%(process)d] %(message)s",
    )
    try:
        import uvloop

        uvloop.install()
    except ImportError:  # pragma: no cover - uvloop is in requirements
        pass
    try:
        asyncio.run(_run(worker_id, first_agent_id, agent_count, tps_share, cfg))
    except KeyboardInterrupt:
        pass
