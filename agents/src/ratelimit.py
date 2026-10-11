"""Throughput shaping.

Two layers, deliberately:

* `poisson_delay` paces each agent independently, so order arrivals look like a
  real market (exponential inter-arrival times) rather than a metronome.
* `TokenBucket` sits in front of the single ZeroMQ sender per process and
  enforces the operator's Target TPS as a hard ceiling regardless of how many
  agents are running.
"""

from __future__ import annotations

import asyncio
import random
import time


def poisson_delay(rate_per_sec: float) -> float:
    """Exponential inter-arrival time for a rate of `rate_per_sec` events/sec."""
    if rate_per_sec <= 0:
        return 3600.0
    return random.expovariate(rate_per_sec)


class TokenBucket:
    __slots__ = ("_rate", "_capacity", "_tokens", "_updated")

    def __init__(self, rate: float, burst_seconds: float = 0.25):
        self._rate = max(rate, 1.0)
        self._capacity = max(self._rate * burst_seconds, 1.0)
        self._tokens = self._capacity
        self._updated = time.monotonic()

    def set_rate(self, rate: float, burst_seconds: float = 0.25) -> None:
        self._rate = max(rate, 1.0)
        self._capacity = max(self._rate * burst_seconds, 1.0)
        self._tokens = min(self._tokens, self._capacity)

    async def acquire(self, n: int = 1) -> None:
        while True:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
            self._updated = now
            if self._tokens >= n:
                self._tokens -= n
                return
            await asyncio.sleep((n - self._tokens) / self._rate)
