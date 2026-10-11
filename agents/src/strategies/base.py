"""Strategy interface and the wire encoding for an order.

Message keys are single characters and values are integers only: this is packed
with MessagePack and handed to the Rust engine, which works in integer ticks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

BID = 0
ASK = 1

KIND_LIMIT = 0
KIND_MARKET = 1
KIND_CANCEL = 2

MIN_PRICE_TICKS = 1


def limit_order(agent_id: int, side: int, price_ticks: int, qty: int) -> dict:
    return {
        "k": KIND_LIMIT,
        "a": agent_id,
        "s": side,
        "p": max(MIN_PRICE_TICKS, int(price_ticks)),
        "q": max(1, int(qty)),
    }


def market_order(agent_id: int, side: int, qty: int) -> dict:
    return {"k": KIND_MARKET, "a": agent_id, "s": side, "p": 0, "q": max(1, int(qty))}


class Strategy(ABC):
    """One instance per simulated trader. `decide` runs on every wake-up and
    returns an order to send, or None to sit this one out."""

    __slots__ = ("agent_id", "rate")

    def __init__(self, agent_id: int, rate: float):
        self.agent_id = agent_id
        self.rate = rate

    @abstractmethod
    def decide(self, state) -> dict | None: ...
