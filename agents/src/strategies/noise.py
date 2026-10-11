"""Noise traders: 70% of the population.

Post random bids and asks clustered around the current spread. A minority of
their flow is marketable, which is what actually prints trades and keeps the
book from silently widening forever.
"""

from __future__ import annotations

import random

from .base import ASK, BID, Strategy, limit_order, market_order


class NoiseTrader(Strategy):
    __slots__ = ("max_offset", "max_qty", "aggression")

    def __init__(self, agent_id: int, rate: float, max_offset: int = 12, max_qty: int = 60,
                 aggression: float = 0.18):
        super().__init__(agent_id, rate)
        self.max_offset = max_offset
        self.max_qty = max_qty
        self.aggression = aggression

    def decide(self, state) -> dict | None:
        side = BID if random.random() < 0.5 else ASK
        qty = random.randint(1, self.max_qty)

        if random.random() < self.aggression:
            return market_order(self.agent_id, side, max(1, qty // 2))

        mid = state.mid
        offset = random.randint(1, self.max_offset)
        # Occasionally quote inside the spread to keep it tight.
        if random.random() < 0.25:
            offset = max(1, state.spread // 2)

        price = mid - offset if side == BID else mid + offset
        return limit_order(self.agent_id, side, price, qty)
