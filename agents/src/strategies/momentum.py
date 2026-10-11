"""Momentum traders: 20% of the population.

Compare a fast and a slow EMA of the trade tape. When the fast EMA pulls away
from the slow one, take the aggressive side of the market in that direction.
Below the threshold they stand aside, which is why they do not fight the noise.
"""

from __future__ import annotations

import random

from .base import ASK, BID, Strategy, limit_order


class MomentumTrader(Strategy):
    __slots__ = ("threshold_bps", "max_qty", "cross_ticks")

    def __init__(self, agent_id: int, rate: float, threshold_bps: float = 4.0,
                 max_qty: int = 120, cross_ticks: int = 4):
        super().__init__(agent_id, rate)
        self.threshold_bps = threshold_bps
        self.max_qty = max_qty
        self.cross_ticks = cross_ticks

    def decide(self, state) -> dict | None:
        if state.ema_slow <= 0 or len(state.prices) < 20:
            return None

        drift_bps = (state.ema_fast - state.ema_slow) / state.ema_slow * 10_000
        if abs(drift_bps) < self.threshold_bps:
            return None

        # Size scales with conviction, capped.
        strength = min(abs(drift_bps) / self.threshold_bps, 4.0)
        qty = max(1, int(self.max_qty * strength / 4 * random.uniform(0.4, 1.0)))

        if drift_bps > 0:
            reference = state.best_ask or state.mid
            return limit_order(self.agent_id, BID, reference + self.cross_ticks, qty)
        reference = state.best_bid or state.mid
        return limit_order(self.agent_id, ASK, reference - self.cross_ticks, qty)
