"""Mean-reversion traders: 10% of the population.

Lean against deviation from a statistical anchor (the slow EMA blended with the
session's opening price). Size grows with the size of the dislocation, so they
provide most of the restoring force after a momentum run.
"""

from __future__ import annotations

import random

from .base import ASK, BID, Strategy, limit_order


class MeanReversionTrader(Strategy):
    __slots__ = ("band_bps", "max_qty", "cross_ticks", "anchor_weight")

    def __init__(self, agent_id: int, rate: float, band_bps: float = 8.0,
                 max_qty: int = 150, cross_ticks: int = 2, anchor_weight: float = 0.5):
        super().__init__(agent_id, rate)
        self.band_bps = band_bps
        self.max_qty = max_qty
        self.cross_ticks = cross_ticks
        self.anchor_weight = anchor_weight

    def decide(self, state) -> dict | None:
        anchor = self.anchor_weight * state.anchor + (1 - self.anchor_weight) * state.ema_slow
        if anchor <= 0:
            return None

        deviation_bps = (state.last - anchor) / anchor * 10_000
        if abs(deviation_bps) < self.band_bps:
            return None

        strength = min(abs(deviation_bps) / self.band_bps, 5.0)
        qty = max(1, int(self.max_qty * strength / 5 * random.uniform(0.5, 1.0)))

        if deviation_bps > 0:
            # Rich: sell into the bid.
            reference = state.best_bid or state.mid
            return limit_order(self.agent_id, ASK, reference - self.cross_ticks, qty)
        reference = state.best_ask or state.mid
        return limit_order(self.agent_id, BID, reference + self.cross_ticks, qty)
