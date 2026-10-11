"""Per-process view of the market.

One instance per worker process, fed by a single Redis subscriber task and read
by every agent in that process. 2,000 individual Redis subscriptions would be
the bottleneck long before the strategies were.
"""

from __future__ import annotations

from collections import deque


class MarketState:
    __slots__ = (
        "last",
        "best_bid",
        "best_ask",
        "bids",
        "asks",
        "seq",
        "ema_fast",
        "ema_slow",
        "anchor",
        "prices",
        "_alpha_fast",
        "_alpha_slow",
    )

    def __init__(self, initial_price: int, ema_fast_span: int = 20, ema_slow_span: int = 120):
        self.last = initial_price
        self.best_bid: int | None = None
        self.best_ask: int | None = None
        self.bids: list[list[int]] = []
        self.asks: list[list[int]] = []
        self.seq = 0
        self.ema_fast = float(initial_price)
        self.ema_slow = float(initial_price)
        self.anchor = float(initial_price)
        self.prices: deque[int] = deque(maxlen=256)
        self._alpha_fast = 2.0 / (ema_fast_span + 1)
        self._alpha_slow = 2.0 / (ema_slow_span + 1)

    @property
    def mid(self) -> int:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) // 2
        return self.last

    @property
    def spread(self) -> int:
        if self.best_bid is not None and self.best_ask is not None:
            return max(1, self.best_ask - self.best_bid)
        return 2

    def on_book(self, msg: dict) -> None:
        self.seq = msg.get("seq", 0)
        self.best_bid = msg.get("best_bid")
        self.best_ask = msg.get("best_ask")
        self.bids = msg.get("bids") or []
        self.asks = msg.get("asks") or []
        last = msg.get("last")
        if last:
            self.last = last

    def on_trades(self, trades: list[dict]) -> None:
        for t in trades:
            price = t["price"]
            self.last = price
            self.prices.append(price)
            self.ema_fast += self._alpha_fast * (price - self.ema_fast)
            self.ema_slow += self._alpha_slow * (price - self.ema_slow)
