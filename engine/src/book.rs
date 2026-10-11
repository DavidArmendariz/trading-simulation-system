//! Continuous Double Auction limit order book.
//!
//! Price levels live in a `BTreeMap<Ticks, VecDeque<Order>>` so the best price
//! is always at one end of the map, and orders inside a level keep strict FIFO
//! time priority. The book is single-owner: exactly one thread touches it, so
//! there are no locks on the hot path.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::*;

pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// A price level: FIFO queue plus a running total, so building an L2 snapshot
/// is O(depth) instead of O(orders). At 60 snapshots/sec against a book with
/// hundreds of thousands of resting orders, that difference is the whole cost.
#[derive(Debug, Default)]
struct Level {
    orders: VecDeque<Order>,
    total: Qty,
}

impl Level {
    fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
}

pub struct OrderBook {
    bids: BTreeMap<Ticks, Level>,
    asks: BTreeMap<Ticks, Level>,
    /// order id -> (side, price) so cancels are O(1) to locate
    index: HashMap<OrderId, (Side, Ticks)>,
    /// resting order ids in arrival order, used to expire the oldest quotes
    /// once the book hits its cap. Agents never cancel (they never learn their
    /// order ids), so without this the book grows without bound.
    arrivals: VecDeque<OrderId>,
    max_resting: usize,
    next_order_id: OrderId,
    next_trade_id: u64,
    next_seq: u64,
    last_trade_price: Ticks,
    resting_orders: usize,
    expired: u64,
}

impl OrderBook {
    pub fn new(initial_price_ticks: Ticks) -> Self {
        Self::with_capacity(initial_price_ticks, 0)
    }

    /// `max_resting` of 0 means unlimited.
    pub fn with_capacity(initial_price_ticks: Ticks, max_resting: usize) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            index: HashMap::new(),
            arrivals: VecDeque::new(),
            max_resting,
            next_order_id: 1,
            next_trade_id: 1,
            next_seq: 1,
            last_trade_price: initial_price_ticks,
            resting_orders: 0,
            expired: 0,
        }
    }

    pub fn reset(&mut self, initial_price_ticks: Ticks) {
        self.bids.clear();
        self.asks.clear();
        self.index.clear();
        self.arrivals.clear();
        self.resting_orders = 0;
        self.last_trade_price = initial_price_ticks;
    }

    pub fn best_bid(&self) -> Option<Ticks> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<Ticks> {
        self.asks.keys().next().copied()
    }

    pub fn last_price(&self) -> Ticks {
        self.last_trade_price
    }

    pub fn resting_orders(&self) -> usize {
        self.resting_orders
    }

    pub fn expired(&self) -> u64 {
        self.expired
    }

    /// Submit an order, match it against the opposite side, rest any remainder.
    /// Market orders are immediate-or-cancel: an unmatched remainder is dropped.
    pub fn submit(&mut self, req: NewOrder) -> SubmitOutcome {
        let mut out = SubmitOutcome::default();
        if req.qty == 0 {
            return out;
        }

        let taker_id = self.next_order_id;
        self.next_order_id += 1;
        out.order_id = taker_id;

        let mut remaining = req.qty;
        let ts = now_nanos();

        while remaining > 0 {
            let best = match req.side {
                Side::Bid => self.best_ask(),
                Side::Ask => self.best_bid(),
            };
            let Some(best_price) = best else { break };

            // A limit order only crosses while the opposite best is within its limit.
            if req.kind == OrderKind::Limit {
                let crosses = match req.side {
                    Side::Bid => best_price <= req.price_ticks,
                    Side::Ask => best_price >= req.price_ticks,
                };
                if !crosses {
                    break;
                }
            }

            // Lift the level out of the map so the rest of `self` (trade ids,
            // the cancel index, resting count) stays mutably reachable while we
            // fill against it. Re-inserted below if anything is left.
            let mut level = match req.side {
                Side::Bid => &mut self.asks,
                Side::Ask => &mut self.bids,
            }
            .remove(&best_price)
            .expect("best price level must exist");

            while remaining > 0 {
                let Some(maker) = level.orders.front_mut() else { break };
                let fill = remaining.min(maker.remaining);
                maker.remaining -= fill;
                remaining -= fill;
                level.total -= fill;

                out.trades.push(Trade {
                    id: self.next_trade_id,
                    price_ticks: best_price, // the resting order sets the price
                    qty: fill,
                    taker_side: u8::from(req.side),
                    maker_order_id: maker.id,
                    taker_order_id: taker_id,
                    maker_agent: maker.agent_id,
                    taker_agent: req.agent_id,
                    ts_nanos: ts,
                });
                self.next_trade_id += 1;

                if maker.remaining == 0 {
                    let done = level.orders.pop_front().expect("front exists");
                    self.index.remove(&done.id);
                    self.resting_orders -= 1;
                }
            }

            if !level.is_empty() {
                match req.side {
                    Side::Bid => &mut self.asks,
                    Side::Ask => &mut self.bids,
                }
                .insert(best_price, level);
            }
        }

        if let Some(t) = out.trades.last() {
            self.last_trade_price = t.price_ticks;
        }

        // Rest the remainder. Market orders never rest.
        if remaining > 0 && req.kind == OrderKind::Limit {
            let seq = self.next_seq;
            self.next_seq += 1;
            let order = Order {
                id: taker_id,
                agent_id: req.agent_id,
                side: req.side,
                price_ticks: req.price_ticks,
                qty: req.qty,
                remaining,
                seq,
            };
            let side_map = match req.side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let level = side_map.entry(req.price_ticks).or_default();
            level.total += remaining;
            level.orders.push_back(order);
            self.index.insert(taker_id, (req.side, req.price_ticks));
            self.arrivals.push_back(taker_id);
            self.resting_orders += 1;
            out.resting = remaining;
            self.evict_stale();
        }

        out
    }

    /// Expire the oldest quotes once the book exceeds its cap. Entries for
    /// orders that already filled or cancelled are simply skipped, which also
    /// keeps the arrival queue itself bounded.
    fn evict_stale(&mut self) {
        if self.max_resting == 0 {
            return;
        }
        while self.arrivals.len() > self.max_resting {
            let Some(id) = self.arrivals.pop_front() else { break };
            if self.cancel(id) {
                self.expired += 1;
            }
        }
    }

    /// Remove a resting order. Returns true if it was found.
    pub fn cancel(&mut self, order_id: OrderId) -> bool {
        let Some((side, price)) = self.index.remove(&order_id) else {
            return false;
        };
        let side_map = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let Some(level) = side_map.get_mut(&price) else {
            return false;
        };
        let mut removed_qty = 0;
        level.orders.retain(|o| {
            if o.id == order_id {
                removed_qty = o.remaining;
                false
            } else {
                true
            }
        });
        let removed = removed_qty > 0;
        level.total -= removed_qty;
        if level.is_empty() {
            side_map.remove(&price);
        }
        if removed {
            self.resting_orders -= 1;
        }
        removed
    }

    /// Aggregate the top `depth` price levels per side, best price first.
    pub fn l2_snapshot(&self, depth: usize, seq: u64) -> BookSnapshot {
        let bids: Vec<[u64; 2]> = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .map(|(price, level)| [*price, level.total])
            .collect();
        let asks: Vec<[u64; 2]> = self
            .asks
            .iter()
            .take(depth)
            .map(|(price, level)| [*price, level.total])
            .collect();

        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        BookSnapshot {
            seq,
            ts: now_nanos() / 1_000_000,
            bids,
            asks,
            last: self.last_trade_price,
            best_bid,
            best_ask,
            spread: match (best_bid, best_ask) {
                (Some(b), Some(a)) if a >= b => Some(a - b),
                _ => None,
            },
            resting_orders: self.resting_orders,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(agent: AgentId, side: Side, price: Ticks, qty: Qty) -> NewOrder {
        NewOrder {
            agent_id: agent,
            side,
            kind: OrderKind::Limit,
            price_ticks: price,
            qty,
        }
    }

    fn market(agent: AgentId, side: Side, qty: Qty) -> NewOrder {
        NewOrder {
            agent_id: agent,
            side,
            kind: OrderKind::Market,
            price_ticks: 0,
            qty,
        }
    }

    #[test]
    fn resting_order_does_not_trade_on_empty_book() {
        let mut b = OrderBook::new(10_000);
        let out = b.submit(limit(1, Side::Bid, 9_990, 100));
        assert!(out.trades.is_empty());
        assert_eq!(out.resting, 100);
        assert_eq!(b.best_bid(), Some(9_990));
        assert_eq!(b.resting_orders(), 1);
    }

    #[test]
    fn non_crossing_limits_leave_a_spread() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Bid, 9_990, 100));
        b.submit(limit(2, Side::Ask, 10_010, 100));
        assert_eq!(b.best_bid(), Some(9_990));
        assert_eq!(b.best_ask(), Some(10_010));
        assert_eq!(b.l2_snapshot(5, 1).spread, Some(20));
    }

    #[test]
    fn fifo_time_priority_within_a_price_level() {
        let mut b = OrderBook::new(10_000);
        let first = b.submit(limit(1, Side::Ask, 10_000, 50)).order_id;
        let second = b.submit(limit(2, Side::Ask, 10_000, 50)).order_id;

        let out = b.submit(limit(3, Side::Bid, 10_000, 60));
        assert_eq!(out.trades.len(), 2);
        // The earlier order fills completely before the later one gets anything.
        assert_eq!(out.trades[0].maker_order_id, first);
        assert_eq!(out.trades[0].qty, 50);
        assert_eq!(out.trades[1].maker_order_id, second);
        assert_eq!(out.trades[1].qty, 10);
        assert_eq!(b.best_ask(), Some(10_000));
    }

    #[test]
    fn price_priority_beats_time_priority() {
        let mut b = OrderBook::new(10_000);
        let expensive = b.submit(limit(1, Side::Ask, 10_020, 100)).order_id;
        let cheap = b.submit(limit(2, Side::Ask, 10_000, 100)).order_id;

        let out = b.submit(limit(3, Side::Bid, 10_020, 150));
        assert_eq!(out.trades[0].maker_order_id, cheap);
        assert_eq!(out.trades[0].price_ticks, 10_000);
        assert_eq!(out.trades[1].maker_order_id, expensive);
        assert_eq!(out.trades[1].price_ticks, 10_020);
    }

    #[test]
    fn partial_fill_rests_the_remainder() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Ask, 10_000, 30));
        let out = b.submit(limit(2, Side::Bid, 10_000, 100));
        assert_eq!(out.trades.len(), 1);
        assert_eq!(out.resting, 70);
        assert_eq!(b.best_bid(), Some(10_000));
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn sweeping_multiple_levels_clears_them() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Ask, 10_000, 10));
        b.submit(limit(2, Side::Ask, 10_010, 10));
        b.submit(limit(3, Side::Ask, 10_020, 10));

        let out = b.submit(limit(4, Side::Bid, 10_020, 30));
        assert_eq!(out.trades.len(), 3);
        assert_eq!(out.resting, 0);
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.resting_orders(), 0);
        assert_eq!(b.last_price(), 10_020);
    }

    #[test]
    fn market_order_is_ioc_and_drops_the_remainder() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Ask, 10_000, 10));
        let out = b.submit(market(2, Side::Bid, 100));
        assert_eq!(out.trades.len(), 1);
        assert_eq!(out.resting, 0);
        assert_eq!(b.resting_orders(), 0);
    }

    #[test]
    fn cancel_removes_from_the_middle_of_a_queue() {
        let mut b = OrderBook::new(10_000);
        let a = b.submit(limit(1, Side::Bid, 9_990, 10)).order_id;
        let mid = b.submit(limit(2, Side::Bid, 9_990, 10)).order_id;
        let c = b.submit(limit(3, Side::Bid, 9_990, 10)).order_id;

        assert!(b.cancel(mid));
        assert!(!b.cancel(mid), "cancelling twice is a no-op");
        assert_eq!(b.resting_orders(), 2);

        let out = b.submit(limit(4, Side::Ask, 9_990, 20));
        assert_eq!(out.trades[0].maker_order_id, a);
        assert_eq!(out.trades[1].maker_order_id, c);
    }

    #[test]
    fn cancelling_the_only_order_removes_the_price_level() {
        let mut b = OrderBook::new(10_000);
        let id = b.submit(limit(1, Side::Ask, 10_050, 5)).order_id;
        assert!(b.cancel(id));
        assert_eq!(b.best_ask(), None);
        assert!(b.l2_snapshot(5, 1).asks.is_empty());
    }

    #[test]
    fn book_never_stays_crossed() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Ask, 10_000, 100));
        b.submit(limit(2, Side::Bid, 10_050, 100));
        match (b.best_bid(), b.best_ask()) {
            (Some(bid), Some(ask)) => assert!(bid < ask, "book left crossed: {bid} >= {ask}"),
            _ => {}
        }
    }

    #[test]
    fn snapshot_aggregates_quantity_per_level_and_respects_depth() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Bid, 9_990, 10));
        b.submit(limit(2, Side::Bid, 9_990, 15));
        b.submit(limit(3, Side::Bid, 9_980, 5));
        b.submit(limit(4, Side::Bid, 9_970, 5));

        let snap = b.l2_snapshot(2, 42);
        assert_eq!(snap.seq, 42);
        assert_eq!(snap.bids.len(), 2, "depth cap applied");
        assert_eq!(snap.bids[0], [9_990, 25], "best price first, qty aggregated");
        assert_eq!(snap.bids[1], [9_980, 5]);
    }

    #[test]
    fn level_totals_track_fills_and_cancels() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Ask, 10_000, 40));
        let second = b.submit(limit(2, Side::Ask, 10_000, 60)).order_id;
        assert_eq!(b.l2_snapshot(1, 1).asks[0], [10_000, 100]);

        b.submit(limit(3, Side::Bid, 10_000, 50)); // eats 40, then 10 of 60
        assert_eq!(b.l2_snapshot(1, 2).asks[0], [10_000, 50]);

        assert!(b.cancel(second));
        assert!(b.l2_snapshot(1, 3).asks.is_empty());
    }

    #[test]
    fn oldest_quotes_expire_once_the_book_is_full() {
        let mut b = OrderBook::with_capacity(10_000, 3);
        let first = b.submit(limit(1, Side::Bid, 9_990, 10)).order_id;
        b.submit(limit(2, Side::Bid, 9_989, 10));
        b.submit(limit(3, Side::Bid, 9_988, 10));
        assert_eq!(b.resting_orders(), 3);
        assert_eq!(b.expired(), 0);

        b.submit(limit(4, Side::Bid, 9_987, 10));
        assert_eq!(b.resting_orders(), 3, "book stays capped");
        assert_eq!(b.expired(), 1);
        // The oldest quote is the one that left.
        assert!(!b.cancel(first));
        assert_eq!(b.best_bid(), Some(9_989));
    }

    #[test]
    fn filled_orders_do_not_count_against_the_cap() {
        let mut b = OrderBook::with_capacity(10_000, 2);
        b.submit(limit(1, Side::Ask, 10_000, 10));
        b.submit(limit(2, Side::Bid, 10_000, 10)); // fills, rests nothing
        b.submit(limit(3, Side::Bid, 9_990, 10));
        b.submit(limit(4, Side::Bid, 9_989, 10));
        assert_eq!(b.resting_orders(), 2);
        assert_eq!(b.expired(), 0, "matched orders were never resting");
    }

    #[test]
    fn reset_empties_the_book_and_reanchors_price() {
        let mut b = OrderBook::new(10_000);
        b.submit(limit(1, Side::Bid, 9_990, 10));
        b.submit(limit(2, Side::Ask, 10_010, 10));
        b.reset(20_000);
        assert_eq!(b.resting_orders(), 0);
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.last_price(), 20_000);
    }
}
