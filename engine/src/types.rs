//! Wire and domain types. Every price is an integer tick (cent); every
//! quantity is an integer share. No floating point ever enters the engine.

use serde::{Deserialize, Serialize};

pub type Ticks = u64;
pub type Qty = u64;
pub type OrderId = u64;
pub type AgentId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
}

impl From<Side> for u8 {
    fn from(s: Side) -> u8 {
        match s {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }
}

impl TryFrom<u8> for Side {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Side::Bid),
            1 => Ok(Side::Ask),
            other => Err(format!("invalid side {other}")),
        }
    }
}

/// Inbound message decoded from MessagePack on the ZeroMQ PULL socket.
/// Field names are single characters to keep the wire small at 50k msg/s.
#[derive(Debug, Clone, Deserialize)]
pub struct InboundMsg {
    /// kind: 0 = limit, 1 = market, 2 = cancel
    #[serde(rename = "k")]
    pub kind: u8,
    #[serde(rename = "a")]
    pub agent_id: AgentId,
    #[serde(rename = "s")]
    pub side: u8,
    #[serde(rename = "p", default)]
    pub price_ticks: Ticks,
    #[serde(rename = "q", default)]
    pub qty: Qty,
    /// only meaningful for cancels
    #[serde(rename = "o", default)]
    pub order_id: OrderId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    Limit,
    Market,
}

/// A request to mutate the book. Produced by the IPC thread and the control
/// thread, consumed by the single matching thread.
#[derive(Debug, Clone)]
pub enum Command {
    New(NewOrder),
    Cancel(OrderId),
    /// Wipe the book and re-anchor the reference price.
    Reset { initial_price_ticks: Ticks },
}

#[derive(Debug, Clone, Copy)]
pub struct NewOrder {
    pub agent_id: AgentId,
    pub side: Side,
    pub kind: OrderKind,
    pub price_ticks: Ticks,
    pub qty: Qty,
}

/// A resting order. `remaining` shrinks as it is filled; it leaves the book at 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub id: OrderId,
    pub agent_id: AgentId,
    pub side: Side,
    pub price_ticks: Ticks,
    pub qty: Qty,
    pub remaining: Qty,
    /// arrival sequence, the tiebreaker behind price priority
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Trade {
    pub id: u64,
    #[serde(rename = "price")]
    pub price_ticks: Ticks,
    pub qty: Qty,
    /// 0 = buyer was the aggressor, 1 = seller was
    #[serde(rename = "side")]
    pub taker_side: u8,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub maker_agent: AgentId,
    pub taker_agent: AgentId,
    pub ts_nanos: u64,
}

/// Aggregated Level 2 depth, top N levels per side, best price first.
#[derive(Debug, Clone, Serialize)]
pub struct BookSnapshot {
    pub seq: u64,
    pub ts: u64,
    pub bids: Vec<[u64; 2]>,
    pub asks: Vec<[u64; 2]>,
    pub last: Ticks,
    pub best_bid: Option<Ticks>,
    pub best_ask: Option<Ticks>,
    pub spread: Option<Ticks>,
    pub resting_orders: usize,
}

/// What the matching thread hands to the publisher thread, once per tick.
#[derive(Debug)]
pub struct Update {
    pub snapshot: BookSnapshot,
    pub trades: Vec<Trade>,
}

/// Result of submitting one order.
#[derive(Debug, Default)]
pub struct SubmitOutcome {
    pub order_id: OrderId,
    pub trades: Vec<Trade>,
    pub resting: Qty,
}
