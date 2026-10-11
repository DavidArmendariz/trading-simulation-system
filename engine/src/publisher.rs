//! Outbound fan-out to Redis Pub/Sub. Serializes what the matching thread
//! already coalesced; does no aggregation of its own.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::Receiver;
use redis::Commands;
use serde::Serialize;
use tracing::{error, info, warn};

use crate::config::{Config, CH_BOOK, CH_STATS, CH_TRADES};
use crate::metrics::Metrics;
use crate::types::{Trade, Update};

#[derive(Serialize)]
struct TradeBatch<'a> {
    seq: u64,
    ts: u64,
    symbol: &'a str,
    trades: &'a [Trade],
}

#[derive(Serialize)]
struct Stats<'a> {
    ts: u64,
    symbol: &'a str,
    orders_in: u64,
    cancels: u64,
    trades_out: u64,
    volume: u64,
    orders_per_sec: u64,
    trades_per_sec: u64,
    resting_orders: u64,
    expired_orders: u64,
    orders_dropped: u64,
    snapshots_dropped: u64,
    decode_errors: u64,
}

fn connect(url: &str) -> Result<redis::Connection> {
    let client = redis::Client::open(url).context("opening Redis client")?;
    // Retry: Redis may still be accepting connections when we boot.
    for attempt in 1..=30 {
        match client.get_connection() {
            Ok(conn) => return Ok(conn),
            Err(e) if attempt < 30 => {
                warn!(attempt, error = %e, "Redis not ready, retrying");
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => return Err(e).context("connecting to Redis"),
        }
    }
    unreachable!()
}

pub fn run(cfg: Config, rx: Receiver<Update>, running: Arc<AtomicBool>) -> Result<()> {
    let mut conn = connect(&cfg.redis_url)?;
    info!("publisher connected to Redis");

    while running.load(Ordering::Relaxed) {
        let Ok(update) = rx.recv_timeout(Duration::from_millis(250)) else {
            continue;
        };

        match serde_json::to_string(&update.snapshot) {
            Ok(payload) => {
                if let Err(e) = conn.publish::<_, _, ()>(CH_BOOK, payload) {
                    error!(error = %e, "publishing book snapshot failed");
                    conn = connect(&cfg.redis_url)?;
                    continue;
                }
            }
            Err(e) => error!(error = %e, "serializing snapshot failed"),
        }

        if !update.trades.is_empty() {
            let batch = TradeBatch {
                seq: update.snapshot.seq,
                ts: update.snapshot.ts,
                symbol: &cfg.symbol,
                trades: &update.trades,
            };
            match serde_json::to_string(&batch) {
                Ok(payload) => {
                    if let Err(e) = conn.publish::<_, _, ()>(CH_TRADES, payload) {
                        error!(error = %e, "publishing trades failed");
                        conn = connect(&cfg.redis_url)?;
                    }
                }
                Err(e) => error!(error = %e, "serializing trades failed"),
            }
        }
    }

    info!("publisher stopped");
    Ok(())
}

/// Throughput reporter. Separate connection so a slow stats publish can never
/// delay market data.
pub fn run_stats(cfg: Config, metrics: Arc<Metrics>, running: Arc<AtomicBool>) -> Result<()> {
    let mut conn = connect(&cfg.redis_url)?;
    let mut prev_orders = 0u64;
    let mut prev_trades = 0u64;

    while running.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_secs(1));

        let orders_in = Metrics::get(&metrics.orders_in);
        let trades_out = Metrics::get(&metrics.trades_out);
        let stats = Stats {
            ts: crate::book::now_nanos() / 1_000_000,
            symbol: &cfg.symbol,
            orders_in,
            cancels: Metrics::get(&metrics.cancels),
            trades_out,
            volume: Metrics::get(&metrics.volume),
            orders_per_sec: orders_in.saturating_sub(prev_orders),
            trades_per_sec: trades_out.saturating_sub(prev_trades),
            resting_orders: Metrics::get(&metrics.resting_orders),
            expired_orders: Metrics::get(&metrics.expired_orders),
            orders_dropped: Metrics::get(&metrics.orders_dropped),
            snapshots_dropped: Metrics::get(&metrics.snapshots_dropped),
            decode_errors: Metrics::get(&metrics.decode_errors),
        };
        prev_orders = orders_in;
        prev_trades = trades_out;

        if let Ok(payload) = serde_json::to_string(&stats) {
            if let Err(e) = conn.publish::<_, _, ()>(CH_STATS, payload) {
                error!(error = %e, "publishing stats failed");
                conn = connect(&cfg.redis_url)?;
            }
        }
    }
    Ok(())
}
