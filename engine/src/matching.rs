//! The matching thread. Sole owner of the `OrderBook`.
//!
//! Consumes commands as fast as they arrive and, on a fixed tick, hands the
//! publisher a coalesced L2 snapshot plus every trade printed since the last
//! tick. Coalescing is deliberate: at 50k orders/sec a snapshot per event would
//! saturate Redis and the browser, and the UI cannot use more than frame rate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TrySendError};
use tracing::{info, warn};

use crate::book::OrderBook;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::types::*;

pub fn run(
    cfg: Config,
    rx: Receiver<Command>,
    tx: Sender<Update>,
    metrics: Arc<Metrics>,
    running: Arc<AtomicBool>,
) {
    let mut book = OrderBook::with_capacity(cfg.initial_price_ticks, cfg.max_resting);
    let period = Duration::from_micros(1_000_000 / cfg.snapshot_hz);
    let ticker = crossbeam_channel::tick(period);
    let mut trade_batch: Vec<Trade> = Vec::with_capacity(8192);
    let mut seq: u64 = 0;

    info!(
        depth = cfg.book_depth,
        hz = cfg.snapshot_hz,
        max_resting = cfg.max_resting,
        "matching engine running"
    );

    while running.load(Ordering::Relaxed) {
        crossbeam_channel::select! {
            recv(rx) -> msg => match msg {
                Ok(Command::New(order)) => {
                    let out = book.submit(order);
                    Metrics::bump(&metrics.orders_in, 1);
                    if !out.trades.is_empty() {
                        Metrics::bump(&metrics.trades_out, out.trades.len() as u64);
                        Metrics::bump(&metrics.volume, out.trades.iter().map(|t| t.qty).sum());
                        trade_batch.extend(out.trades);
                    }
                }
                Ok(Command::Cancel(id)) => {
                    if book.cancel(id) {
                        Metrics::bump(&metrics.cancels, 1);
                    }
                }
                Ok(Command::Reset { initial_price_ticks }) => {
                    info!(initial_price_ticks, "book reset");
                    book.reset(initial_price_ticks);
                    trade_batch.clear();
                }
                Err(_) => break, // intake and control threads both gone
            },
            recv(ticker) -> _ => {
                seq += 1;
                let snapshot = book.l2_snapshot(cfg.book_depth, seq);
                metrics
                    .resting_orders
                    .store(book.resting_orders() as u64, Ordering::Relaxed);
                metrics
                    .expired_orders
                    .store(book.expired(), Ordering::Relaxed);
                let trades = std::mem::take(&mut trade_batch);
                match tx.try_send(Update { snapshot, trades }) {
                    Ok(()) => {}
                    Err(TrySendError::Full(rejected)) => {
                        // Drop the stale snapshot but keep the trades: the
                        // gateway persists them, so losing prints loses data.
                        trade_batch = rejected.trades;
                        Metrics::bump(&metrics.snapshots_dropped, 1);
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        warn!("publisher gone, stopping matching");
                        break;
                    }
                }
            }
        }
    }

    info!("matching engine stopped");
}
