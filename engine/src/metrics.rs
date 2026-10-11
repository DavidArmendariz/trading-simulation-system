//! Lock-free counters shared across threads. The only state that is genuinely
//! shared between threads in this process.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Metrics {
    pub orders_in: AtomicU64,
    pub cancels: AtomicU64,
    pub trades_out: AtomicU64,
    pub volume: AtomicU64,
    /// inbound commands dropped because the matching thread fell behind
    pub orders_dropped: AtomicU64,
    /// snapshots dropped because the publisher fell behind (trades are retried)
    pub snapshots_dropped: AtomicU64,
    pub decode_errors: AtomicU64,
    pub resting_orders: AtomicU64,
    /// resting orders expired because the book hit its cap
    pub expired_orders: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[inline]
    pub fn bump(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
