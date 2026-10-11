//! Environment-driven configuration. Every value has a working default so the
//! binary runs outside Docker for local testing.

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub ipc_endpoint: String,
    pub redis_url: String,
    pub symbol: String,
    pub book_depth: usize,
    pub snapshot_hz: u64,
    pub initial_price_ticks: u64,
    pub inbound_capacity: usize,
    /// Cap on resting orders. Agents never cancel, so the oldest quotes expire
    /// once the book reaches this size. 0 disables the cap.
    pub max_resting: usize,
}

fn var<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            ipc_endpoint: env::var("IPC_ENDPOINT")
                .unwrap_or_else(|_| "ipc:///tmp/ipc/orders.ipc".to_string()),
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            symbol: env::var("SIM_SYMBOL").unwrap_or_else(|_| "SIMX".to_string()),
            book_depth: var("BOOK_DEPTH", 15usize),
            snapshot_hz: var("SNAPSHOT_HZ", 60u64).clamp(1, 240),
            initial_price_ticks: var("INITIAL_PRICE_TICKS", 10_000u64),
            inbound_capacity: var("INBOUND_CAPACITY", 262_144usize),
            max_resting: var("MAX_RESTING", 200_000usize),
        }
    }
}

pub const CH_BOOK: &str = "market:book";
pub const CH_TRADES: &str = "market:trades";
pub const CH_STATS: &str = "market:stats";
pub const CH_CONTROL: &str = "simulation:control";
