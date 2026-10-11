//! Core matching engine process.
//!
//! Thread layout, each communicating over bounded crossbeam channels:
//!
//!   ZMQ PULL (ipc)  ─┐
//!   Redis control   ─┴─> matching (owns the book) ──> publisher (Redis)
//!                                                └──> stats (Redis, 1Hz)
//!
//! The book itself is never shared, so the hot path has no locks.

mod book;
mod config;
mod control;
mod ipc;
mod matching;
mod metrics;
mod publisher;
mod types;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cfg = config::Config::from_env();
    info!(?cfg, "starting matching engine");

    let metrics = metrics::Metrics::new();
    let running = Arc::new(AtomicBool::new(true));

    let (cmd_tx, cmd_rx) = crossbeam_channel::bounded(cfg.inbound_capacity);
    let (upd_tx, upd_rx) = crossbeam_channel::bounded(256);

    {
        let running = running.clone();
        ctrlc::set_handler(move || {
            info!("shutdown signal received");
            running.store(false, Ordering::Relaxed);
        })?;
    }

    let mut handles = Vec::new();

    handles.push({
        let (cfg, rx, tx, metrics, running) =
            (cfg.clone(), cmd_rx, upd_tx, metrics.clone(), running.clone());
        std::thread::Builder::new()
            .name("matching".into())
            .spawn(move || matching::run(cfg, rx, tx, metrics, running))?
    });

    handles.push({
        let (endpoint, tx, metrics, running) = (
            cfg.ipc_endpoint.clone(),
            cmd_tx.clone(),
            metrics.clone(),
            running.clone(),
        );
        let flag = running.clone();
        std::thread::Builder::new().name("ipc".into()).spawn(move || {
            if let Err(e) = ipc::run(endpoint, tx, metrics, running) {
                error!(error = %e, "order intake failed");
                flag.store(false, Ordering::Relaxed);
            }
        })?
    });

    handles.push({
        let (cfg, tx, running) = (cfg.clone(), cmd_tx.clone(), running.clone());
        std::thread::Builder::new()
            .name("control".into())
            .spawn(move || {
                if let Err(e) = control::run(cfg, tx, running) {
                    error!(error = %e, "control listener failed");
                }
            })?
    });

    handles.push({
        let (cfg, rx, running) = (cfg.clone(), upd_rx, running.clone());
        let flag = running.clone();
        std::thread::Builder::new()
            .name("publisher".into())
            .spawn(move || {
                if let Err(e) = publisher::run(cfg, rx, running) {
                    error!(error = %e, "publisher failed");
                    flag.store(false, Ordering::Relaxed);
                }
            })?
    });

    handles.push({
        let (cfg, metrics, running) = (cfg.clone(), metrics.clone(), running.clone());
        std::thread::Builder::new()
            .name("stats".into())
            .spawn(move || {
                if let Err(e) = publisher::run_stats(cfg, metrics, running) {
                    error!(error = %e, "stats reporter failed");
                }
            })?
    });

    drop(cmd_tx);
    for h in handles {
        let _ = h.join();
    }
    info!("engine shut down cleanly");
    Ok(())
}
