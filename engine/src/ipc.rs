//! Inbound order ingestion over a ZeroMQ PULL socket bound to a Unix domain
//! socket on a volume shared with the agent containers.
//!
//! Runs on its own OS thread with a blocking socket and a receive timeout so
//! shutdown stays responsive. Decoded commands go to the matching thread over a
//! bounded channel; if that channel is full we drop and count rather than block
//! the socket, because backpressure here would silently stall every agent.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use crossbeam_channel::{Sender, TrySendError};
use tracing::{error, info, warn};

use crate::metrics::Metrics;
use crate::types::*;

/// ZeroMQ refuses to bind an `ipc://` endpoint whose socket file already exists,
/// which happens after an unclean shutdown on a persistent volume.
fn prepare_ipc_path(endpoint: &str) -> Result<()> {
    let Some(path) = endpoint.strip_prefix("ipc://") else {
        return Ok(());
    };
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating IPC directory {}", parent.display()))?;
    }
    if path.exists() {
        warn!(path = %path.display(), "removing stale IPC socket");
        std::fs::remove_file(path)
            .with_context(|| format!("removing stale socket {}", path.display()))?;
    }
    Ok(())
}

fn decode(bytes: &[u8]) -> Result<Command> {
    let msg: InboundMsg = rmp_serde::from_slice(bytes)?;
    let side = Side::try_from(msg.side).map_err(anyhow::Error::msg)?;
    Ok(match msg.kind {
        0 => Command::New(NewOrder {
            agent_id: msg.agent_id,
            side,
            kind: OrderKind::Limit,
            price_ticks: msg.price_ticks,
            qty: msg.qty,
        }),
        1 => Command::New(NewOrder {
            agent_id: msg.agent_id,
            side,
            kind: OrderKind::Market,
            price_ticks: 0,
            qty: msg.qty,
        }),
        2 => Command::Cancel(msg.order_id),
        other => anyhow::bail!("unknown message kind {other}"),
    })
}

pub fn run(
    endpoint: String,
    tx: Sender<Command>,
    metrics: Arc<Metrics>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    prepare_ipc_path(&endpoint)?;

    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::PULL).context("creating PULL socket")?;
    socket.set_rcvhwm(1_000_000)?;
    // Bounded wait so the loop can observe the shutdown flag.
    socket.set_rcvtimeo(200)?;
    socket
        .bind(&endpoint)
        .with_context(|| format!("binding PULL socket to {endpoint}"))?;
    info!(%endpoint, "order intake bound");

    while running.load(Ordering::Relaxed) {
        match socket.recv_bytes(0) {
            Ok(bytes) => match decode(&bytes) {
                Ok(cmd) => match tx.try_send(cmd) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        Metrics::bump(&metrics.orders_dropped, 1);
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        error!("matching thread gone, stopping intake");
                        break;
                    }
                },
                Err(e) => {
                    Metrics::bump(&metrics.decode_errors, 1);
                    if Metrics::get(&metrics.decode_errors) % 1000 == 1 {
                        warn!(error = %e, "failed to decode inbound message");
                    }
                }
            },
            Err(zmq::Error::EAGAIN) => continue,
            Err(zmq::Error::EINTR) => continue,
            Err(e) => {
                error!(error = %e, "PULL recv failed");
                break;
            }
        }
    }

    info!("order intake stopped");
    Ok(())
}
