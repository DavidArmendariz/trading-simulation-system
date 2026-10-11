//! Control-plane listener. Subscribes to `simulation:control` so the admin
//! panel can reset the book without restarting the container.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::{Config, CH_CONTROL};
use crate::types::Command;

#[derive(Debug, Deserialize)]
struct ControlMsg {
    cmd: String,
    #[serde(default)]
    payload: Payload,
}

#[derive(Debug, Default, Deserialize)]
struct Payload {
    #[serde(default)]
    initial_price_ticks: Option<u64>,
}

pub fn run(cfg: Config, tx: Sender<Command>, running: Arc<AtomicBool>) -> Result<()> {
    let client = redis::Client::open(cfg.redis_url.as_str()).context("opening Redis client")?;

    while running.load(Ordering::Relaxed) {
        let mut conn = match client.get_connection() {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "control listener cannot reach Redis, retrying");
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
        };
        let mut pubsub = conn.as_pubsub();
        if let Err(e) = pubsub.subscribe(CH_CONTROL) {
            warn!(error = %e, "control subscribe failed");
            std::thread::sleep(Duration::from_millis(500));
            continue;
        }
        // Bounded so the shutdown flag is observed promptly.
        let _ = pubsub.set_read_timeout(Some(Duration::from_millis(250)));
        info!(channel = CH_CONTROL, "control listener subscribed");

        while running.load(Ordering::Relaxed) {
            let msg = match pubsub.get_message() {
                Ok(m) => m,
                Err(e) if e.is_timeout() => continue,
                Err(e) => {
                    warn!(error = %e, "control stream broken, reconnecting");
                    break;
                }
            };
            let Ok(raw) = msg.get_payload::<String>() else { continue };
            let Ok(control) = serde_json::from_str::<ControlMsg>(&raw) else {
                warn!(payload = %raw, "unparseable control message");
                continue;
            };

            match control.cmd.to_uppercase().as_str() {
                "RESET" => {
                    let price = control
                        .payload
                        .initial_price_ticks
                        .unwrap_or(cfg.initial_price_ticks);
                    let _ = tx.send(Command::Reset {
                        initial_price_ticks: price,
                    });
                }
                // START / STOP / CONFIG_UPDATE are for the agent pool; the
                // engine keeps matching whatever arrives either way.
                _ => {}
            }
        }
    }

    info!("control listener stopped");
    Ok(())
}
