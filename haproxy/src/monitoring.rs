use anyhow::Result;
use common::{Telemetry, TelemetryEvent};
use reqwest::blocking::Client;
use std::process::Child;
use std::time::Duration;
use tracing::{error, info, warn};

const STATS_URL: &str = "http://localhost:8404/stats;csv";
const CHECK_INTERVAL: Duration = Duration::from_secs(5);

pub fn run_monitoring_loop(mut child: Child, telemetry: &Telemetry) -> Result<()> {
    let pid = child.id();
    info!(pid, "HAProxy started, beginning monitoring");

    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;

    let mut no_primary_alerted = false;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                error!(?status, "HAProxy exited unexpectedly");
                std::process::exit(status.code().unwrap_or(1));
            }
            Ok(None) => {}
            Err(e) => {
                error!(error = %e, "failed to check HAProxy status");
                std::process::exit(1);
            }
        }

        match check_primary_health(&client) {
            Ok(true) => {
                if no_primary_alerted {
                    info!("primary backend recovered");
                }
                no_primary_alerted = false;
            }
            Ok(false) => {
                if !no_primary_alerted {
                    warn!("no healthy primary backend — cluster has no writable master");
                    telemetry.send(TelemetryEvent::NoPrimary {
                        backends: vec!["redis_primary_backend".to_string()],
                    });
                    no_primary_alerted = true;
                }
            }
            Err(e) => {
                warn!(error = %e, "stats poll failed");
            }
        }

        std::thread::sleep(CHECK_INTERVAL);
    }
}

/// Returns true if at least one server in redis_primary_backend is UP.
fn check_primary_health(client: &Client) -> Result<bool> {
    let text = client.get(STATS_URL).send()?.text()?;
    for line in text.lines() {
        // CSV: pxname,svname,...,status,...
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 18 {
            continue;
        }
        let pxname = cols[0];
        let svname = cols[1];
        let status = cols[17];
        if pxname == "redis_primary_backend" && svname != "BACKEND" && status == "UP" {
            return Ok(true);
        }
    }
    Ok(false)
}
