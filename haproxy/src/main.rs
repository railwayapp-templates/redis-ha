mod config;
mod monitoring;
mod nodes;
mod template;

use anyhow::{Context, Result};
use common::{init_logging, Telemetry, TelemetryEvent};
use std::fs;
use std::process::Command;
use tracing::info;

use config::Config;
use monitoring::run_monitoring_loop;
use nodes::parse_nodes;
use template::generate_config;

const CONFIG_FILE: &str = "/usr/local/etc/haproxy/haproxy.cfg";

fn main() -> Result<()> {
    let _guard = init_logging("haproxy-entrypoint");

    let telemetry = Telemetry::from_env("redis-ha");
    let config = Config::from_env()?;
    let nodes = parse_nodes(&config.redis_nodes)?;

    info!(
        nodes = %config.redis_nodes,
        count = nodes.len(),
        health_port = config.health_port,
        "generating HAProxy config"
    );

    let haproxy_cfg = generate_config(&config, &nodes);

    fs::write(CONFIG_FILE, &haproxy_cfg).context("failed to write haproxy.cfg")?;
    info!(path = CONFIG_FILE, "config written");

    for line in haproxy_cfg.lines() {
        info!("  {}", line);
    }

    telemetry.send(TelemetryEvent::NodeStarted {
        node: "haproxy".to_string(),
        role: "edge".to_string(),
    });

    info!("starting HAProxy");

    let child = Command::new("haproxy")
        .arg("-f")
        .arg(CONFIG_FILE)
        .spawn()
        .context("failed to spawn haproxy")?;

    run_monitoring_loop(child, &telemetry)
}
