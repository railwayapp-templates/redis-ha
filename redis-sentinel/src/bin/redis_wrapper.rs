//! Entrypoint for the Redis + Sentinel container.
//!
//! Responsibilities:
//!   1. Parse and validate configuration from environment variables.
//!   2. Generate redis.conf (always; picks up env-var changes on restart).
//!   3. Generate sentinel.conf only on first boot — Sentinel rewrites it after
//!      failovers so the new master address survives container restarts.
//!   4. Spawn redis-server and (if SENTINEL_ENABLED) redis-sentinel.
//!   5. Run an HTTP health server on HEALTH_PORT for HAProxy to probe.
//!   6. Supervise both processes; exit the container if either dies.

use anyhow::{Context, Result};
use common::{init_logging, RailwayEnv, Telemetry, TelemetryEvent};
use redis_sentinel::{
    config::Config,
    health_server::run_health_server,
    process_manager::{spawn_redis, spawn_sentinel, supervise},
    redis_conf::generate_redis_conf,
    sentinel_conf::generate_sentinel_conf,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_logging("redis-wrapper");

    let config = Config::from_env().context("invalid configuration")?;
    let telemetry = Telemetry::from_env("redis-ha");

    info!(
        is_primary = config.is_primary(),
        sentinel_enabled = config.sentinel_enabled,
        redis_port = config.redis_port,
        "starting redis-wrapper"
    );

    if RailwayEnv::is_railway() {
        let mount = std::env::var("RAILWAY_VOLUME_MOUNT_PATH").unwrap_or_default();
        if mount != config.data_dir {
            tracing::error!(
                expected = %config.data_dir,
                got = %mount,
                "volume not mounted at expected path"
            );
            telemetry.send(TelemetryEvent::ComponentError {
                component: "redis-wrapper".to_string(),
                error: format!("volume at {} instead of {}", mount, config.data_dir),
                context: "startup".to_string(),
            });
            std::process::exit(1);
        }
    }

    fs::create_dir_all(&config.data_dir)
        .context("failed to create data directory")?;

    // Always regenerate redis.conf so env-var changes take effect on restart.
    let redis_conf_path = format!("{}/redis.conf", config.data_dir);
    let redis_conf = generate_redis_conf(&config);
    fs::write(&redis_conf_path, &redis_conf)
        .context("failed to write redis.conf")?;
    info!(path = %redis_conf_path, "wrote redis.conf");

    // Only write sentinel.conf on first boot — Sentinel owns it after that.
    let sentinel_conf_path = format!("{}/sentinel.conf", config.data_dir);
    if config.sentinel_enabled && !Path::new(&sentinel_conf_path).exists() {
        let sentinel_conf = generate_sentinel_conf(&config);
        fs::write(&sentinel_conf_path, &sentinel_conf)
            .context("failed to write sentinel.conf")?;
        fs::set_permissions(&sentinel_conf_path, fs::Permissions::from_mode(0o600))
            .context("failed to set sentinel.conf permissions")?;
        info!(path = %sentinel_conf_path, "wrote sentinel.conf (first boot)");
    } else if config.sentinel_enabled {
        info!(path = %sentinel_conf_path, "sentinel.conf exists, preserving");
    }

    // Start health HTTP server (non-blocking — runs in background)
    let hp = config.health_port;
    let rp = config.redis_port;
    let sp = config.sentinel_port;
    let pw = config.redis_password.clone();
    let domain = config.private_domain.clone();
    tokio::spawn(async move {
        run_health_server(hp, rp, sp, pw, domain).await;
    });

    let role = if config.is_primary() { "master" } else { "replica" };
    telemetry.send(TelemetryEvent::NodeStarted {
        node: RailwayEnv::private_domain(),
        role: role.to_string(),
    });

    // Spawn Redis
    let redis_proc = spawn_redis(&config.data_dir, config.redis_port).await?;

    // Spawn Sentinel (colocated)
    let sentinel_proc = if config.sentinel_enabled {
        Some(spawn_sentinel(&config.data_dir).await?)
    } else {
        None
    };

    // Block until a process exits or we receive a signal
    supervise(redis_proc, sentinel_proc).await
}
