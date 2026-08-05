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
    config::{data_dir_is_on_volume, Config},
    health_server::run_health_server,
    link_heal,
    process_manager::{enable_aof_after_rdb_load, spawn_redis, spawn_sentinel, supervise},
    redis_conf::{
        generate_redis_conf, needs_rdb_to_aof_migration, quarantine_manifestless_aof_dir,
    },
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

    // The data dir now follows the volume (Config::resolve_data_dir), so the
    // two only diverge when DATA_DIR was set explicitly. A subdirectory of the
    // mount is a legitimate choice; a path outside it is not — the data would
    // live on the container filesystem and vanish on redeploy.
    //
    // This used to `exit(1)` on any divergence, which turned a conversion that
    // merely adopted a root mounted somewhere other than /data into a
    // crashlooping primary. Warn instead: refusing to boot never protected the
    // data, it only removed the node.
    if RailwayEnv::is_railway() {
        let mount = std::env::var("RAILWAY_VOLUME_MOUNT_PATH").unwrap_or_default();
        if !data_dir_is_on_volume(&config.data_dir, &mount) {
            tracing::warn!(
                data_dir = %config.data_dir,
                volume_mount_path = %mount,
                "data directory is outside the mounted volume — data will not persist across redeploys"
            );
            telemetry.send(TelemetryEvent::ComponentError {
                component: "redis-wrapper".to_string(),
                error: format!(
                    "data dir {} is outside volume mount {}",
                    config.data_dir, mount
                ),
                context: "startup".to_string(),
            });
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

    // Captured before spawning: once Redis is up it writes its own
    // appendonlydir, so the check would no longer be true.
    let adopting_rdb = needs_rdb_to_aof_migration(&config.data_dir);

    if adopting_rdb {
        match quarantine_manifestless_aof_dir(&config.data_dir) {
            Ok(Some(orphaned)) => tracing::warn!(
                to = %orphaned.display(),
                "moved manifest-less appendonlydir aside before AOF migration"
            ),
            Ok(None) => {}
            Err(err) => tracing::error!(
                error = %err,
                "failed to move manifest-less appendonlydir aside"
            ),
        }
    }

    // Spawn Redis
    let redis_proc = spawn_redis(&config.data_dir, config.redis_port).await?;

    // redis.conf carries `appendonly no` for this boot so the adopted RDB is
    // what Redis loads; AOF is turned back on as soon as the load finishes.
    // Runs in the background: a large RDB takes minutes to load, and neither
    // Sentinel startup nor signal handling (supervise) should wait on it —
    // this keeps the startup sequence identical to a non-adopting boot.
    if adopting_rdb {
        info!("adopted dataset has an RDB and no AOF — enabling AOF once it finishes loading");
        let redis_port = config.redis_port;
        let redis_password = config.redis_password.clone();
        let data_dir = config.data_dir.clone();
        let telemetry = telemetry.clone();
        tokio::spawn(async move {
            enable_aof_after_rdb_load(redis_port, &redis_password, &data_dir, &telemetry).await;
        });
    }

    // Spawn Sentinel (colocated)
    let sentinel_proc = if config.sentinel_enabled {
        Some(spawn_sentinel(&config.data_dir).await?)
    } else {
        None
    };

    // Local self-heal for a replica whose link to the master is durably down
    // — only meaningful with Sentinel colocated, since it is Sentinel's
    // answer that supplies the authoritative fix target.
    if config.sentinel_enabled {
        link_heal::spawn(
            config.data_dir.clone(),
            config.redis_port,
            config.redis_password.clone(),
            config.sentinel_port,
            config.redis_master_name.clone(),
            telemetry,
        );
    }

    // Block until a process exits or we receive a signal
    supervise(redis_proc, sentinel_proc).await
}
