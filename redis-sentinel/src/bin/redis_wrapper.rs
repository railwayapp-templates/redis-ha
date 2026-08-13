//! Entrypoint for the Redis + Sentinel container.
//!
//! Responsibilities:
//!   1. Parse and validate configuration from environment variables.
//!   2. Generate redis.conf (always; picks up env-var changes on restart).
//!   3. Generate sentinel.conf only on first boot — Sentinel rewrites it after
//!      failovers so the new master address survives container restarts.
//!   4. Spawn redis-server and (if SENTINEL_ENABLED) redis-sentinel.
//!   5. Run an HTTP health server on HEALTH_PORT for HAProxy to probe.
//!   6. Supervise both processes; exit the container if either dies, and
//!      exit non-zero when the ghost-master watcher asks for a restart so
//!      the boot-time sanitizer can run.

use anyhow::{Context, Result};
use common::{init_logging, RailwayEnv, Telemetry, TelemetryEvent};
use redis_sentinel::{
    boot_role::{
        boot_master_for_this_boot, empty_primary_boot_guard, BootMaster, EmptyPrimaryBoot,
        EMPTY_PRIMARY_GUARD_ENV,
    },
    config::{data_dir_is_on_volume, Config},
    demote_on_shutdown::DemoteTarget,
    ghost_master,
    health_server,
    link_heal,
    process_manager::{enable_aof_after_rdb_load, spawn_redis, spawn_sentinel, supervise},
    quorum,
    redis_conf::{
        generate_redis_conf, needs_rdb_to_aof_migration, persisted_requirepass,
        quarantine_manifestless_aof_dir,
    },
    sentinel_auth,
    sentinel_conf::{conf_requires_auth, generate_sentinel_conf},
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_logging("redis-wrapper");

    let mut config = Config::from_env().context("invalid configuration")?;
    let telemetry = Telemetry::from_env("redis-ha");

    // The password this node actually runs with is the one already persisted
    // on the volume, not whatever REDIS_PASSWORD holds right now. The
    // platform's contract for database services is that editing a credential
    // variable does NOT rotate the live credential (the dashboard's variable
    // editor warns exactly that), and half-applying an edit here is worse
    // than not applying it: sentinel.conf is written once at first boot and
    // owned by Sentinel afterwards, so a regenerated redis.conf carrying a
    // new password strands every Sentinel (outbound auth-pass) and every
    // wrapper watcher (health server, link-heal, quorum-sync) on the old one
    // — a full write outage through /role going 503 on every node, with
    // Redis itself perfectly healthy. Pinning to the persisted requirepass
    // keeps the whole node coherent, and a future orchestrated rotation that
    // goes through `CONFIG SET requirepass` + `CONFIG REWRITE` updates the
    // persisted conf and is honored here on the next boot.
    //
    // A fresh volume (scale-up, conversion) has no conf and takes the
    // variable as-is — so after an unapplied variable edit, a NEW node joins
    // with the new value and cannot authenticate against its peers. The
    // warning below is the durable signal that the variable has drifted from
    // the active password.
    if let Some(active_password) = persisted_requirepass(&config.data_dir) {
        if active_password != config.redis_password {
            tracing::warn!(
                "REDIS_PASSWORD differs from the password this node's dataset already runs \
                 with — keeping the active password; variable edits do not rotate the \
                 database password"
            );
            telemetry.send(TelemetryEvent::ComponentError {
                component: "redis-wrapper".to_string(),
                error: "REDIS_PASSWORD variable drifted from the active password; kept the active one"
                    .to_string(),
                context: "startup".to_string(),
            });
            config.redis_password = active_password;
        }
    }

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

    // At most one container runs against this dataset at a time: wait for a
    // previous container's supervisor to release the volume before reading
    // or writing anything under it (see volume_lock for the overlap
    // rationale). Fail-stop on timeout — the restart policy retries the
    // boot; two engines on one dataset is the outcome that must not happen.
    redis_sentinel::volume_lock::acquire_volume_runtime_lock(&config.data_dir)?;

    // Who is master right now, according to the best record available at
    // boot: the sentinel.conf Sentinel itself rewrites after every failover,
    // or — on a first boot with no local state — the answer of the peer
    // Sentinels this node is joining. Read before the conf is regenerated,
    // because the regenerated conf is what would otherwise re-impose the
    // deploy-time topology on a node Sentinel has since promoted or demoted.
    let resolution = boot_master_for_this_boot(&config).await;

    // Fail-stop guard, checked before anything is written to the volume: an
    // env-primary booting with no loadable dataset into a live cluster whose
    // Sentinels still name it master must not fall back to the env topology.
    // No failover ever repointed the replicas, so they reconnect and ack —
    // min-replicas-to-write is satisfied — and then full-resync the empty
    // dataset: the documented replication wipe Sentinel does not protect
    // against. Exiting leaves the master down instead; the peers fail over
    // to a replica that still holds the data, and this node's next boot
    // joins the new master as a replica through the peer query.
    if empty_primary_boot_guard(&config, &resolution) == EmptyPrimaryBoot::Refuse {
        let error = format!(
            "refusing to boot as an empty master: the peer sentinels name this node ({}) as \
             the current master, but {} holds no loadable dataset — the volume was wiped or \
             replaced. Booting would have every replica full-resync the empty dataset and \
             destroy the cluster's data. This container exits so Sentinel fails over to a \
             data-bearing replica; this node then rejoins as a replica on a later boot. Set \
             {}=false to override.",
            config.private_domain, config.data_dir, EMPTY_PRIMARY_GUARD_ENV
        );
        tracing::error!("{error}");
        telemetry.send(TelemetryEvent::ComponentError {
            component: "redis-wrapper".to_string(),
            error,
            context: "startup".to_string(),
        });
        std::process::exit(1);
    }
    let boot_master = resolution.master;

    // Always regenerate redis.conf so env-var changes take effect on restart.
    let redis_conf_path = format!("{}/redis.conf", config.data_dir);
    let redis_conf = generate_redis_conf(&config, &boot_master);
    fs::write(&redis_conf_path, &redis_conf)
        .context("failed to write redis.conf")?;
    info!(path = %redis_conf_path, "wrote redis.conf");

    // Only write sentinel.conf on first boot — Sentinel owns it after that.
    // First boot is also the ONE moment this node's auth posture is decided
    // (`requirepass` cannot be added to a running Sentinel or a preserved
    // conf), so the generation path probes the peers and posture-matches:
    // auth on (reusing the cluster's REDIS_PASSWORD) for a fresh or
    // already-authed cluster, off when joining a cluster that runs open —
    // see `sentinel_auth` for why a mixed-auth cluster cannot vote.
    let sentinel_conf_path = format!("{}/sentinel.conf", config.data_dir);
    let local_sentinel_requires_auth = if config.sentinel_enabled
        && !Path::new(&sentinel_conf_path).exists()
    {
        let sentinel_password = sentinel_auth::first_boot_sentinel_password(&config).await;
        let sentinel_conf = generate_sentinel_conf(&config, &boot_master, &sentinel_password);
        fs::write(&sentinel_conf_path, &sentinel_conf)
            .context("failed to write sentinel.conf")?;
        fs::set_permissions(&sentinel_conf_path, fs::Permissions::from_mode(0o600))
            .context("failed to set sentinel.conf permissions")?;
        info!(path = %sentinel_conf_path, "wrote sentinel.conf (first boot)");
        conf_requires_auth(&sentinel_conf)
    } else if config.sentinel_enabled {
        info!(path = %sentinel_conf_path, "sentinel.conf exists, preserving");
        fs::read_to_string(&sentinel_conf_path)
            .map(|existing| conf_requires_auth(&existing))
            .unwrap_or(false)
    } else {
        false
    };
    // The password to use for THIS wrapper's own connections to the
    // co-located Sentinel — gated on the file, not on the env-derived
    // default. A preserved conf from before Sentinel auth existed has no
    // `requirepass` (it cannot be retrofitted at runtime — see
    // quorum::ensure_announce_identity's doc comment), and AUTHing against
    // a Sentinel that requires none is a hard connection failure in Redis
    // ("Client sent AUTH, but no password is set"), not a harmless no-op.
    // Trusting the default here would turn this image's rollout onto an
    // already-running unauthenticated cluster into an outage of every
    // local watcher (health server, link-heal, quorum-sync,
    // demote-on-shutdown) on top of not even closing the auth gap on that
    // node. When the conf does require auth, the password IS the cluster's
    // REDIS_PASSWORD — the whole point of the reuse.
    let local_sentinel_password = if local_sentinel_requires_auth {
        config.redis_password.clone()
    } else {
        String::new()
    };

    // Supervised health HTTP server: HAProxy's only signal for routing reads
    // and writes, so an unsupervised task dying here silently pulls this
    // node from BOTH backends forever (see health_server module docs).
    // Mirrors link_heal/quorum's respawn shape rather than a bare spawn.
    health_server::spawn(
        config.health_port,
        config.redis_port,
        config.sentinel_port,
        config.redis_password.clone(),
        config.private_domain.clone(),
        config.redis_master_name.clone(),
        local_sentinel_password.clone(),
        telemetry.clone(),
    );

    // The role this boot actually starts in, which is the resolved one — not
    // the env-declared one it can now contradict.
    let role = match &boot_master {
        BootMaster::SelfIsMaster => "master",
        BootMaster::ReplicaOf(..) => "replica",
        BootMaster::NoLocalState if config.is_primary() => "master",
        BootMaster::NoLocalState => "replica",
    };
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

    // How an in-process watcher asks `supervise` for a restart through the
    // boot path (currently only the ghost-master watcher sends on it).
    let (restart_tx, restart_rx) = tokio::sync::mpsc::channel::<String>(1);

    // Local self-heal for a replica whose replication link is durably down
    // or durably attached to the wrong master — only meaningful with
    // Sentinel colocated, since it is Sentinel's answer that supplies the
    // authoritative fix target.
    if config.sentinel_enabled {
        link_heal::spawn(
            config.data_dir.clone(),
            config.redis_port,
            config.redis_password.clone(),
            config.sentinel_port,
            config.redis_master_name.clone(),
            telemetry.clone(),
            local_sentinel_password.clone(),
        );
        // Keep this Sentinel's odown quorum a majority of the Sentinels it
        // actually knows — and the local Redis's split-brain fence at
        // majority − 1 of them — so scale changes converge without a conf
        // rewrite or redeploy.
        quorum::spawn(
            config.sentinel_port,
            config.redis_port,
            config.redis_password.clone(),
            config.redis_master_name.clone(),
            config.private_domain.clone(),
            local_sentinel_password.clone(),
        );
        // Runtime cure for a ghost-mastered cluster: when quorum consensus
        // durably names a master that is not a live member and no node holds
        // the master role, restart through the boot path so the boot-time
        // sanitizer (dead-world quarantine + role re-resolution) runs.
        ghost_master::spawn(
            &config,
            telemetry,
            local_sentinel_password.clone(),
            restart_tx,
        );
    }

    // What a graceful stop needs to trigger its own failover before
    // signaling either child — see `demote_on_shutdown` for the sequence.
    let demote_target = DemoteTarget {
        redis_port: config.redis_port,
        redis_password: config.redis_password.clone(),
        sentinel_port: config.sentinel_port,
        redis_master_name: config.redis_master_name.clone(),
        sentinel_enabled: config.sentinel_enabled,
        local_sentinel_password,
    };

    // Block until a process exits, we receive a signal, or a watcher asks
    // for a restart through the boot path.
    supervise(redis_proc, sentinel_proc, demote_target, restart_rx).await
}
