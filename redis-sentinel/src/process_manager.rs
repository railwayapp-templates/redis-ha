//! Process supervision for Redis and Sentinel subprocesses.
//!
//! Spawns both processes, forwards OS signals to them, and exits the container
//! if either dies — letting Railway's restart policy handle recovery.

use anyhow::{Context, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use redis::Client;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info, warn};

pub async fn spawn_redis(data_dir: &str, _redis_port: u16) -> Result<Child> {
    let conf = format!("{}/redis.conf", data_dir);
    info!(conf, "starting redis-server");

    Command::new("redis-server")
        .arg(&conf)
        .kill_on_drop(false)
        .spawn()
        .context("failed to spawn redis-server")
}

/// Turn AOF on after Redis has loaded an adopted RDB.
///
/// `CONFIG SET appendonly yes` makes Redis rewrite the AOF from the dataset it
/// currently holds, so the adopted keys end up in the AOF instead of being
/// abandoned. `CONFIG REWRITE` then persists `appendonly yes` into redis.conf
/// — though the wrapper regenerates that file on every boot anyway, and by
/// then `appendonlydir` exists so the migration no longer triggers.
///
/// Best-effort: a failure here leaves a running Redis serving the adopted data
/// with AOF off, which is recoverable. Killing the node would not be.
pub async fn enable_aof_after_rdb_load(port: u16, password: &str) -> Result<()> {
    let url = format!("redis://:{}@127.0.0.1:{}", password, port);
    let client = Client::open(url).context("failed to build redis client")?;

    // Redis listens BEFORE it finishes loading the RDB and answers -LOADING
    // to most commands (DBSIZE included) until the load completes, so the
    // command has to be retried, not just the connection. The deadline is
    // sized for multi-gigabyte RDBs; the caller runs this in the background,
    // so nothing else waits on it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30 * 60);
    let mut waiting_logged = false;
    let dbsize: i64 = loop {
        let attempt = async {
            let mut conn = client.get_multiplexed_async_connection().await?;
            redis::cmd("DBSIZE").query_async::<i64>(&mut conn).await
        }
        .await;
        match attempt {
            Ok(n) => break n,
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow::Error::new(err)
                        .context("redis did not finish loading the dataset in time"));
                }
                if !waiting_logged {
                    info!("waiting for redis to finish loading the adopted dataset");
                    waiting_logged = true;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to reconnect after the dataset loaded")?;

    redis::cmd("CONFIG")
        .arg("SET")
        .arg("appendonly")
        .arg("yes")
        .query_async::<()>(&mut conn)
        .await
        .context("CONFIG SET appendonly yes failed")?;

    redis::cmd("CONFIG")
        .arg("REWRITE")
        .query_async::<()>(&mut conn)
        .await
        .context("CONFIG REWRITE failed")?;

    info!(
        keys_adopted = dbsize,
        "enabled AOF after loading adopted RDB"
    );
    Ok(())
}

pub async fn spawn_sentinel(data_dir: &str) -> Result<Child> {
    let conf = format!("{}/sentinel.conf", data_dir);
    info!(conf, "starting redis-sentinel");

    Command::new("redis-server")
        .arg(&conf)
        .arg("--sentinel")
        .kill_on_drop(false)
        .spawn()
        .context("failed to spawn redis-sentinel")
}

/// Run the supervisor loop.
///
/// Waits for either child to exit or for a termination signal. On SIGTERM/SIGINT
/// both children are forwarded the signal and we wait briefly before exiting.
pub async fn supervise(
    mut redis: Child,
    mut sentinel: Option<Child>,
) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    let redis_pid = redis.id().map(|id| Pid::from_raw(id as i32));
    let sentinel_pid = sentinel.as_ref()
        .and_then(|s| s.id())
        .map(|id| Pid::from_raw(id as i32));

    loop {
        tokio::select! {
            status = redis.wait() => {
                match status {
                    Ok(s) => error!(code = s.code(), "redis-server exited unexpectedly"),
                    Err(e) => error!(error = %e, "redis-server wait error"),
                }
                // Kill sentinel before exiting
                if let (Some(ref mut s), Some(pid)) = (&mut sentinel, sentinel_pid) {
                    let _ = signal::kill(pid, Signal::SIGTERM);
                    let _ = s.wait().await;
                }
                std::process::exit(1);
            }

            status = async {
                match sentinel.as_mut() {
                    Some(s) => s.wait().await,
                    None => std::future::pending().await,
                }
            } => {
                match status {
                    Ok(s) => error!(code = s.code(), "redis-sentinel exited unexpectedly"),
                    Err(e) => error!(error = %e, "redis-sentinel wait error"),
                }
                // Kill Redis before exiting
                if let Some(pid) = redis_pid {
                    let _ = signal::kill(pid, Signal::SIGTERM);
                    let _ = redis.wait().await;
                }
                std::process::exit(1);
            }

            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                graceful_shutdown(redis_pid, sentinel_pid, &mut redis, &mut sentinel).await;
                std::process::exit(0);
            }

            _ = sigint.recv() => {
                info!("received SIGINT, shutting down");
                graceful_shutdown(redis_pid, sentinel_pid, &mut redis, &mut sentinel).await;
                std::process::exit(0);
            }
        }
    }
}

async fn graceful_shutdown(
    redis_pid: Option<Pid>,
    sentinel_pid: Option<Pid>,
    redis: &mut Child,
    sentinel: &mut Option<Child>,
) {
    // Sentinel first so it doesn't trigger spurious failovers
    if let (Some(ref mut s), Some(pid)) = (sentinel, sentinel_pid) {
        info!("sending SIGTERM to redis-sentinel");
        let _ = signal::kill(pid, Signal::SIGTERM);
        tokio::select! {
            _ = s.wait() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                warn!("redis-sentinel did not exit in time, killing");
                let _ = s.kill().await;
            }
        }
    }

    if let Some(pid) = redis_pid {
        info!("sending SIGTERM to redis-server");
        let _ = signal::kill(pid, Signal::SIGTERM);
        tokio::select! {
            _ = redis.wait() => {}
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                warn!("redis-server did not exit in time, killing");
                let _ = redis.kill().await;
            }
        }
    }
}
