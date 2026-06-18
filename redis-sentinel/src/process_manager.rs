//! Process supervision for Redis and Sentinel subprocesses.
//!
//! Spawns both processes, forwards OS signals to them, and exits the container
//! if either dies — letting Railway's restart policy handle recovery.

use anyhow::{Context, Result};
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
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
