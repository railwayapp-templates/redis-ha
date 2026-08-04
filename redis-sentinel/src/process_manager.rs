//! Process supervision for Redis and Sentinel subprocesses.
//!
//! Spawns both processes, forwards OS signals to them, and exits the container
//! if either dies — letting Railway's restart policy handle recovery.

use crate::redis_conf::aof_manifest_exists;
use anyhow::{Context, Result};
use common::{Telemetry, TelemetryEvent};
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

/// Parsed slice of `INFO persistence`: (aof enabled, rewrite in progress).
fn aof_status_from_info(info: &str) -> (bool, bool) {
    let flag = |needle: &str| info.lines().any(|line| line.trim_end() == needle);
    (flag("aof_enabled:1"), flag("aof_rewrite_in_progress:1"))
}

/// What the reconcile loop should do next, given the observable state.
#[derive(Debug, PartialEq, Eq)]
enum AofNudge {
    /// The manifest exists — the migration is durably committed.
    Done,
    /// AOF is off; `CONFIG SET appendonly yes` starts the enable + rewrite.
    EnableAof,
    /// A rewrite is running; poll again soon.
    Wait,
    /// AOF is nominally on, nothing is running, and there is no manifest:
    /// the previous rewrite child died. `CONFIG SET` is a no-op here —
    /// only `BGREWRITEAOF` starts a new attempt.
    StartRewrite,
}

fn aof_nudge(manifest_exists: bool, enabled: bool, in_progress: bool) -> AofNudge {
    if manifest_exists {
        AofNudge::Done
    } else if !enabled {
        AofNudge::EnableAof
    } else if in_progress {
        AofNudge::Wait
    } else {
        AofNudge::StartRewrite
    }
}

/// Drive the adopted dataset's AOF migration until it is durably committed.
///
/// `CONFIG SET appendonly yes` only STARTS the migration: Redis rewrites the
/// in-memory dataset into the AOF in a background child and commits by
/// atomically renaming the manifest into place. The child can fail — fork
/// under the memory pressure that follows loading a large RDB, a full volume
/// — and a failed child leaves AOF nominally enabled with nothing durable.
/// Re-issuing CONFIG SET is a no-op in that state; BGREWRITEAOF is what
/// starts a new attempt.
///
/// So this reconciles instead of firing once: wait out the RDB load, then
/// keep nudging Redis until the manifest exists. Runs for as long as it
/// takes — it lives in a background task that dies with the container, and a
/// boot that never commits a manifest re-triggers the migration on the next
/// one. While it retries, the node serves the adopted data with RDB save
/// points as its durability — exactly what the standalone service had before
/// conversion.
pub async fn enable_aof_after_rdb_load(
    port: u16,
    password: &str,
    data_dir: &str,
    telemetry: &Telemetry,
) {
    let url = format!("redis://:{}@127.0.0.1:{}", password, port);
    let client = match Client::open(url) {
        Ok(client) => client,
        Err(err) => {
            error!(error = %err, "failed to build redis client for the AOF migration");
            return;
        }
    };

    // Phase 1: wait out the RDB load. Redis listens before it finishes
    // loading and answers -LOADING to most commands, so the command has to be
    // retried, not just the connection. No deadline: a huge RDB takes as long
    // as it takes, and if redis dies instead, supervise exits the container.
    let mut waiting_logged = false;
    let dbsize: i64 = loop {
        let attempt = async {
            let mut conn = client.get_multiplexed_async_connection().await?;
            redis::cmd("DBSIZE").query_async::<i64>(&mut conn).await
        }
        .await;
        match attempt {
            Ok(n) => break n,
            Err(_) => {
                if !waiting_logged {
                    info!("waiting for redis to finish loading the adopted dataset");
                    waiting_logged = true;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };

    // Phase 2: reconcile until the manifest exists. Healthy waits (rewrite
    // running) poll fast; failure paths back off, since each BGREWRITEAOF
    // attempt forks the whole dataset.
    let mut reported = false;
    let mut backoff = Duration::from_secs(1);
    loop {
        // Ok(true) = progressing (done, just enabled, or a rewrite is running).
        // Ok(false) = a previous rewrite child failed; a new attempt was started.
        let step: Result<bool, redis::RedisError> = async {
            let mut conn = client.get_multiplexed_async_connection().await?;
            let info: String = redis::cmd("INFO")
                .arg("persistence")
                .query_async(&mut conn)
                .await?;
            let (enabled, in_progress) = aof_status_from_info(&info);
            match aof_nudge(aof_manifest_exists(data_dir), enabled, in_progress) {
                AofNudge::Done | AofNudge::Wait => Ok(true),
                AofNudge::EnableAof => {
                    redis::cmd("CONFIG")
                        .arg("SET")
                        .arg("appendonly")
                        .arg("yes")
                        .query_async::<()>(&mut conn)
                        .await?;
                    Ok(true)
                }
                AofNudge::StartRewrite => {
                    redis::cmd("BGREWRITEAOF")
                        .query_async::<String>(&mut conn)
                        .await?;
                    Ok(false)
                }
            }
        }
        .await;

        if aof_manifest_exists(data_dir) {
            info!(
                keys_adopted = dbsize,
                "enabled AOF after loading adopted RDB"
            );
            return;
        }

        match step {
            Ok(true) => backoff = Duration::from_secs(1),
            Ok(false) => {
                warn!("background AOF rewrite did not commit; started a new attempt");
                if !reported {
                    reported = true;
                    telemetry.send(TelemetryEvent::ComponentError {
                        component: "redis-wrapper".to_string(),
                        error: "AOF migration rewrite failing; retrying".to_string(),
                        context: "startup".to_string(),
                    });
                }
                backoff = (backoff * 2).min(Duration::from_secs(300));
            }
            Err(err) => {
                warn!(error = %err, "AOF migration attempt failed; will retry");
                if !reported {
                    reported = true;
                    telemetry.send(TelemetryEvent::ComponentError {
                        component: "redis-wrapper".to_string(),
                        error: format!("AOF migration failing; retrying: {}", err),
                        context: "startup".to_string(),
                    });
                }
                backoff = (backoff * 2).min(Duration::from_secs(300));
            }
        }
        tokio::time::sleep(backoff).await;
    }
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

#[cfg(test)]
mod tests {
    use super::aof_status_from_info;

    // Real `INFO persistence` lines are CRLF-terminated; keep that in the
    // samples so the parser is tested against what Redis actually sends.
    fn info(enabled: u8, in_progress: u8) -> String {
        format!(
            "# Persistence\r\naof_enabled:{}\r\naof_rewrite_in_progress:{}\r\naof_last_bgrewrite_status:ok\r\n",
            enabled, in_progress
        )
    }

    #[test]
    fn parses_enabled_with_rewrite_running() {
        assert_eq!(aof_status_from_info(&info(1, 1)), (true, true));
    }

    #[test]
    fn parses_enabled_and_idle() {
        // The failure-recovery branch: enabled, idle, and (per the caller) no
        // manifest — the state a dead rewrite child leaves behind.
        assert_eq!(aof_status_from_info(&info(1, 0)), (true, false));
    }

    #[test]
    fn parses_disabled() {
        assert_eq!(aof_status_from_info(&info(0, 0)), (false, false));
    }

    #[test]
    fn missing_fields_read_as_disabled() {
        assert_eq!(aof_status_from_info(""), (false, false));
        assert_eq!(
            aof_status_from_info("# Persistence\r\nloading:0\r\n"),
            (false, false)
        );
    }
}

#[cfg(test)]
mod nudge_tests {
    use super::{aof_nudge, AofNudge};

    #[test]
    fn manifest_wins_over_everything() {
        assert_eq!(aof_nudge(true, false, false), AofNudge::Done);
        assert_eq!(aof_nudge(true, true, true), AofNudge::Done);
    }

    #[test]
    fn disabled_gets_enabled() {
        assert_eq!(aof_nudge(false, false, false), AofNudge::EnableAof);
    }

    #[test]
    fn running_rewrite_is_left_alone() {
        assert_eq!(aof_nudge(false, true, true), AofNudge::Wait);
    }

    #[test]
    fn dead_rewrite_child_gets_a_new_attempt() {
        // Enabled, idle, no manifest: the state a failed child leaves behind,
        // where CONFIG SET is a no-op and only BGREWRITEAOF makes progress.
        assert_eq!(aof_nudge(false, true, false), AofNudge::StartRewrite);
    }
}
