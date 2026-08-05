//! Background task that keeps a replica pointed at the CURRENT master.
//!
//! `REPLICA_OF` only sets what this node replicates from at BOOT — Redis never
//! re-reads it. After a failover, a node stays pointed at whatever host it
//! booted with unless something tells it otherwise. Sentinel normally
//! reconfigures survivors directly during a failover, but a node partitioned
//! at that exact moment (unreachable, container mid-restart) never receives
//! that command: once it's back, it reconnects to the OLD master and silently
//! serves a dataset it will never leave on its own.
//!
//! This polls the local (colocated) Sentinel for the ground truth —
//! `SENTINEL get-master-addr-by-name`, the same call `/role` already uses to
//! fence writes — and issues `REPLICAOF` directly whenever this node
//! disagrees with it. Seconds, no restart: unlike redeploying the service
//! (which was tried and reverted — see mono#34477), this repoints the SAME
//! live dataset rather than re-cloning it, and it works wherever the image
//! runs, not only on a platform that can trigger a redeploy.

use redis::{aio::MultiplexedConnection, Client};
use std::time::Duration;
use tracing::{info, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

// Redis retries a dropped link on its own within a couple of seconds for an
// ordinary blip (failover elsewhere, a master restart). Only a link still down
// after this many consecutive polls is worth investigating — that shape means
// a stale target, not a transient hiccup.
const DOWN_DWELL_POLLS: u32 = 3;

#[derive(Debug, PartialEq)]
struct ReplicationInfo {
    role_is_master: bool,
    master_host: Option<String>,
    master_port: Option<u16>,
    link_down: bool,
}

fn parse_replication_info(info: &str) -> ReplicationInfo {
    let mut role_is_master = false;
    let mut master_host = None;
    let mut master_port = None;
    let mut link_down = false;

    for line in info.lines() {
        let line = line.trim();
        if line == "role:master" {
            role_is_master = true;
        } else if let Some(v) = line.strip_prefix("master_host:") {
            master_host = Some(v.to_string());
        } else if let Some(v) = line.strip_prefix("master_port:") {
            master_port = v.parse().ok();
        } else if line == "master_link_status:down" {
            link_down = true;
        }
    }

    ReplicationInfo {
        role_is_master,
        master_host,
        master_port,
        link_down,
    }
}

/// What to do about a replica whose link has been down past the dwell,
/// given what Sentinel says the master actually is right now.
///
/// A pure decision function so every branch is unit-testable without a Redis
/// server: the reconciler is only as trustworthy as this table.
#[derive(Debug, PartialEq)]
enum ReconcileAction {
    /// Sentinel confirms we're already pointed at the right master — the
    /// link being down is a different problem (the master itself is
    /// unreachable), not a stale pointer. Not ours to fix.
    NoOp,
    /// Sentinel's answer IS this node — a promotion whose REPLICAOF NO ONE
    /// never landed (partitioned at the exact moment of failover).
    PromoteSelf,
    /// Sentinel's answer is a different node than the one we're replicating
    /// from — repoint.
    Repoint { host: String, port: u16 },
}

fn decide_action(
    repl: &ReplicationInfo,
    sentinel_host: &str,
    sentinel_port: u16,
    own_private_domain: &str,
) -> ReconcileAction {
    if sentinel_host == own_private_domain {
        return ReconcileAction::PromoteSelf;
    }
    let already_correct = repl.master_host.as_deref() == Some(sentinel_host)
        && repl.master_port == Some(sentinel_port);
    if already_correct {
        return ReconcileAction::NoOp;
    }
    ReconcileAction::Repoint {
        host: sentinel_host.to_string(),
        port: sentinel_port,
    }
}

async fn fetch_replication_info(conn: &mut MultiplexedConnection) -> Option<ReplicationInfo> {
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(conn)
        .await
        .ok()?;
    Some(parse_replication_info(&info))
}

/// Ask the local Sentinel who it currently believes `master_name` is.
/// None on any failure — callers must treat that as "don't know", never as
/// license to guess.
async fn ask_sentinel_for_master(
    conn: &mut MultiplexedConnection,
    master_name: &str,
) -> Option<(String, u16)> {
    let result: redis::RedisResult<Vec<String>> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg(master_name)
        .query_async(conn)
        .await;
    match result {
        Ok(parts) if parts.len() == 2 => {
            let port: u16 = parts[1].parse().ok()?;
            Some((parts[0].clone(), port))
        }
        _ => None,
    }
}

pub async fn run_replication_reconciler(
    redis_port: u16,
    sentinel_port: u16,
    redis_password: String,
    master_name: String,
    private_domain: String,
) {
    let redis_url = format!("redis://:{}@127.0.0.1:{}", redis_password, redis_port);
    let sentinel_url = format!("redis://127.0.0.1:{}", sentinel_port);

    let Ok(redis_client) = Client::open(redis_url) else {
        warn!("replication reconciler: failed to build redis client, not running");
        return;
    };
    let Ok(sentinel_client) = Client::open(sentinel_url) else {
        warn!("replication reconciler: failed to build sentinel client, not running");
        return;
    };

    let mut consecutive_down = 0u32;

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let Ok(mut redis_conn) = redis_client.get_multiplexed_async_connection().await else {
            continue;
        };
        let Some(repl) = fetch_replication_info(&mut redis_conn).await else {
            continue;
        };

        // Masters have no link to repair, and issuing REPLICAOF against one
        // would DEMOTE it — the opposite of reconciling. Checked on live role
        // every poll (not the boot-time REPLICA_OF), since a former root can
        // become a replica after a failover and vice versa.
        if repl.role_is_master || !repl.link_down {
            consecutive_down = 0;
            continue;
        }

        consecutive_down += 1;
        if consecutive_down < DOWN_DWELL_POLLS {
            continue;
        }

        let Ok(mut sentinel_conn) = sentinel_client.get_multiplexed_async_connection().await else {
            warn!("replication reconciler: sentinel unreachable, cannot verify current master");
            continue;
        };
        let Some((sentinel_host, sentinel_port_answer)) =
            ask_sentinel_for_master(&mut sentinel_conn, &master_name).await
        else {
            warn!(
                "replication reconciler: sentinel gave no answer for get-master-addr-by-name, skipping"
            );
            continue;
        };

        let action = decide_action(&repl, &sentinel_host, sentinel_port_answer, &private_domain);

        let outcome: redis::RedisResult<()> = match &action {
            ReconcileAction::NoOp => {
                continue;
            }
            ReconcileAction::PromoteSelf => {
                info!("replication reconciler: sentinel says we are the master, promoting");
                redis::cmd("REPLICAOF")
                    .arg("NO")
                    .arg("ONE")
                    .query_async(&mut redis_conn)
                    .await
            }
            ReconcileAction::Repoint { host, port } => {
                info!(
                    stale_master = ?repl.master_host,
                    new_master = %host,
                    new_port = port,
                    "replication reconciler: replica points at a stale master, repointing"
                );
                redis::cmd("REPLICAOF")
                    .arg(host)
                    .arg(port)
                    .query_async(&mut redis_conn)
                    .await
            }
        };

        match outcome {
            Ok(()) => {
                info!("replication reconciler: reconciled successfully");
                consecutive_down = 0;
            }
            Err(e) => {
                warn!(error = %e, "replication reconciler: REPLICAOF failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(
        role: &str,
        master_host: Option<&str>,
        master_port: Option<&str>,
        link: &str,
    ) -> String {
        let mut lines = vec![format!("role:{}", role)];
        if let Some(h) = master_host {
            lines.push(format!("master_host:{}", h));
        }
        if let Some(p) = master_port {
            lines.push(format!("master_port:{}", p));
        }
        lines.push(format!("master_link_status:{}", link));
        lines.join("\r\n")
    }

    // --- parse_replication_info: every field, every shape ---

    #[test]
    fn parses_a_healthy_replica() {
        let parsed = parse_replication_info(&info("slave", Some("redis-1"), Some("6379"), "up"));
        assert_eq!(
            parsed,
            ReplicationInfo {
                role_is_master: false,
                master_host: Some("redis-1".to_string()),
                master_port: Some(6379),
                link_down: false,
            }
        );
    }

    #[test]
    fn parses_a_broken_replica() {
        let parsed = parse_replication_info(&info("slave", Some("redis-1"), Some("6379"), "down"));
        assert!(parsed.link_down);
    }

    #[test]
    fn parses_a_master() {
        let parsed = parse_replication_info("role:master\r\nconnected_slaves:2");
        assert!(parsed.role_is_master);
        assert!(!parsed.link_down);
    }

    #[test]
    fn missing_fields_read_as_absent_not_a_crash() {
        let parsed = parse_replication_info("role:slave");
        assert_eq!(parsed.master_host, None);
        assert_eq!(parsed.master_port, None);
        assert!(!parsed.link_down);
    }

    #[test]
    fn unparseable_port_is_none_not_a_panic() {
        let parsed =
            parse_replication_info(&info("slave", Some("redis-1"), Some("not-a-port"), "up"));
        assert_eq!(parsed.master_port, None);
    }

    // --- decide_action: every branch ---

    fn replica_at(host: &str, port: u16, link_down: bool) -> ReplicationInfo {
        ReplicationInfo {
            role_is_master: false,
            master_host: Some(host.to_string()),
            master_port: Some(port),
            link_down,
        }
    }

    #[test]
    fn already_correct_is_a_noop() {
        let repl = replica_at("redis-1", 6379, true);
        assert_eq!(
            decide_action(&repl, "redis-1", 6379, "redis-2"),
            ReconcileAction::NoOp
        );
    }

    #[test]
    fn stale_target_is_a_repoint() {
        // The exact partitioned-during-failover shape: still pointed at the
        // old master while Sentinel has already moved on.
        let repl = replica_at("redis-1", 6379, true);
        assert_eq!(
            decide_action(&repl, "redis-2", 6379, "redis-3"),
            ReconcileAction::Repoint {
                host: "redis-2".to_string(),
                port: 6379,
            }
        );
    }

    #[test]
    fn sentinel_naming_us_as_master_is_self_promotion() {
        // A promoted node whose REPLICAOF NO ONE from Sentinel never landed.
        let repl = replica_at("redis-1", 6379, true);
        assert_eq!(
            decide_action(&repl, "redis-2", 6379, "redis-2"),
            ReconcileAction::PromoteSelf
        );
    }

    #[test]
    fn self_promotion_wins_even_with_no_prior_master_recorded() {
        // A node with no master_host at all (e.g. fresh replica config) that
        // Sentinel already considers the master.
        let repl = ReplicationInfo {
            role_is_master: false,
            master_host: None,
            master_port: None,
            link_down: true,
        };
        assert_eq!(
            decide_action(&repl, "redis-1", 6379, "redis-1"),
            ReconcileAction::PromoteSelf
        );
    }

    #[test]
    fn different_port_on_the_same_host_is_still_a_repoint() {
        let repl = replica_at("redis-1", 6379, true);
        assert_eq!(
            decide_action(&repl, "redis-1", 6380, "redis-2"),
            ReconcileAction::Repoint {
                host: "redis-1".to_string(),
                port: 6380,
            }
        );
    }
}
