//! Sync gate: a replica that has never completed a full sync is not a
//! failover candidate.
//!
//! ## The gap
//! Sentinel's candidate selection (`sentinelSelectSlave`) filters on
//! liveness — `s_down`, a disconnected instance link, stale INFO, a
//! `master_link_down_since_seconds` past the validity window — and then
//! ranks by priority, replication offset and run id. Nothing in it asks
//! whether the replica ever finished its first sync. A replica still
//! receiving its first RDB reports `master_link_down_since_seconds:-1`
//! (Redis keeps `repl_down_since = 0` for "never connected"), an offset of
//! 1 and the default priority, and passes every filter. When the master
//! dies while its replicas are mid-transfer, Sentinel promotes an EMPTY
//! node; the surviving replica syncs the empty dataset from it; and when the
//! old master returns it is demoted (`+slave-reconf`) and full-syncs the
//! empty dataset over its own — the documented replication wipe, with the
//! customer's data intact on the volume right up to that last step.
//!
//! The window is the conversion: a standalone root becomes the HA master
//! and two fresh replicas start their first sync from it, seconds after
//! Sentinel has learned them from the master's INFO. A master that stops
//! mid-transfer — the BGSAVE fork OOM-killed over a dataset the new plan
//! does not fit, a planned stop whose demote-on-shutdown forces `SENTINEL
//! FAILOVER` — is the trigger. The same window reopens on every scale-up
//! for as long as the new node's first sync runs.
//!
//! ## The gate
//! `replica-priority 0` is Sentinel's own "never promote this one"
//! (`if (slave->slave_priority == 0) continue;` in the selection loop, and
//! `SENTINEL FAILOVER` selects from the same loop). A boot that replicates
//! from another node and holds no loadable dataset is stamped priority 0
//! in redis.conf ([`boot_is_gated`]); the watcher spawned by
//! [`spawn`] polls the local `INFO replication` and lifts the priority to
//! Redis's default with `CONFIG SET` the first time the link reads `up` —
//! the full sync has completed and the node holds the dataset. Later boots
//! find that dataset on disk and are never gated. `demote_on_shutdown`
//! needs no change: a gated-only candidate set answers its forced
//! failover with `-NOGOODSLAVE`, which it already treats as "shut down
//! without a handoff".
//!
//! With every replica gated and the master gone, the election aborts
//! (`-failover-abort-no-good-slave`) and Sentinel retries on its own clock
//! until the master is back or a replica has synced: the cluster is down
//! rather than empty — the same contract as the empty-primary boot guard
//! (`boot_role::decide_empty_primary_boot`), seen from the replica's side.
//!
//! Sentinel refreshes a replica's INFO every 10s on a healthy cluster, so a
//! just-synced replica becomes a candidate within one refresh of the lift.
//! A failover in that gap finds no candidate and retries after
//! `failover-timeout`; it never promotes the wrong node.
//!
//! ## Kill switch
//! `REPLICA_SYNC_GATE=false` disables both the stamp and the watcher;
//! anything else (unset, empty, garbage) leaves the gate on.

use crate::boot_role::{enabled, BootMaster};
use crate::config::Config;
use crate::sentinel_query::{build_redis_url, connect};
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Operator kill switch. Only the literal `false` disables the gate.
pub const REPLICA_SYNC_GATE_ENV: &str = "REPLICA_SYNC_GATE";
/// The priority Sentinel never promotes.
pub const GATED_PRIORITY: &str = "0";
/// Redis's own default `replica-priority`, restored once the first sync
/// completes.
pub const UNGATED_PRIORITY: &str = "100";

/// Short: the eligibility gap after a sync completes is this poll plus
/// Sentinel's own INFO refresh, and the poll is one local round-trip.
const POLL: Duration = Duration::from_secs(2);
const CONNECT_DEADLINE: Duration = Duration::from_secs(3);

pub fn gate_enabled() -> bool {
    enabled(std::env::var(REPLICA_SYNC_GATE_ENV).ok().as_deref())
}

/// Pure decision: gate a Sentinel-managed boot that replicates from another
/// node while holding nothing loadable of its own. A master boot is never
/// gated (its priority is irrelevant until it is demoted, and it holds the
/// dataset by definition); a replica boot over an existing dataset is never
/// gated (the dataset is what the gate protects, and it is already there).
pub fn decide_gated(
    sentinel_enabled: bool,
    replicates_from_peer: bool,
    holds_dataset: bool,
    gate_enabled: bool,
) -> bool {
    gate_enabled && sentinel_enabled && replicates_from_peer && !holds_dataset
}

/// [`decide_gated`] with its inputs gathered from the resolved boot role and
/// the data dir. Evaluate BEFORE redis-server starts: once it runs it
/// creates its own files and the dataset check no longer describes what
/// this boot found on the volume.
pub fn boot_is_gated(config: &Config, boot_master: &BootMaster) -> bool {
    decide_gated(
        config.sentinel_enabled,
        crate::redis_conf::replicate_from(config, boot_master).is_some(),
        Config::holds_redis_dataset(&config.data_dir),
        gate_enabled(),
    )
}

/// Whether `/switchover` must refuse: a node at the gated priority holds no
/// dataset to promote. An operator who set 0 deliberately gets the same
/// refusal — that is what the value means.
pub fn blocks_switchover(current_priority: &str) -> bool {
    current_priority.trim() == GATED_PRIORITY
}

/// What one poll of the gated node's state calls for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStep {
    /// Still syncing (or unreadable) — keep polling.
    Wait,
    /// The first sync completed — restore the default priority and stop.
    Lift,
    /// Nothing left for the watcher to do: the node is not a replica any
    /// more, or its priority is no longer the gated one (an operator moved
    /// it) — hands off.
    Stop,
}

/// Pure decision from the fields the watcher reads.
pub fn decide_step(role: &str, link_up: bool, current_priority: &str) -> GateStep {
    match role {
        "slave" if current_priority.trim() != GATED_PRIORITY => GateStep::Stop,
        "slave" if link_up => GateStep::Lift,
        "slave" => GateStep::Wait,
        "master" => GateStep::Stop,
        _ => GateStep::Wait,
    }
}

/// `role` and whether `master_link_status` reads `up`, from `INFO
/// replication`. CRLF-terminated in real output; `trim_end` handles it.
pub fn parse_role_and_link(info: &str) -> (String, bool) {
    let mut role = String::new();
    let mut link_up = false;
    for line in info.lines() {
        let line = line.trim_end();
        if let Some(v) = line.strip_prefix("role:") {
            role = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("master_link_status:") {
            link_up = v.trim() == "up";
        }
    }
    (role, link_up)
}

/// Spawn the lift watcher for a boot [`boot_is_gated`] said yes to. Runs
/// until the priority is lifted or there is nothing left to lift; respawns
/// only on a panic, so a bug here can never leave the node permanently
/// unpromotable.
pub fn spawn(redis_port: u16, redis_password: String) {
    let redis_url = build_redis_url("127.0.0.1", redis_port, &redis_password);
    info!(
        "sync gate: this boot replicates from another node with no loadable dataset — \
         replica-priority {} until the first full sync completes",
        GATED_PRIORITY
    );
    tokio::spawn(async move {
        loop {
            let url = redis_url.clone();
            match tokio::task::spawn(async move { run(url).await }).await {
                Ok(()) => return,
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "sync gate: watcher panicked — respawning in 5s")
                }
                Err(e) => warn!(error = %e, "sync gate: join error — respawning in 5s"),
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run(redis_url: String) {
    let mut conn: Option<MultiplexedConnection> = None;
    loop {
        sleep(POLL).await;
        if conn.is_none() {
            conn = connect(&redis_url, CONNECT_DEADLINE).await;
        }
        let Some(c) = conn.as_mut() else {
            continue;
        };
        let info: String = match redis::cmd("INFO").arg("replication").query_async(c).await {
            Ok(info) => info,
            Err(_) => {
                conn = None;
                continue;
            }
        };
        let priority: Vec<String> = match redis::cmd("CONFIG")
            .arg("GET")
            .arg("replica-priority")
            .query_async(c)
            .await
        {
            Ok(reply) => reply,
            Err(_) => {
                conn = None;
                continue;
            }
        };
        let current_priority = priority.get(1).cloned().unwrap_or_default();
        let (role, link_up) = parse_role_and_link(&info);
        match decide_step(&role, link_up, &current_priority) {
            GateStep::Wait => {}
            GateStep::Lift => {
                match redis::cmd("CONFIG")
                    .arg("SET")
                    .arg("replica-priority")
                    .arg(UNGATED_PRIORITY)
                    .query_async::<()>(c)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "sync gate: first full sync complete — replica-priority {}, this \
                             node is now a failover candidate",
                            UNGATED_PRIORITY
                        );
                        return;
                    }
                    Err(e) => {
                        warn!(error = %e, "sync gate: CONFIG SET replica-priority failed — retrying");
                        conn = None;
                    }
                }
            }
            GateStep::Stop => {
                info!(
                    role = %role,
                    priority = %current_priority,
                    "sync gate: nothing left to lift — watcher stopping"
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn only_a_sentinel_replica_boot_over_nothing_is_gated() {
        assert!(decide_gated(true, true, false, true));
        // A master boot: nothing to gate.
        assert!(!decide_gated(true, false, false, true));
        // A replica boot over an existing dataset: the data is already here.
        assert!(!decide_gated(true, true, true, true));
        // Standalone (no Sentinel): no election to be excluded from.
        assert!(!decide_gated(false, true, false, true));
        // Kill switch.
        assert!(!decide_gated(true, true, false, false));
    }

    #[test]
    fn boot_is_gated_reads_the_resolved_role_and_the_data_dir() {
        let dir = tempdir().unwrap();
        let mut config = Config::for_tests();
        config.sentinel_enabled = true;
        config.data_dir = dir.path().to_str().unwrap().to_string();
        config.replica_of = "redis-1.railway.internal:6379".to_string();
        config.private_domain = "redis-2.railway.internal".to_string();

        // Env replica on an empty volume: gated.
        assert!(boot_is_gated(&config, &BootMaster::NoLocalState));
        // Sentinel's persisted answer naming another master: gated too — the
        // role comes from the resolution, not from REPLICA_OF alone.
        let mut primary = Config::for_tests();
        primary.sentinel_enabled = true;
        primary.data_dir = config.data_dir.clone();
        primary.private_domain = "redis-1.railway.internal".to_string();
        primary.replica_of = String::new();
        assert!(boot_is_gated(
            &primary,
            &BootMaster::ReplicaOf("redis-3.railway.internal".to_string(), 6379)
        ));
        // Master by env or by Sentinel's answer: never gated.
        assert!(!boot_is_gated(&primary, &BootMaster::NoLocalState));
        assert!(!boot_is_gated(&config, &BootMaster::SelfIsMaster));

        // The same replica boot over a dataset: not gated.
        fs::write(dir.path().join("dump.rdb"), b"REDIS0011fake").unwrap();
        assert!(!boot_is_gated(&config, &BootMaster::NoLocalState));
    }

    #[test]
    fn the_gated_priority_blocks_a_switchover() {
        assert!(blocks_switchover("0"));
        assert!(blocks_switchover(" 0\r\n"));
        assert!(!blocks_switchover("100"));
        assert!(!blocks_switchover("1"));
        assert!(!blocks_switchover(""));
    }

    #[test]
    fn step_waits_while_syncing_lifts_on_first_up_and_stops_when_moot() {
        assert_eq!(decide_step("slave", false, "0"), GateStep::Wait);
        assert_eq!(decide_step("slave", true, "0"), GateStep::Lift);
        // Someone already moved the priority: hands off, whatever the link.
        assert_eq!(decide_step("slave", true, "100"), GateStep::Stop);
        assert_eq!(decide_step("slave", false, "50"), GateStep::Stop);
        // Not a replica any more.
        assert_eq!(decide_step("master", false, "0"), GateStep::Stop);
        // Unreadable role: keep polling rather than guessing.
        assert_eq!(decide_step("", false, "0"), GateStep::Wait);
    }

    #[test]
    fn parses_role_and_link_from_crlf_info() {
        let syncing = "# Replication\r\nrole:slave\r\nmaster_host:redis-1\r\n\
                       master_link_status:down\r\nmaster_sync_in_progress:1\r\n";
        assert_eq!(parse_role_and_link(syncing), ("slave".to_string(), false));
        let synced = "role:slave\r\nmaster_link_status:up\r\n";
        assert_eq!(parse_role_and_link(synced), ("slave".to_string(), true));
        let master = "role:master\r\nconnected_slaves:2\r\n";
        assert_eq!(parse_role_and_link(master), ("master".to_string(), false));
    }
}
