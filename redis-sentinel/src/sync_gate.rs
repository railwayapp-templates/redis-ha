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
//! the full sync has completed and the node holds the dataset.
//! `demote_on_shutdown` needs no change: a gated-only candidate set answers
//! its forced failover with `-NOGOODSLAVE`, which it already treats as
//! "shut down without a handoff". `/switchover` refuses a node still at the
//! gated priority, and `/role` reports it as `promotable:false` so a caller
//! can hold the action instead of having it refused.
//!
//! ## The gate survives a restart
//! Dataset presence alone cannot carry the gate across a restart. With
//! `appendonly yes`, redis-server creates the AOF manifest — over an empty
//! base file — at startup, before replication even begins, so the second
//! boot of a replica that never finished its first sync finds "a dataset"
//! on the volume and would come up at the default priority, empty and
//! promotable. The gate therefore keeps its own state: [`arm`] writes
//! [`PENDING_MARKER`] into the data dir before redis-server starts, and the
//! watcher removes it only once the lift has landed. A boot that finds the
//! marker is gated whatever else the volume holds; a boot that is not gated
//! (a master boot, the kill switch) removes a stale one.
//!
//! With every replica gated and the master gone, the election aborts
//! (`-failover-abort-no-good-slave`) and Sentinel retries on its own clock
//! until the master is back or a replica has synced: the cluster is down
//! rather than empty — the same contract as the empty-primary boot guard
//! (`boot_role::decide_empty_primary_boot`), seen from the replica's side.
//!
//! Sentinel refreshes a replica's INFO every 10s on a healthy cluster (every
//! second while the replica reports its link down), so a just-synced replica
//! becomes a candidate within one refresh of the lift. A failover in that
//! gap finds no candidate and retries after `failover-timeout`; it never
//! promotes the wrong node.
//!
//! ## Kill switch
//! `REPLICA_SYNC_GATE=false` disables both the stamp and the watcher;
//! anything else (unset, empty, garbage) leaves the gate on.

use crate::atomic_write::write_atomic;
use crate::boot_role::{enabled, BootMaster};
use crate::config::Config;
use crate::sentinel_query::{build_redis_url, config_get_value, connect};
use redis::aio::MultiplexedConnection;
use std::fs;
use std::path::{Path, PathBuf};
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
/// Written into the data dir while a gated boot's first full sync is
/// outstanding; removed by the watcher after the lift. Its presence gates
/// the next boot regardless of what else the volume holds — see the module
/// doc for why the dataset check alone cannot.
pub const PENDING_MARKER: &str = ".sync_gate_pending";

/// Short: the eligibility gap after a sync completes is this poll plus
/// Sentinel's own INFO refresh, and the poll is one local round-trip.
const POLL: Duration = Duration::from_secs(2);
const CONNECT_DEADLINE: Duration = Duration::from_secs(3);

pub fn gate_enabled() -> bool {
    enabled(std::env::var(REPLICA_SYNC_GATE_ENV).ok().as_deref())
}

pub fn pending_marker_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(PENDING_MARKER)
}

/// Whether an earlier boot armed the gate and never saw its lift.
pub fn pending_from_earlier_boot(data_dir: &str) -> bool {
    pending_marker_path(data_dir).exists()
}

/// Pure decision: gate a Sentinel-managed boot that replicates from another
/// node while holding nothing loadable of its own — or while an earlier
/// gated boot's first sync is still outstanding, whatever the volume holds
/// now. A master boot is never gated (its priority is irrelevant until it is
/// demoted, and it holds the dataset by definition); a replica boot over an
/// existing dataset with no pending gate is never gated (the dataset is what
/// the gate protects, and it is already there).
pub fn decide_gated(
    sentinel_enabled: bool,
    replicates_from_peer: bool,
    holds_dataset: bool,
    gate_enabled: bool,
    pending_from_earlier_boot: bool,
) -> bool {
    gate_enabled
        && sentinel_enabled
        && replicates_from_peer
        && (!holds_dataset || pending_from_earlier_boot)
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
        pending_from_earlier_boot(&config.data_dir),
    )
}

/// Decide this boot's gate and persist it: a gated boot leaves
/// [`PENDING_MARKER`] behind for the boots that may follow before the first
/// sync completes; an ungated boot clears a stale one. Call after redis.conf
/// is written (it reads the same inputs, so the stamp and this agree) and
/// before redis-server starts. Returns whether the boot is gated.
pub fn arm(config: &Config, boot_master: &BootMaster) -> bool {
    let gated = boot_is_gated(config, boot_master);
    let marker = pending_marker_path(&config.data_dir);
    if gated {
        if marker.exists() {
            info!(
                "sync gate: an earlier boot's first full sync never completed — \
                 replica-priority {} until it does",
                GATED_PRIORITY
            );
        } else {
            if let Err(e) = write_atomic(&marker, "first full sync not yet completed\n", None) {
                warn!(
                    error = %e,
                    path = %marker.display(),
                    "sync gate: could not persist the pending marker — a restart before the \
                     first sync completes would boot ungated"
                );
            }
            info!(
                "sync gate: this boot replicates from another node with no loadable dataset — \
                 replica-priority {} until the first full sync completes",
                GATED_PRIORITY
            );
        }
    } else {
        match clear_pending_marker(&config.data_dir) {
            Ok(true) => {
                info!("sync gate: this boot is not gated — removed the stale pending marker")
            }
            Ok(false) => {}
            Err(e) => warn!(
                error = %e,
                path = %marker.display(),
                "sync gate: could not remove the stale pending marker"
            ),
        }
    }
    gated
}

/// Remove [`PENDING_MARKER`]; `Ok(true)` when there was one to remove.
pub fn clear_pending_marker(data_dir: &str) -> std::io::Result<bool> {
    match fs::remove_file(pending_marker_path(data_dir)) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
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

/// Spawn the lift watcher for a boot [`arm`] said yes to. Runs until the
/// priority is lifted or there is nothing left to lift; respawns only on a
/// panic, so a bug here can never leave the node permanently unpromotable.
pub fn spawn(redis_port: u16, redis_password: String, data_dir: String) {
    let redis_url = build_redis_url("127.0.0.1", redis_port, &redis_password);
    tokio::spawn(async move {
        loop {
            let url = redis_url.clone();
            let dir = data_dir.clone();
            match tokio::task::spawn(async move { run(url, dir).await }).await {
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

async fn run(redis_url: String, data_dir: String) {
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
        // A reply without the value half is unreadable, not "moved": keep
        // polling on a fresh connection rather than stopping with the node
        // still gated.
        let Some(current_priority) = config_get_value(&priority) else {
            conn = None;
            continue;
        };
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
                        // The lift landed: the next boot loads this dataset
                        // and must not be gated by the marker.
                        if let Err(e) = clear_pending_marker(&data_dir) {
                            warn!(
                                error = %e,
                                "sync gate: lifted, but could not remove the pending marker — the \
                                 next boot stays gated only until its link reads up"
                            );
                        }
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
                // A master holds the dataset by definition; a replica whose
                // priority someone moved is still unsynced, so its marker
                // stays and the next boot re-arms the gate.
                if role == "master" {
                    if let Err(e) = clear_pending_marker(&data_dir) {
                        warn!(error = %e, "sync gate: could not remove the pending marker");
                    }
                }
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

    fn replica_config(data_dir: &str) -> Config {
        let mut config = Config::for_tests();
        config.sentinel_enabled = true;
        config.data_dir = data_dir.to_string();
        config.replica_of = "redis-1.railway.internal:6379".to_string();
        config.private_domain = "redis-2.railway.internal".to_string();
        config
    }

    fn primary_config(data_dir: &str) -> Config {
        let mut config = replica_config(data_dir);
        config.private_domain = "redis-1.railway.internal".to_string();
        config.replica_of = String::new();
        config
    }

    /// What redis-server leaves on the volume after one boot with
    /// `appendonly yes` and nothing to load: a manifest over an empty base.
    fn write_startup_manifest(dir: &Path) {
        fs::create_dir_all(dir.join("appendonlydir")).unwrap();
        fs::write(
            dir.join("appendonlydir").join("appendonly.aof.manifest"),
            b"file appendonly.aof.1.base.rdb seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\n",
        )
        .unwrap();
    }

    #[test]
    fn only_a_sentinel_replica_boot_over_nothing_is_gated() {
        assert!(decide_gated(true, true, false, true, false));
        // A master boot: nothing to gate, pending marker or not.
        assert!(!decide_gated(true, false, false, true, false));
        assert!(!decide_gated(true, false, false, true, true));
        // A replica boot over an existing dataset: the data is already here.
        assert!(!decide_gated(true, true, true, true, false));
        // ...unless an earlier gated boot never saw its lift.
        assert!(decide_gated(true, true, true, true, true));
        // Standalone (no Sentinel): no election to be excluded from.
        assert!(!decide_gated(false, true, false, true, false));
        // Kill switch wins over everything, the marker included.
        assert!(!decide_gated(true, true, false, false, false));
        assert!(!decide_gated(true, true, true, false, true));
    }

    #[test]
    fn boot_is_gated_reads_the_resolved_role_and_the_data_dir() {
        let dir = tempdir().unwrap();
        let config = replica_config(dir.path().to_str().unwrap());

        // Env replica on an empty volume: gated.
        assert!(boot_is_gated(&config, &BootMaster::NoLocalState));
        // Sentinel's persisted answer naming another master: gated too — the
        // role comes from the resolution, not from REPLICA_OF alone.
        let primary = primary_config(dir.path().to_str().unwrap());
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
        // Over a dataset AND a pending marker: gated — the marker outranks
        // whatever the volume holds.
        fs::write(pending_marker_path(&config.data_dir), b"pending\n").unwrap();
        assert!(boot_is_gated(&config, &BootMaster::NoLocalState));
        // A master boot ignores the marker.
        assert!(!boot_is_gated(&primary, &BootMaster::NoLocalState));
    }

    #[test]
    fn arm_persists_the_gate_across_the_manifest_redis_writes_at_startup() {
        let dir = tempdir().unwrap();
        let config = replica_config(dir.path().to_str().unwrap());

        // Boot 1: empty volume, gated, marker written.
        assert!(arm(&config, &BootMaster::NoLocalState));
        assert!(pending_from_earlier_boot(&config.data_dir));

        // redis-server ran and wrote its startup manifest; the first sync
        // never completed. Boot 2 must still be gated.
        write_startup_manifest(dir.path());
        assert!(Config::holds_redis_dataset(&config.data_dir));
        assert!(boot_is_gated(&config, &BootMaster::NoLocalState));
        assert!(arm(&config, &BootMaster::NoLocalState));
        assert!(pending_from_earlier_boot(&config.data_dir));

        // The lift removes the marker; boot 3 finds the synced dataset and
        // is not gated.
        assert!(clear_pending_marker(&config.data_dir).unwrap());
        assert!(!clear_pending_marker(&config.data_dir).unwrap());
        assert!(!boot_is_gated(&config, &BootMaster::NoLocalState));
        assert!(!arm(&config, &BootMaster::NoLocalState));
        assert!(!pending_from_earlier_boot(&config.data_dir));
    }

    #[test]
    fn arm_clears_a_stale_marker_on_a_boot_that_is_not_gated() {
        let dir = tempdir().unwrap();
        let primary = primary_config(dir.path().to_str().unwrap());
        fs::write(pending_marker_path(&primary.data_dir), b"pending\n").unwrap();
        assert!(!arm(&primary, &BootMaster::NoLocalState));
        assert!(!pending_from_earlier_boot(&primary.data_dir));
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
