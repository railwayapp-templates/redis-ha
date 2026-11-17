//! Did the master the peer Sentinels name ever actually run?
//!
//! ## The race
//! On a fresh deploy the first node to boot finds no peer Sentinel, falls
//! back to the env topology and starts its Sentinel monitoring the
//! env-declared master. When that master's container starts a few seconds
//! later, its first-boot peer query gets the env topology echoed back: the
//! peers name it master only because the env told them to. It has no
//! sentinel.conf and an empty data dir, which is exactly the shape of a
//! wiped incumbent master, so the empty-primary guard refused it, in a loop,
//! forever: the replicas it would "wipe" have never held anything, and no
//! failover can ever happen because no replica ever synced. Production
//! VM-conformance hit it on 6 of 6 fresh deploys (8-15s boot variance).
//!
//! ## The evidence
//! The two cases differ in what the peers have seen, and the peers can be
//! asked cheaply:
//!
//!   - `SENTINEL MASTER <name>` on each peer Sentinel. A Sentinel that ever
//!     reached the master holds its `runid` (learned from its INFO). One that
//!     ever saw it with replicas lists them (`num-slaves`, persisted in
//!     sentinel.conf as `known-replica`, so it survives a Sentinel restart),
//!     and one that ever ran a failover has a `config-epoch` above zero.
//!   - `INFO replication` on each peer's Redis. A replica that ever synced
//!     from a master, or a node that ever served as one, has a
//!     `master_repl_offset` above zero, restored from its RDB across
//!     restarts.
//!
//! [`decide_master_history`] reads a fresh cluster only off a complete,
//! clean set of answers: every configured peer's Sentinel AND Redis
//! answered, and none shows any of the above. A single trace of history
//! means a master really existed, and the guard keeps refusing. A peer that
//! did not answer is ambiguous, and the guard keeps refusing too: the
//! refusal exits the container, and the restart asks again, so a fresh
//! cluster converges as soon as every peer is up, while a real wipe never
//! gets waved through on a partial answer.

use crate::config::Config;
use std::time::Duration;

/// Kill switch for the fresh-cluster check. Same semantics as the other
/// boot kill switches: only the literal `false` disables it. Disabled, every
/// peer answer naming this node over an empty dataset refuses, as before.
pub const FRESH_CLUSTER_CHECK_ENV: &str = "EMPTY_PRIMARY_FRESH_CLUSTER_CHECK";

const DEFAULT_EVIDENCE_TIMEOUT_MS: u64 = 2000;

/// The fields of one peer Sentinel's `SENTINEL MASTER` reply that show it
/// ever observed the master it monitors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentinelMasterView {
    pub runid: String,
    pub num_slaves: u64,
    pub config_epoch: u64,
}

impl SentinelMasterView {
    /// `None` when a field is missing or unparseable: a reply this code
    /// cannot read is no evidence of a fresh cluster.
    pub fn from_fields(fields: &[String]) -> Option<Self> {
        let get = |key: &str| crate::quorum::field_value(fields, key);
        Some(Self {
            runid: get("runid")?,
            num_slaves: get("num-slaves")?.parse().ok()?,
            config_epoch: get("config-epoch")?.parse().ok()?,
        })
    }
}

/// One configured peer's answers. `None` means that endpoint did not answer
/// (or answered something unreadable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEvidence {
    pub host: String,
    pub sentinel: Option<SentinelMasterView>,
    pub master_repl_offset: Option<u64>,
}

/// What the peers' answers say about the master they name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MasterHistory {
    /// Every peer answered and none ever saw the master run: the peers only
    /// echo the env topology. Booting as the env master is safe.
    NeverRan,
    /// Some peer shows the master existed. The string names the evidence.
    Existed(String),
    /// Not every peer answered. The string names who did not.
    Ambiguous(String),
}

/// Pure decision over the gathered answers. Order matters: positive
/// evidence of history wins over a missing answer, so the log names the
/// strongest reason to refuse.
pub fn decide_master_history(peers: &[PeerEvidence]) -> MasterHistory {
    if peers.is_empty() {
        return MasterHistory::Ambiguous("no peer is configured".to_string());
    }
    for peer in peers {
        if let Some(view) = &peer.sentinel {
            if !view.runid.is_empty() {
                return MasterHistory::Existed(format!(
                    "the sentinel on {} has reached the master (runid {})",
                    peer.host, view.runid
                ));
            }
            if view.num_slaves > 0 {
                return MasterHistory::Existed(format!(
                    "the sentinel on {} knows {} replica(s) of the master",
                    peer.host, view.num_slaves
                ));
            }
            if view.config_epoch > 0 {
                return MasterHistory::Existed(format!(
                    "the sentinel on {} is at config-epoch {}",
                    peer.host, view.config_epoch
                ));
            }
        }
        if let Some(offset) = peer.master_repl_offset.filter(|o| *o > 0) {
            return MasterHistory::Existed(format!(
                "redis on {} has replicated (master_repl_offset {})",
                peer.host, offset
            ));
        }
    }
    let missing: Vec<String> = peers
        .iter()
        .flat_map(|peer| {
            let mut gaps = Vec::new();
            if peer.sentinel.is_none() {
                gaps.push(format!("sentinel on {}", peer.host));
            }
            if peer.master_repl_offset.is_none() {
                gaps.push(format!("redis on {}", peer.host));
            }
            gaps
        })
        .collect();
    if missing.is_empty() {
        MasterHistory::NeverRan
    } else {
        MasterHistory::Ambiguous(format!("no answer from {}", missing.join(", ")))
    }
}

/// `master_repl_offset` out of `INFO replication`. CRLF-tolerant.
pub fn parse_master_repl_offset(info: &str) -> Option<u64> {
    info.lines()
        .find_map(|line| line.trim_end().strip_prefix("master_repl_offset:"))
        .and_then(|v| v.trim().parse().ok())
}

async fn peer_redis_offset(
    host: &str,
    port: u16,
    password: &str,
    deadline: Duration,
) -> Option<u64> {
    let url = crate::sentinel_query::build_redis_url(host, port, password);
    let mut conn = crate::sentinel_query::connect(&url, deadline).await?;
    let info = tokio::time::timeout(
        deadline,
        redis::cmd("INFO")
            .arg("replication")
            .query_async::<String>(&mut conn),
    )
    .await
    .ok()?
    .ok()?;
    parse_master_repl_offset(&info)
}

/// Ask every configured peer, concurrently, for its view of the master.
/// Each peer's Redis is assumed on this node's `REDIS_PORT`, the same
/// assumption every member of the template makes about its siblings.
pub async fn gather_peer_evidence(config: &Config) -> Vec<PeerEvidence> {
    use common::ConfigExt;

    let deadline = Duration::from_millis(u64::env_parse(
        "FRESH_CLUSTER_CHECK_TIMEOUT_MS",
        DEFAULT_EVIDENCE_TIMEOUT_MS,
    ));
    let mut set = tokio::task::JoinSet::new();
    for (index, (host, sentinel_port)) in crate::boot_role::peer_sentinel_addrs(config)
        .into_iter()
        .enumerate()
    {
        let master_name = config.redis_master_name.clone();
        let password = config.redis_password.clone();
        let redis_port = config.redis_port;
        set.spawn(async move {
            let (fields, offset) = tokio::join!(
                crate::sentinel_query::get_master_fields_with_auth_fallback(
                    &host,
                    sentinel_port,
                    &master_name,
                    &password,
                    deadline,
                ),
                peer_redis_offset(&host, redis_port, &password, deadline),
            );
            let evidence = PeerEvidence {
                host,
                sentinel: fields.as_deref().and_then(SentinelMasterView::from_fields),
                master_repl_offset: offset,
            };
            (index, evidence)
        });
    }
    let mut gathered = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(answer) = joined {
            gathered.push(answer);
        }
    }
    // Keep SENTINEL_HOSTS order so the logged reason is deterministic.
    gathered.sort_by_key(|(index, _)| *index);
    gathered.into_iter().map(|(_, evidence)| evidence).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh(host: &str) -> PeerEvidence {
        PeerEvidence {
            host: host.to_string(),
            sentinel: Some(SentinelMasterView {
                runid: String::new(),
                num_slaves: 0,
                config_epoch: 0,
            }),
            master_repl_offset: Some(0),
        }
    }

    #[test]
    fn peers_echoing_the_env_for_a_master_that_never_ran_is_a_fresh_cluster() {
        assert_eq!(
            decide_master_history(&[fresh("redis-2"), fresh("redis-3")]),
            MasterHistory::NeverRan
        );
    }

    #[test]
    fn a_sentinel_that_reached_the_master_proves_it_existed() {
        let mut seen = fresh("redis-3");
        seen.sentinel.as_mut().unwrap().runid = "abc123".to_string();
        assert!(matches!(
            decide_master_history(&[fresh("redis-2"), seen]),
            MasterHistory::Existed(reason) if reason.contains("redis-3") && reason.contains("abc123")
        ));
    }

    #[test]
    fn known_replicas_prove_it_existed_even_after_a_sentinel_restart() {
        // A restarted Sentinel forgets the master's runid but reloads its
        // known-replica lines.
        let mut seen = fresh("redis-2");
        seen.sentinel.as_mut().unwrap().num_slaves = 2;
        assert!(matches!(
            decide_master_history(&[seen, fresh("redis-3")]),
            MasterHistory::Existed(reason) if reason.contains("2 replica")
        ));
    }

    #[test]
    fn a_failover_epoch_proves_it_existed() {
        let mut seen = fresh("redis-2");
        seen.sentinel.as_mut().unwrap().config_epoch = 1;
        assert!(matches!(
            decide_master_history(&[seen]),
            MasterHistory::Existed(_)
        ));
    }

    #[test]
    fn a_replica_with_an_offset_proves_it_existed() {
        let mut synced = fresh("redis-3");
        synced.master_repl_offset = Some(4242);
        assert!(matches!(
            decide_master_history(&[fresh("redis-2"), synced]),
            MasterHistory::Existed(reason) if reason.contains("4242")
        ));
    }

    #[test]
    fn a_silent_sentinel_is_ambiguous() {
        let mut silent = fresh("redis-3");
        silent.sentinel = None;
        assert!(matches!(
            decide_master_history(&[fresh("redis-2"), silent]),
            MasterHistory::Ambiguous(reason) if reason.contains("sentinel on redis-3")
        ));
    }

    #[test]
    fn a_silent_redis_is_ambiguous() {
        let mut silent = fresh("redis-2");
        silent.master_repl_offset = None;
        assert!(matches!(
            decide_master_history(&[silent, fresh("redis-3")]),
            MasterHistory::Ambiguous(reason) if reason.contains("redis on redis-2")
        ));
    }

    #[test]
    fn history_outranks_a_missing_answer() {
        let mut silent = fresh("redis-2");
        silent.sentinel = None;
        let mut synced = fresh("redis-3");
        synced.master_repl_offset = Some(1);
        assert!(matches!(
            decide_master_history(&[silent, synced]),
            MasterHistory::Existed(_)
        ));
    }

    #[test]
    fn no_peers_is_ambiguous() {
        assert!(matches!(
            decide_master_history(&[]),
            MasterHistory::Ambiguous(_)
        ));
    }

    #[test]
    fn reads_the_sentinel_master_reply() {
        let fields: Vec<String> = [
            "name",
            "mymaster",
            "ip",
            "redis-1",
            "port",
            "6379",
            "runid",
            "",
            "flags",
            "s_down,master,disconnected",
            "config-epoch",
            "0",
            "num-slaves",
            "0",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            SentinelMasterView::from_fields(&fields),
            Some(SentinelMasterView {
                runid: String::new(),
                num_slaves: 0,
                config_epoch: 0,
            })
        );
    }

    #[test]
    fn an_incomplete_sentinel_reply_is_no_answer() {
        let fields: Vec<String> = ["name", "mymaster", "runid", ""]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(SentinelMasterView::from_fields(&fields), None);
    }

    #[test]
    fn reads_the_offset_out_of_info_replication() {
        let info = "# Replication\r\nrole:slave\r\nmaster_host:redis-1\r\nmaster_link_status:down\r\nmaster_repl_offset:0\r\n";
        assert_eq!(parse_master_repl_offset(info), Some(0));
        assert_eq!(
            parse_master_repl_offset("role:master\nmaster_repl_offset:12345\n"),
            Some(12345)
        );
        assert_eq!(parse_master_repl_offset("role:master\n"), None);
    }
}
