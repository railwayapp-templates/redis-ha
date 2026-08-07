//! Boot-time role resolution from Sentinel's own persisted state.
//!
//! ## The problem
//! `redis.conf` is regenerated on every boot, and its `replicaof` line came
//! solely from the deploy-time `REPLICA_OF` env var. That re-imposes the
//! deploy-time topology on every restart, no matter who Sentinel currently
//! considers master:
//!
//!   - A replica promoted by Sentinel that later restarts (OOM, redeploy)
//!     would regenerate `replicaof <old master>` and **demote itself**,
//!     full-syncing from the stale node it was promoted over — every write
//!     accepted since the promotion is discarded.
//!   - The node deployed as the initial master (`REPLICA_OF` empty) restarts
//!     after a failover elected someone else and comes back declaring itself
//!     master — a dual-master window until Sentinel demotes it.
//!   - A whole-cluster cold restart recreates the pre-failover topology
//!     wholesale, which no amount of Sentinel gossip can undo: every node
//!     boots in the role the env says, and the promoted node is the one that
//!     loses.
//!
//! ## The state we already have
//! Sentinel keeps its own truth on the same volume. The wrapper writes
//! `<data_dir>/sentinel.conf` on first boot and never again — Sentinel *owns*
//! that file afterwards and rewrites it after every failover, including the
//!
//! ```text
//! sentinel monitor <master-name> <host> <port> <quorum>
//! ```
//!
//! line, which always names the master Sentinel currently believes in. Reading
//! it back at boot turns "who was master when this service was deployed" into
//! "who is master according to the last thing Sentinel observed", which is the
//! only local answer that survives a cold start.
//!
//! ## Fallbacks
//! Missing, unreadable or unparseable file → [`BootMaster::NoLocalState`], and
//! the env topology is used exactly as before. That is the first-boot path
//! (the file we are about to write carries the env-declared master anyway), so
//! fresh clusters are unaffected.
//!
//! ## Limits
//! This is local state, not consensus. A node that was *down* for the whole
//! failover never saw the switch-master event, so its `sentinel.conf` still
//! names itself and it comes back as a master — Sentinel demotes it within a
//! failover-timeout, exactly as it does today. What this module fixes is every
//! case where the node's own Sentinel did observe the switch, which includes
//! the promoted node itself and any node that outlived the failover.

use crate::config::Config;
use std::io::ErrorKind;
use tracing::{info, warn};

/// Operator kill switch. Only the literal `false` disables the behavior;
/// anything else (unset, empty, garbage) leaves it on.
pub const BOOT_ROLE_ENV: &str = "BOOT_ROLE_FROM_SENTINEL_STATE";

/// The role this boot must start in, as far as local Sentinel state knows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootMaster {
    /// `sentinel.conf` names this very node as the master.
    SelfIsMaster,
    /// `sentinel.conf` names another address as the master.
    ReplicaOf(String, u16),
    /// No usable local Sentinel state — use the env topology.
    NoLocalState,
}

/// Extract the master address from a `sentinel monitor <name> <host> <port>
/// <quorum>` line.
///
/// Whitespace-tolerant (Sentinel writes single spaces, humans do not) and the
/// directive keywords are matched case-insensitively the way Redis parses its
/// own config. The master name is compared exactly — a config can monitor
/// several master sets and only ours is authoritative here.
///
/// The first line naming our master set is the answer, including when its
/// address fails to parse: falling through to a later duplicate would prefer
/// a stale entry over the live one Sentinel rewrote.
pub fn parse_sentinel_monitor(contents: &str, master_name: &str) -> Option<(String, u16)> {
    for line in contents.lines() {
        let mut fields = line.split_whitespace();
        if !matches!(fields.next(), Some(kw) if kw.eq_ignore_ascii_case("sentinel")) {
            continue;
        }
        if !matches!(fields.next(), Some(kw) if kw.eq_ignore_ascii_case("monitor")) {
            continue;
        }
        if fields.next() != Some(master_name) {
            continue;
        }
        let host = fields.next()?;
        let port = fields.next()?.parse::<u16>().ok()?;
        if host.is_empty() {
            return None;
        }
        return Some((host.to_string(), port));
    }
    None
}

/// DNS names are case-insensitive and a fully-qualified name may carry a
/// trailing root dot; `redis-2.railway.internal.` and `Redis-2.railway.internal`
/// are the same host.
fn normalize_host(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

/// Whether the recorded master address is this node.
///
/// The port has to match too: the same host on a different port is a
/// different Redis instance, and treating it as "self" would drop the
/// `replicaof` that instance needs.
fn addr_is_self(config: &Config, host: &str, port: u16) -> bool {
    port == config.redis_port && normalize_host(host) == normalize_host(&config.private_domain)
}

/// Read `<data_dir>/sentinel.conf` and turn its `sentinel monitor` line into
/// the role this boot has to start in.
pub fn resolve_boot_master(config: &Config) -> BootMaster {
    let path = format!("{}/sentinel.conf", config.data_dir);
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            // First boot: the file we are about to write carries the
            // env-declared master, so the env topology is the right answer.
            info!(path = %path, "no sentinel.conf on the volume — first boot");
            return BootMaster::NoLocalState;
        }
        Err(err) => {
            warn!(path = %path, error = %err, "could not read sentinel.conf — falling back to the env topology");
            return BootMaster::NoLocalState;
        }
    };

    let Some((host, port)) = parse_sentinel_monitor(&contents, &config.redis_master_name) else {
        warn!(
            path = %path,
            master_name = %config.redis_master_name,
            "sentinel.conf has no usable `sentinel monitor` line — falling back to the env topology"
        );
        return BootMaster::NoLocalState;
    };

    if addr_is_self(config, &host, port) {
        BootMaster::SelfIsMaster
    } else {
        BootMaster::ReplicaOf(host, port)
    }
}

/// True when the resolved role contradicts what `REPLICA_OF` alone would have
/// produced — the case the whole module exists for, and the one worth calling
/// out in the logs.
pub fn overrides_env_topology(config: &Config, resolved: &BootMaster) -> bool {
    match resolved {
        BootMaster::NoLocalState => false,
        BootMaster::SelfIsMaster => !config.is_primary(),
        BootMaster::ReplicaOf(host, port) => {
            config.is_primary()
                || normalize_host(&config.initial_master_host()) != normalize_host(host)
                || config.initial_master_port() != *port
        }
    }
}

/// The single line that states the boot decision and where it came from.
pub fn boot_role_log_line(config: &Config, resolved: &BootMaster) -> String {
    let overridden = overrides_env_topology(config, resolved);
    match resolved {
        BootMaster::ReplicaOf(host, port) if overridden => format!(
            "boot role: replica of {}:{} (from sentinel.conf — overriding REPLICA_OF={:?})",
            host, port, config.replica_of
        ),
        BootMaster::ReplicaOf(host, port) => {
            format!("boot role: replica of {}:{} (from sentinel.conf)", host, port)
        }
        BootMaster::SelfIsMaster if overridden => format!(
            "boot role: master (sentinel.conf names this node — overriding REPLICA_OF={:?})",
            config.replica_of
        ),
        BootMaster::SelfIsMaster => "boot role: master (sentinel.conf names this node)".to_string(),
        BootMaster::NoLocalState => {
            "boot role: from env topology (no local sentinel state)".to_string()
        }
    }
}

/// Kill switch semantics, split out from the environment so it is testable.
/// Only the literal `false` turns the behavior off.
fn enabled(raw: Option<&str>) -> bool {
    !matches!(raw.map(|v| v.trim().to_ascii_lowercase()), Some(v) if v == "false")
}

/// Resolve the role this boot starts in, honoring the kill switch, and log the
/// decision. Standalone boots (no Sentinel) have no Sentinel state to consult.
pub fn boot_master_for_this_boot(config: &Config) -> BootMaster {
    if !config.sentinel_enabled {
        return BootMaster::NoLocalState;
    }
    if !enabled(std::env::var(BOOT_ROLE_ENV).ok().as_deref()) {
        info!("boot role: from env topology ({}=false)", BOOT_ROLE_ENV);
        return BootMaster::NoLocalState;
    }
    let resolved = resolve_boot_master(config);
    info!("{}", boot_role_log_line(config, &resolved));
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    // --- parse_sentinel_monitor ---

    #[test]
    fn parses_a_normal_monitor_line() {
        let conf = "port 26379\nsentinel monitor mymaster redis-2.railway.internal 6379 2\n";
        assert_eq!(
            parse_sentinel_monitor(conf, "mymaster"),
            Some(("redis-2.railway.internal".to_string(), 6379))
        );
    }

    #[test]
    fn tolerates_extra_whitespace() {
        let conf = "  sentinel   monitor\tmymaster   redis-2   6379    2  \n";
        assert_eq!(
            parse_sentinel_monitor(conf, "mymaster"),
            Some(("redis-2".to_string(), 6379))
        );
    }

    #[test]
    fn ignores_a_different_master_name() {
        let conf = "sentinel monitor othermaster redis-9 6379 2\n";
        assert_eq!(parse_sentinel_monitor(conf, "mymaster"), None);
    }

    #[test]
    fn picks_the_line_matching_our_master_name() {
        let conf = concat!(
            "sentinel monitor othermaster redis-9 6379 2\n",
            "sentinel monitor mymaster redis-3 6380 2\n",
            "sentinel monitor thirdmaster redis-7 6379 2\n"
        );
        assert_eq!(
            parse_sentinel_monitor(conf, "mymaster"),
            Some(("redis-3".to_string(), 6380))
        );
    }

    #[test]
    fn ignores_other_sentinel_directives() {
        // A real rewritten config is mostly these.
        let conf = concat!(
            "sentinel auth-pass mymaster hunter2\n",
            "sentinel known-replica mymaster redis-1 6379\n",
            "sentinel monitor mymaster redis-2 6379 2\n",
            "sentinel known-sentinel mymaster redis-3 26379 abc123\n"
        );
        assert_eq!(
            parse_sentinel_monitor(conf, "mymaster"),
            Some(("redis-2".to_string(), 6379))
        );
    }

    #[test]
    fn garbage_file_has_no_monitor_line() {
        assert_eq!(parse_sentinel_monitor("\u{0}\u{1}not a config at all", "mymaster"), None);
        assert_eq!(parse_sentinel_monitor("", "mymaster"), None);
    }

    #[test]
    fn an_unparseable_port_is_not_a_match() {
        let conf = "sentinel monitor mymaster redis-2 not-a-port 2\n";
        assert_eq!(parse_sentinel_monitor(conf, "mymaster"), None);
    }

    #[test]
    fn a_truncated_monitor_line_is_not_a_match() {
        assert_eq!(parse_sentinel_monitor("sentinel monitor mymaster\n", "mymaster"), None);
        assert_eq!(
            parse_sentinel_monitor("sentinel monitor mymaster redis-2\n", "mymaster"),
            None
        );
    }

    #[test]
    fn a_stale_duplicate_never_wins_over_the_first_entry() {
        // Sentinel rewrites the file wholesale, so duplicates should not
        // happen — if they ever do, the first entry is the one to trust, even
        // when it is the broken one.
        let conf = concat!(
            "sentinel monitor mymaster redis-2 bogus 2\n",
            "sentinel monitor mymaster redis-1 6379 2\n"
        );
        assert_eq!(parse_sentinel_monitor(conf, "mymaster"), None);
    }

    // --- resolve_boot_master ---

    fn config_at(dir: &std::path::Path) -> Config {
        let mut config = Config::for_tests();
        config.data_dir = dir.to_str().unwrap().to_string();
        config
    }

    fn write_sentinel_conf(dir: &std::path::Path, body: &str) {
        fs::write(dir.join("sentinel.conf"), body).unwrap();
    }

    #[test]
    fn missing_file_is_no_local_state() {
        let dir = tempdir().unwrap();
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::NoLocalState
        );
    }

    #[test]
    fn unparseable_file_is_no_local_state() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(dir.path(), "port 26379\nlogfile \"\"\n");
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::NoLocalState
        );
    }

    #[test]
    fn a_conf_naming_this_node_means_self_is_master() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-1.railway.internal 6379 2\n",
        );
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::SelfIsMaster
        );
    }

    #[test]
    fn the_host_compare_is_case_insensitive() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster REDIS-1.Railway.Internal 6379 2\n",
        );
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::SelfIsMaster
        );
    }

    #[test]
    fn a_trailing_root_dot_is_still_this_node() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-1.railway.internal. 6379 2\n",
        );
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::SelfIsMaster
        );
    }

    #[test]
    fn our_host_on_another_port_is_a_different_instance() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-1.railway.internal 6380 2\n",
        );
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::ReplicaOf("redis-1.railway.internal".to_string(), 6380)
        );
    }

    #[test]
    fn a_conf_naming_another_node_means_replica() {
        let dir = tempdir().unwrap();
        write_sentinel_conf(dir.path(), "sentinel monitor mymaster redis-2 6379 2\n");
        assert_eq!(
            resolve_boot_master(&config_at(dir.path())),
            BootMaster::ReplicaOf("redis-2".to_string(), 6379)
        );
    }

    #[test]
    fn a_non_default_master_name_is_honored() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path());
        config.redis_master_name = "cache".to_string();
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-9 6379 2\nsentinel monitor cache redis-2 6379 2\n",
        );
        assert_eq!(
            resolve_boot_master(&config),
            BootMaster::ReplicaOf("redis-2".to_string(), 6379)
        );
    }

    // --- overrides_env_topology / log line ---

    #[test]
    fn agreement_with_the_env_is_not_an_override() {
        let mut config = Config::for_tests();
        config.replica_of = "redis-2:6379".to_string();
        assert!(!overrides_env_topology(
            &config,
            &BootMaster::ReplicaOf("redis-2".to_string(), 6379)
        ));
    }

    #[test]
    fn a_promoted_node_keeping_master_is_an_override() {
        let mut config = Config::for_tests();
        config.replica_of = "redis-1.railway.internal:6379".to_string();
        assert!(overrides_env_topology(&config, &BootMaster::SelfIsMaster));
        assert!(boot_role_log_line(&config, &BootMaster::SelfIsMaster).contains("overriding"));
    }

    #[test]
    fn a_deployed_master_demoted_by_sentinel_is_an_override() {
        // REPLICA_OF empty, sentinel.conf names someone else.
        let config = Config::for_tests();
        let resolved = BootMaster::ReplicaOf("redis-2".to_string(), 6379);
        assert!(overrides_env_topology(&config, &resolved));
        let line = boot_role_log_line(&config, &resolved);
        assert!(line.contains("replica of redis-2:6379"));
        assert!(line.contains("overriding"));
    }

    #[test]
    fn a_replica_repointed_at_a_new_master_is_an_override() {
        let mut config = Config::for_tests();
        config.replica_of = "redis-1:6379".to_string();
        assert!(overrides_env_topology(
            &config,
            &BootMaster::ReplicaOf("redis-3".to_string(), 6379)
        ));
    }

    #[test]
    fn no_local_state_is_never_an_override() {
        let config = Config::for_tests();
        assert!(!overrides_env_topology(&config, &BootMaster::NoLocalState));
        assert!(boot_role_log_line(&config, &BootMaster::NoLocalState)
            .contains("no local sentinel state"));
    }

    #[test]
    fn a_deployed_master_staying_master_is_not_an_override() {
        let config = Config::for_tests();
        assert!(!overrides_env_topology(&config, &BootMaster::SelfIsMaster));
        assert!(!boot_role_log_line(&config, &BootMaster::SelfIsMaster).contains("overriding"));
    }

    // --- kill switch ---

    #[test]
    fn the_behavior_is_on_by_default() {
        assert!(enabled(None));
        assert!(enabled(Some("")));
        assert!(enabled(Some("true")));
        assert!(enabled(Some("yes")));
        assert!(enabled(Some("garbage")));
    }

    #[test]
    fn only_false_turns_it_off() {
        assert!(!enabled(Some("false")));
        assert!(!enabled(Some("FALSE")));
        assert!(!enabled(Some(" false ")));
        assert!(!enabled(Some("False")));
    }
}
