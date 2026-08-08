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
//! Missing, unreadable or unparseable file → [`BootMaster::NoLocalState`].
//! That is the first-boot path — but "first boot" does not mean "fresh
//! cluster": a node added by a scale-up, or one whose volume was replaced,
//! first-boots into a cluster that may have failed over long ago, and the
//! env topology would attach it to a demoted ex-master no Sentinel would
//! ever repoint (Sentinel only learns replicas from the master's INFO). So
//! a first boot asks the peer Sentinels from `SENTINEL_HOSTS` who the
//! master currently is, and only a cluster where no peer answers — a
//! genuinely fresh one — falls back to the env topology.
//!
//! A peer answer naming THIS node is refused rather than obeyed
//! ([`boot_master_from_peer_answer`]), and when the env fallback would then
//! self-promote a primary whose data dir holds no loadable dataset, the boot
//! is refused outright ([`decide_empty_primary_boot`]): the cluster never
//! failed over, so its replicas still point at this node, reconnect the
//! moment it listens — satisfying min-replicas-to-write — and full-resync
//! the empty dataset. That is the documented replication wipe ("Safety of
//! replication when master has persistence turned off") Sentinel explicitly
//! does not protect against. Exiting instead lets the peers fail over to a
//! replica that still holds the data, and this node's next boot joins the
//! new master as a replica through the peer query.
//!
//! ## Limits
//! This is local state, not consensus. A node that was *down* for the whole
//! failover never saw the switch-master event, so its `sentinel.conf` still
//! names itself and it comes back as a master — Sentinel demotes it within a
//! failover-timeout, exactly as it does today. What this module fixes is every
//! case where the node's own Sentinel did observe the switch, which includes
//! the promoted node itself and any node that outlived the failover.
//!
//! The empty-primary guard only covers a boot that reached the peer query:
//! a volume that kept its sentinel.conf but lost its dataset resolves
//! locally and never asks the peers. The triggers this guards against —
//! volume replaced or recreated (template revert, re-conversion, fork) and
//! the volume-attach race booting on an empty ephemeral dir — lose the
//! whole volume, sentinel.conf included.

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
pub(crate) fn normalize_host(host: &str) -> String {
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

/// Hostnames the current deployment declares as cluster members: every
/// `SENTINEL_HOSTS` entry, this node itself, and the env-declared initial
/// master.
///
/// This is the boundary of the world that exists NOW — as far as the env
/// knows it. Persisted or gossiped state naming a master outside it usually
/// describes a topology that is gone: a volume reused across a template
/// revert, scale-down, or re-conversion still carries sentinel state for
/// nodes that were deleted, and resuming it makes every node a replica of a
/// ghost. The one legitimate exception is a scaled-up cluster: scale-up does
/// not restamp `SENTINEL_HOSTS` on existing nodes, so a failover onto a
/// scale-up member leaves the founding nodes' state naming a master the env
/// never heard of. That is why an undeclared master is probed for live
/// membership ([`undeclared_master_is_member`]) before being condemned.
pub(crate) fn declared_hosts(config: &Config) -> Vec<String> {
    let mut hosts: Vec<String> = Vec::new();
    let candidates = config
        .sentinel_hosts
        .split(',')
        .map(|peer| peer.trim().split(':').next().unwrap_or("").to_string())
        .chain([config.private_domain.clone(), config.initial_master_host()]);
    for candidate in candidates {
        let normalized = normalize_host(&candidate);
        if !normalized.is_empty() && !hosts.contains(&normalized) {
            hosts.push(normalized);
        }
    }
    hosts
}

/// Whether a recorded master host is a member of the declared topology.
pub(crate) fn master_is_declared(config: &Config, host: &str) -> bool {
    declared_hosts(config).contains(&normalize_host(host))
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

/// What the local sentinel.conf says, split so the caller can interpose the
/// membership probe: a master outside the declared topology is either a
/// scaled-up member the env never learned about (preserve) or state from a
/// dead world (quarantine), and only an async probe can tell them apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalSentinelState {
    /// No state, self-is-master, or a declared-member master — usable as-is.
    Usable(BootMaster),
    /// sentinel.conf names a master no declared member ever heard of.
    UndeclaredMaster(String, u16),
}

/// [`resolve_boot_master`] plus the declared-topology check. Self is a
/// declared member by definition and no-state has nothing to validate; only
/// a replica-of answer can point outside the world the env declares.
pub(crate) fn classify_local_state(config: &Config) -> LocalSentinelState {
    let resolved = resolve_boot_master(config);
    if let BootMaster::ReplicaOf(host, port) = &resolved {
        if !master_is_declared(config, host) {
            return LocalSentinelState::UndeclaredMaster(host.clone(), *port);
        }
    }
    LocalSentinelState::Usable(resolved)
}

/// Condemn local state whose master failed the membership probe: it is
/// quarantined (renamed aside, never deleted) and this boot proceeds as if
/// it had none — the peer query / env fallback re-anchor the node to the
/// topology that actually exists. With the stale file moved aside, the
/// wrapper's write-if-absent regenerates a fresh sentinel.conf, so the local
/// Sentinel doesn't resume monitoring the ghost either.
pub(crate) fn quarantine_dead_world_state(config: &Config, host: &str, port: u16) -> BootMaster {
    warn!(
        master = %format!("{host}:{port}"),
        declared = ?declared_hosts(config),
        "sentinel.conf names a master outside the declared topology — \
         quarantining the stale state and re-resolving"
    );
    match crate::sentinel_conf::quarantine_ghost_sentinel_conf(&config.data_dir) {
        Ok(Some(ghost)) => info!(to = %ghost.display(), "moved ghost sentinel.conf aside"),
        Ok(None) => {}
        Err(err) => warn!(error = %err, "failed to move ghost sentinel.conf aside"),
    }
    BootMaster::NoLocalState
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

// ====================================================================
// Peer-Sentinel boot query
// ====================================================================

/// Kill switch for the peer-Sentinel query on first boot. Same semantics as
/// [`BOOT_ROLE_ENV`]: only the literal `false` disables it.
pub const PEER_BOOT_ENV: &str = "BOOT_ROLE_FROM_PEER_SENTINELS";

/// Kill switch for the live-membership probe on a master outside the declared
/// topology. Same semantics as [`BOOT_ROLE_ENV`]: only the literal `false`
/// disables it — disabled, every undeclared master is quarantined, the
/// pre-#19 behavior.
pub const MEMBER_PROBE_ENV: &str = "BOOT_ROLE_PROBE_UNDECLARED_MASTER";

const DEFAULT_PEER_QUERY_TIMEOUT_MS: u64 = 2000;

const DEFAULT_MEMBER_PROBE_TIMEOUT_MS: u64 = 2000;

/// Whether a master outside the declared topology is a live member of THIS
/// cluster, as opposed to a ghost from a dead one.
///
/// `SENTINEL_HOSTS` is stamped at deploy time and scale-up does not restamp
/// existing nodes (that restamp is a fleet-wide redeploy — the mono #34895
/// lesson), so on a scaled cluster the declared topology undercounts the
/// real membership: after a failover onto a scale-up node, every founding
/// node's sentinel.conf names a master the env never heard of. Quarantining
/// that state would throw away a perfectly good topology on every restart.
///
/// The discriminator is the cluster's shared `REDIS_PASSWORD`: it is minted
/// once per cluster and never survives into another generation, so an
/// authenticated PING against the recorded address proves the host is a
/// live member of this cluster's world. A deleted host (no DNS), a wedged
/// one (timeout), and a foreign service that happens to reuse the hostname
/// of a dead member (AUTH refused) all fail the probe and fall through to
/// the quarantine. Two attempts, because boot is exactly when a peer
/// container may be mid-restart and one refused handshake must not condemn
/// a live member.
async fn undeclared_master_is_member(config: &Config, host: &str, port: u16) -> bool {
    use common::ConfigExt;

    if !enabled(std::env::var(MEMBER_PROBE_ENV).ok().as_deref()) {
        return false;
    }
    let deadline = std::time::Duration::from_millis(u64::env_parse(
        "UNDECLARED_MASTER_PROBE_TIMEOUT_MS",
        DEFAULT_MEMBER_PROBE_TIMEOUT_MS,
    ));
    let url = format!("redis://:{}@{}:{}", config.redis_password, host, port);
    for _ in 0..2 {
        if crate::sentinel_query::authenticated_ping(&url, deadline).await {
            return true;
        }
    }
    false
}

/// The Sentinel peers worth asking at boot: every `host:port` in
/// `SENTINEL_HOSTS` except this node's own — its Sentinel is not running yet,
/// and a self-answer would be circular anyway.
pub(crate) fn peer_sentinel_addrs(config: &Config) -> Vec<(String, u16)> {
    config
        .sentinel_hosts
        .split(',')
        .filter_map(|peer| {
            let peer = peer.trim();
            let (host, port) = peer.split_once(':')?;
            let port = port.parse::<u16>().ok()?;
            if host.is_empty() || normalize_host(host) == normalize_host(&config.private_domain) {
                return None;
            }
            Some((host.to_string(), port))
        })
        .collect()
}

/// Pick the answer most peers agree on. Hosts are compared normalized so
/// `Redis-2.railway.internal.` and `redis-2.railway.internal` count as one
/// vote; a tie keeps the earliest-seen answer, which is as good as any —
/// disagreement here means a failover is mid-flight and the topology heal
/// converges whichever side we land on.
pub(crate) fn majority_answer(answers: &[(String, u16)]) -> Option<(String, u16)> {
    let mut tally: Vec<((String, u16), u32, &(String, u16))> = Vec::new();
    for answer in answers {
        let key = (normalize_host(&answer.0), answer.1);
        match tally.iter_mut().find(|(k, _, _)| *k == key) {
            Some((_, count, _)) => *count += 1,
            None => tally.push((key, 1, answer)),
        }
    }
    // Strictly-greater keeps the earliest-seen answer on a tie (`max_by_key`
    // would keep the last).
    let mut best: Option<(u32, &(String, u16))> = None;
    for (_, count, original) in &tally {
        if best.is_none_or(|(best_count, _)| *count > best_count) {
            best = Some((*count, original));
        }
    }
    best.map(|(_, original)| original.clone())
}

/// Ask every peer Sentinel who the master currently is, concurrently, each
/// attempt bounded by `PEER_BOOT_QUERY_TIMEOUT_MS` (so boot stalls by at most
/// one timeout even when every peer is down — the whole-cluster-first-boot
/// case). Returns the majority answer, or `None` when no peer answered.
async fn query_peer_sentinels(config: &Config) -> Option<(String, u16)> {
    use common::ConfigExt;

    let peers = peer_sentinel_addrs(config);
    if peers.is_empty() {
        return None;
    }
    let deadline = std::time::Duration::from_millis(u64::env_parse(
        "PEER_BOOT_QUERY_TIMEOUT_MS",
        DEFAULT_PEER_QUERY_TIMEOUT_MS,
    ));
    let master_name = config.redis_master_name.clone();

    let mut set = tokio::task::JoinSet::new();
    for (host, port) in peers {
        let master_name = master_name.clone();
        set.spawn(async move {
            // Sentinel has no auth by default.
            let url = format!("redis://{host}:{port}");
            let mut conn = crate::sentinel_query::connect(&url, deadline).await?;
            crate::sentinel_query::get_master_addr(&mut conn, &master_name, deadline).await
        });
    }

    let mut answers: Vec<(String, u16)> = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(Some(answer)) = joined {
            answers.push(answer);
        }
    }
    majority_answer(&answers)
}

/// The resolved boot role plus the one observation the role alone cannot
/// carry: whether the peer-query stage refused a majority answer naming
/// this node. [`BootMaster::NoLocalState`] collapses "no peer answered" and
/// "every peer says the master is us" into the same env fallback, and the
/// empty-primary boot guard has to tell them apart — only the second, on a
/// primary with no dataset, is a cluster wipe in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootResolution {
    pub master: BootMaster,
    /// True only when the peer majority named this node and
    /// [`boot_master_from_peer_answer`] refused the answer.
    pub peers_named_self: bool,
}

impl BootResolution {
    fn of(master: BootMaster) -> Self {
        Self {
            master,
            peers_named_self: false,
        }
    }
}

/// Resolve the role this boot starts in, honoring the kill switches, and log
/// the decision. Standalone boots (no Sentinel) have no Sentinel state to
/// consult.
///
/// Resolution order:
///  1. Local `sentinel.conf` — the last thing this node's own Sentinel
///     observed. Survives restarts; wins whenever it exists AND its master
///     is a declared member ([`master_is_declared`]) OR probes as a live
///     member of this cluster ([`undeclared_master_is_member`] — the
///     scaled-up case the env topology undercounts). A master that is
///     neither is state from a dead world — a volume reused across a
///     revert/scale-down/re-conversion — and is quarantined instead of
///     resumed (see [`quarantine_dead_world_state`]).
///  2. Peer Sentinels — a node with no local state (first boot: a fresh
///     volume, a node newly added by a scale-up) asks the cluster it is
///     joining instead of trusting the env topology, which names whoever was
///     master when the template was stamped, not whoever is master now. A
///     node that attaches to a demoted ex-master never appears in the real
///     master's INFO, so Sentinel never learns it exists and can never
///     `+fix-slave-config` it — the one wrong turn here that nothing
///     downstream un-does (link_heal's wrong-master watch is the backstop).
///     A peer answer naming THIS node is refused rather than obeyed — see
///     [`boot_master_from_peer_answer`].
///  3. Env topology — a genuinely fresh cluster where no peer answers, or a
///     refused self-answer. A refused self-answer on an env-primary with no
///     loadable dataset never reaches this fallback: the wrapper's
///     empty-primary guard ([`empty_primary_boot_guard`]) exits instead.
pub async fn boot_master_for_this_boot(config: &Config) -> BootResolution {
    if !config.sentinel_enabled {
        return BootResolution::of(BootMaster::NoLocalState);
    }
    if !enabled(std::env::var(BOOT_ROLE_ENV).ok().as_deref()) {
        info!("boot role: from env topology ({}=false)", BOOT_ROLE_ENV);
        return BootResolution::of(BootMaster::NoLocalState);
    }
    let resolved = match classify_local_state(config) {
        LocalSentinelState::Usable(resolved) => resolved,
        LocalSentinelState::UndeclaredMaster(host, port) => {
            if undeclared_master_is_member(config, &host, port).await {
                info!(
                    "sentinel.conf names {}:{} — outside the declared topology, but it \
                     authenticates as a live member of this cluster (scaled-up membership); \
                     preserving the state",
                    host, port
                );
                BootMaster::ReplicaOf(host, port)
            } else {
                quarantine_dead_world_state(config, &host, port)
            }
        }
    };
    if !matches!(resolved, BootMaster::NoLocalState) {
        info!("{}", boot_role_log_line(config, &resolved));
        return BootResolution::of(resolved);
    }

    let mut peers_named_self = false;
    if enabled(std::env::var(PEER_BOOT_ENV).ok().as_deref()) {
        if let Some((host, port)) = query_peer_sentinels(config).await {
            // A peer can be resuming the same dead world this node just
            // refused (mid scale-down, or a whole cluster rebooting on
            // reused volumes) — its answer gets the same bar as local
            // state: declared member, or probing as a live member of this
            // cluster (a failover onto a scale-up node the env topology
            // never learned about).
            if !master_is_declared(config, &host)
                && !undeclared_master_is_member(config, &host, port).await
            {
                info!(
                    "peer sentinels name {}:{} as master — outside the declared topology \
                     and not a live member; falling back to the env topology",
                    host, port
                );
            } else {
                match boot_master_from_peer_answer(config, host, port) {
                    Some(resolved) => {
                        if let BootMaster::ReplicaOf(host, port) = &resolved {
                            info!(
                                "boot role: replica of {}:{} (from peer sentinels)",
                                host, port
                            );
                        }
                        return BootResolution::of(resolved);
                    }
                    None => {
                        peers_named_self = true;
                        info!(
                            "peer sentinels name this node as master, but this is a first boot \
                             with no local sentinel state — not self-promoting off a peer answer"
                        );
                    }
                }
            }
        } else {
            info!("no peer sentinel answered — falling back to the env topology");
        }
    }

    info!("{}", boot_role_log_line(config, &resolved));
    BootResolution {
        master: resolved,
        peers_named_self,
    }
}

/// What a peer answer means for the boot role. A peer naming another node is
/// a replication target; a peer naming THIS node is refused (`None`).
///
/// Self-promotion off a peer answer would be actively dangerous: a first
/// boot named master by its peers is the incumbent master coming back on a
/// wiped or replaced volume, and `REPLICAOF NO ONE` there boots an EMPTY
/// master that every data-bearing replica then full-syncs from — a
/// cluster-wide wipe. The refusal is recorded on the resolution
/// ([`BootResolution::peers_named_self`]) so the wrapper can tell it apart
/// from a genuinely fresh cluster: with a loadable dataset on disk the env
/// fallback is the incumbent master restarting normally (today's behavior);
/// without one, on an env-primary, the empty-primary guard refuses the boot
/// entirely rather than letting the env fallback stage the same wipe.
pub(crate) fn boot_master_from_peer_answer(
    config: &Config,
    host: String,
    port: u16,
) -> Option<BootMaster> {
    if addr_is_self(config, &host, port) {
        return None;
    }
    Some(BootMaster::ReplicaOf(host, port))
}

// ====================================================================
// Empty-primary boot guard
// ====================================================================

/// Kill switch for the empty-primary boot guard. Same semantics as
/// [`BOOT_ROLE_ENV`]: only the literal `false` disables it — disabled, the
/// env fallback self-promotes exactly as it did before the guard.
pub const EMPTY_PRIMARY_GUARD_ENV: &str = "EMPTY_PRIMARY_BOOT_GUARD";

/// Whether this boot may proceed into the env fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyPrimaryBoot {
    Proceed,
    /// Exit instead of booting: the cluster still names this node master and
    /// the data dir holds nothing to serve. Its replicas were never
    /// repointed, so they reconnect the moment it listens — acking enough to
    /// satisfy min-replicas-to-write — and full-resync the empty dataset.
    /// With this node down instead, the peers see the master down and fail
    /// over to a replica that still holds the data (their replicas are
    /// inside the down-after×10 validity window), and this node's next boot
    /// finds the new master through the peer query and joins as a replica.
    Refuse,
}

/// Pure (zero-I/O) decision, mirroring `decide_link_heal`: the wrapper
/// gathers the observations and performs the log + telemetry + exit.
///
/// All four must hold. `peers_named_self` already implies Sentinel is
/// enabled and the resolution had no usable local state (only the peer
/// query produces it), so neither is a separate input. `is_primary` scopes
/// the guard to the one node the env fallback would self-promote; a
/// loadable dataset means the incumbent master is restarting normally and
/// the env fallback stays.
pub fn decide_empty_primary_boot(
    peers_named_self: bool,
    is_primary: bool,
    holds_dataset: bool,
    guard_enabled: bool,
) -> EmptyPrimaryBoot {
    if guard_enabled && peers_named_self && is_primary && !holds_dataset {
        EmptyPrimaryBoot::Refuse
    } else {
        EmptyPrimaryBoot::Proceed
    }
}

/// [`decide_empty_primary_boot`] with its inputs gathered from the resolved
/// boot role, the env topology, the data dir, and the kill switch.
pub fn empty_primary_boot_guard(config: &Config, resolution: &BootResolution) -> EmptyPrimaryBoot {
    decide_empty_primary_boot(
        resolution.peers_named_self,
        config.is_primary(),
        Config::holds_redis_dataset(&config.data_dir),
        enabled(std::env::var(EMPTY_PRIMARY_GUARD_ENV).ok().as_deref()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    // --- peer_sentinel_addrs ---

    #[test]
    fn peer_addrs_exclude_self_and_junk() {
        let mut config = Config::for_tests();
        config.private_domain = "redis-1.railway.internal".to_string();
        config.sentinel_hosts = concat!(
            "redis-1.railway.internal:26379,",  // self — excluded
            "Redis-2.railway.internal.:26379,", // normalization is on the self-check only
            "redis-3.railway.internal:26379,",
            "not-a-pair,",
            ":26379,",
            "redis-4.railway.internal:not-a-port,",
            " "
        )
        .to_string();
        assert_eq!(
            peer_sentinel_addrs(&config),
            vec![
                ("Redis-2.railway.internal.".to_string(), 26379),
                ("redis-3.railway.internal".to_string(), 26379),
            ]
        );
    }

    #[test]
    fn self_exclusion_is_normalized() {
        let mut config = Config::for_tests();
        config.private_domain = "redis-1.railway.internal".to_string();
        config.sentinel_hosts = "Redis-1.railway.internal.:26379".to_string();
        assert_eq!(peer_sentinel_addrs(&config), vec![]);
    }

    // --- majority_answer ---

    #[test]
    fn majority_wins() {
        let answers = vec![
            ("redis-2.railway.internal".to_string(), 6379),
            ("redis-1.railway.internal".to_string(), 6379),
            ("redis-2.railway.internal".to_string(), 6379),
        ];
        assert_eq!(
            majority_answer(&answers),
            Some(("redis-2.railway.internal".to_string(), 6379))
        );
    }

    #[test]
    fn votes_are_counted_normalized() {
        let answers = vec![
            ("Redis-2.railway.internal.".to_string(), 6379),
            ("redis-2.railway.internal".to_string(), 6379),
            ("redis-1.railway.internal".to_string(), 6379),
        ];
        assert_eq!(
            majority_answer(&answers),
            Some(("Redis-2.railway.internal.".to_string(), 6379))
        );
    }

    #[test]
    fn a_tie_keeps_the_earliest_answer() {
        let answers = vec![
            ("redis-1.railway.internal".to_string(), 6379),
            ("redis-2.railway.internal".to_string(), 6379),
        ];
        assert_eq!(
            majority_answer(&answers),
            Some(("redis-1.railway.internal".to_string(), 6379))
        );
    }

    #[test]
    fn no_answers_is_none() {
        assert_eq!(majority_answer(&[]), None);
    }

    // --- boot_master_from_peer_answer ---

    #[test]
    fn a_peer_answer_naming_another_node_is_a_replication_target() {
        let config = Config::for_tests(); // self = redis-1.railway.internal:6379
        assert_eq!(
            boot_master_from_peer_answer(&config, "redis-2.railway.internal".to_string(), 6379),
            Some(BootMaster::ReplicaOf(
                "redis-2.railway.internal".to_string(),
                6379
            ))
        );
    }

    #[test]
    fn a_peer_answer_naming_this_node_is_refused() {
        let config = Config::for_tests();
        assert_eq!(
            boot_master_from_peer_answer(&config, "Redis-1.railway.internal.".to_string(), 6379),
            None
        );
    }

    #[test]
    fn a_peer_answer_naming_this_host_on_another_port_is_still_a_target() {
        let config = Config::for_tests();
        assert_eq!(
            boot_master_from_peer_answer(&config, "redis-1.railway.internal".to_string(), 6380),
            Some(BootMaster::ReplicaOf(
                "redis-1.railway.internal".to_string(),
                6380
            ))
        );
    }

    // --- decide_empty_primary_boot ---

    #[test]
    fn an_empty_primary_the_peers_still_name_master_is_refused() {
        // The wipe scenario: env-primary, fresh/wiped volume, cluster never
        // failed over. Every replica would full-resync the empty dataset.
        assert_eq!(
            decide_empty_primary_boot(true, true, false, true),
            EmptyPrimaryBoot::Refuse
        );
    }

    #[test]
    fn a_peer_answer_naming_another_node_never_refuses() {
        // ReplicaOf path, or no peer answered at all — the guard has no say.
        assert_eq!(
            decide_empty_primary_boot(false, true, false, true),
            EmptyPrimaryBoot::Proceed
        );
    }

    #[test]
    fn an_env_replica_never_refuses() {
        // REPLICA_OF set: the env fallback boots a stale replica, which
        // Sentinel's master-in-slave-role handling resolves — never a wipe.
        assert_eq!(
            decide_empty_primary_boot(true, false, false, true),
            EmptyPrimaryBoot::Proceed
        );
    }

    #[test]
    fn an_incumbent_master_with_a_dataset_boots_as_today() {
        // The peers naming us master with data on disk is the incumbent
        // master restarting normally — the env fallback stays.
        assert_eq!(
            decide_empty_primary_boot(true, true, true, true),
            EmptyPrimaryBoot::Proceed
        );
    }

    #[test]
    fn the_kill_switch_restores_the_env_fallback() {
        assert_eq!(
            decide_empty_primary_boot(true, true, false, false),
            EmptyPrimaryBoot::Proceed
        );
    }

    #[test]
    fn the_guard_reads_the_dataset_off_the_data_dir() {
        // EMPTY_PRIMARY_BOOT_GUARD is not set in the test environment, so
        // the kill-switch default (enabled) applies.
        let dir = tempdir().unwrap();
        let config = config_at(dir.path());
        assert!(config.is_primary());
        let refused = BootResolution {
            master: BootMaster::NoLocalState,
            peers_named_self: true,
        };
        assert_eq!(
            empty_primary_boot_guard(&config, &refused),
            EmptyPrimaryBoot::Refuse
        );
        fs::write(dir.path().join("dump.rdb"), b"REDIS0011fake").unwrap();
        assert_eq!(
            empty_primary_boot_guard(&config, &refused),
            EmptyPrimaryBoot::Proceed
        );
        assert_eq!(
            empty_primary_boot_guard(&config, &BootResolution::of(BootMaster::NoLocalState)),
            EmptyPrimaryBoot::Proceed
        );
    }

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

    // --- declared_hosts / master_is_declared ---

    fn cluster_config_at(dir: &std::path::Path) -> Config {
        let mut config = config_at(dir);
        config.private_domain = "redis-1.railway.internal".to_string();
        config.sentinel_hosts = concat!(
            "redis-1.railway.internal:26379,",
            "redis-2.railway.internal:26379,",
            "redis-3.railway.internal:26379"
        )
        .to_string();
        config
    }

    #[test]
    fn declared_hosts_cover_peers_self_and_the_env_master() {
        let dir = tempdir().unwrap();
        let mut config = cluster_config_at(dir.path());
        config.replica_of = "Redis-9.railway.internal.:6379".to_string();
        let hosts = declared_hosts(&config);
        assert!(hosts.contains(&"redis-1.railway.internal".to_string()));
        assert!(hosts.contains(&"redis-2.railway.internal".to_string()));
        assert!(hosts.contains(&"redis-3.railway.internal".to_string()));
        // The env-declared master counts even when SENTINEL_HOSTS omits it.
        assert!(hosts.contains(&"redis-9.railway.internal".to_string()));
        assert_eq!(hosts.len(), 4);
    }

    #[test]
    fn membership_is_normalized() {
        let dir = tempdir().unwrap();
        let config = cluster_config_at(dir.path());
        assert!(master_is_declared(&config, "Redis-2.Railway.Internal."));
        assert!(!master_is_declared(&config, "redis-4.railway.internal"));
    }

    // --- classify_local_state / quarantine_dead_world_state ---

    #[test]
    fn a_declared_master_is_preserved_along_with_the_conf() {
        // A failover target inside the declared topology must never be
        // touched — no probe, no quarantine.
        let dir = tempdir().unwrap();
        let config = cluster_config_at(dir.path());
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-2.railway.internal 6379 2\n",
        );
        assert_eq!(
            classify_local_state(&config),
            LocalSentinelState::Usable(BootMaster::ReplicaOf(
                "redis-2.railway.internal".to_string(),
                6379
            ))
        );
        assert!(dir.path().join("sentinel.conf").exists());
    }

    #[test]
    fn an_undeclared_master_is_flagged_for_the_membership_probe() {
        // Outside the declared topology the state is neither trusted nor
        // condemned here — classification defers to the caller's probe,
        // which distinguishes a scaled-up member from a dead world. The
        // file must be untouched at this stage.
        let dir = tempdir().unwrap();
        let config = cluster_config_at(dir.path());
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-4.railway.internal 6379 2\n",
        );
        assert_eq!(
            classify_local_state(&config),
            LocalSentinelState::UndeclaredMaster("redis-4.railway.internal".to_string(), 6379)
        );
        assert!(dir.path().join("sentinel.conf").exists());
    }

    #[test]
    fn a_dead_world_master_is_refused_and_the_conf_quarantined() {
        // The reused-volume case: sentinel state from a deleted world (a
        // scale-up later reverted) names a master no declared member ever
        // heard of and the probe found nothing alive. Resuming it would
        // strand the node as a replica of a ghost; instead the state is
        // moved aside — preserved, not deleted — and this boot proceeds
        // with no local state.
        let dir = tempdir().unwrap();
        let config = cluster_config_at(dir.path());
        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-4.railway.internal 6379 2\n",
        );
        assert_eq!(
            quarantine_dead_world_state(&config, "redis-4.railway.internal", 6379),
            BootMaster::NoLocalState
        );
        assert!(!dir.path().join("sentinel.conf").exists());
        let quarantined: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("sentinel.conf.ghost-")
            })
            .collect();
        assert_eq!(quarantined.len(), 1);
        assert_eq!(
            fs::read_to_string(quarantined[0].path()).unwrap(),
            "sentinel monitor mymaster redis-4.railway.internal 6379 2\n"
        );
    }

    #[test]
    fn self_and_no_state_pass_through_untouched() {
        let dir = tempdir().unwrap();
        let config = cluster_config_at(dir.path());
        assert_eq!(
            classify_local_state(&config),
            LocalSentinelState::Usable(BootMaster::NoLocalState)
        );

        write_sentinel_conf(
            dir.path(),
            "sentinel monitor mymaster redis-1.railway.internal 6379 2\n",
        );
        assert_eq!(
            classify_local_state(&config),
            LocalSentinelState::Usable(BootMaster::SelfIsMaster)
        );
        assert!(dir.path().join("sentinel.conf").exists());
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
