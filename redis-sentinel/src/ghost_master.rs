//! Runtime self-heal for a ghost-mastered cluster: every Sentinel's answer
//! for the master names an address that is not a live member of this
//! cluster, and no data node holds the master role — the cluster has no
//! writable master and no Sentinel will ever elect one, because the ghost
//! address can never be observed down-then-replaced by something that does
//! not exist.
//!
//! ## How a running cluster gets here
//! The same dead-world state the boot path already quarantines
//! (`boot_role::quarantine_dead_world_state`): sentinel state resumed from a
//! volume that outlived its topology — a template revert, scale-down, or
//! re-conversion — names a master no current member ever heard of. The boot
//! sanitizer only runs at boot. A cluster whose containers keep running
//! never boots, so the cure it carries never fires: every node sits as a
//! replica of nothing, /role answers 503 everywhere, and nothing in
//! Sentinel's own machinery ever changes that (an election needs a live
//! majority to agree a REAL member failed — a ghost is not a member).
//!
//! ## Detection
//! Poll-driven, mirroring `link_heal`/`quorum`. Every confirming poll
//! requires ALL of these:
//!  - the LOCAL Sentinel's `get-master-addr-by-name` names an address that
//!    is outside the declared topology (`boot_role::master_is_declared`) —
//!    the cheap pre-filter that keeps the healthy-cluster poll to one local
//!    round-trip;
//!  - that address fails the authenticated live-member probe
//!    (`boot_role::undeclared_master_is_member`): the same discriminator
//!    the boot path uses, so a failover onto a scaled-up member the env
//!    undercounts is preserved here exactly as it is at boot;
//!  - the local Sentinel reports no failover in progress (an unreadable
//!    flags reply also fails closed);
//!  - the local Redis does not itself hold the master role;
//!  - a strict majority of the Sentinel membership (the declared
//!    `SENTINEL_HOSTS` peers united with the peers the local Sentinel
//!    currently knows — the union covers scale-ups the env never learned
//!    about) is readable and agrees on one master address: quorum
//!    consensus, never one Sentinel's view. Fewer answers than a majority —
//!    a partitioned minority, a lone node, unreachable peers — fails
//!    closed.
//!
//! What that consensus names decides which wedge this is:
//!  - **Consensus names the same ghost.** The whole world is headless. It
//!    confirms only if no data node of that world reports `role:master`
//!    either — the named master is dead (the membership probe just
//!    failed), this node is not master, and every replica the local
//!    Sentinel lists is probed; a master among them means a recovery is
//!    already under way.
//!  - **Consensus names a real member (declared, or live under the shared
//!    password) that disagrees with the local Sentinel.** This node is
//!    provably wedged on a ghost world the quorum has already left — the
//!    tail of this watcher's own staggered recovery, or a lone node that
//!    resumed dead state into an otherwise healthy cluster. Nothing else
//!    re-anchors it: Sentinel cannot fix a replica it cannot discover, and
//!    every local watcher's fix target — the local Sentinel's answer — IS
//!    the ghost. The healthy master is expected here, not a veto; the
//!    restart is what re-attaches this node to it. A consensus master that
//!    is neither declared nor live proves nothing, and ambiguity fails
//!    closed.
//!
//! The condition must hold on every consecutive poll for the dwell —
//! any poll that fails to re-confirm (including fail-closed ambiguity)
//! clears the window, so a normal failover, a boot transient, or a
//! partition can never accrue toward a restart. The window lives in
//! memory: a freshly booted process starts a fresh dwell.
//!
//! ## Action
//! Ask the supervisor to restart the container through the boot path
//! (non-zero exit, so an on-failure restart policy brings it back), which
//! runs the existing boot-time sanitizer: the ghost sentinel.conf is
//! quarantined, and boot-role resolution re-anchors the node to the
//! topology that actually exists. The restart is the delivery mechanism
//! for a cure that already ships in this image — nothing here mutates
//! Sentinel or Redis state directly.
//!
//! Restarts are staggered by seed rank — the env-declared primary first,
//! each replica after one more `GHOST_MASTER_STAGGER_SECONDS` — so the
//! cluster never restarts in lockstep and the re-founded master is already
//! answering when the replicas' boots query the peers.
//!
//! ## Cap
//! At most `GHOST_MASTER_MAX_RESTARTS_PER_WINDOW` (default 1) restarts per
//! rolling window, tracked in `<data_dir>/.ghost_master_state` on the
//! volume so the restart itself cannot reset the count. The boot sanitizer
//! either cures the state on the first pass or the state is something it
//! cannot cure — once the cap is reached the watcher stops restarting and
//! the node stays up serving its probes.

use common::{ConfigExt, Telemetry, TelemetryEvent};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::boot_role::normalize_host;
use crate::config::Config;
use crate::quorum::field_value;
use crate::sentinel_query;

const STATE_FILENAME: &str = ".ghost_master_state";

const DEFAULT_POLL_SECONDS: u64 = 30;
/// A ghost-mastered cluster is already unwritable, so there is no urgency
/// pressure on this number — only false-positive pressure. Fifteen minutes
/// of a consensus-confirmed dead master with no failover in progress and no
/// master anywhere is far past anything a real election explains (odown
/// fires within seconds, a contested election retries within
/// failover-timeout).
const DEFAULT_DWELL_SECONDS: u64 = 15 * 60;
/// Extra dwell per seed rank. Long enough for the previous rank to exit,
/// restart, and have its boot answer peer queries before the next rank's
/// boot asks.
const DEFAULT_STAGGER_SECONDS: u64 = 60;
const DEFAULT_MAX_RESTARTS_PER_WINDOW: u32 = 1;
const DEFAULT_WINDOW_SECONDS: u64 = 24 * 60 * 60;

const CALL_DEADLINE: Duration = Duration::from_secs(5);

/// True when the operator kill switch `GHOST_MASTER_HEAL_DISABLED=1` is
/// set — same convention as `LINK_HEAL_DISABLED`/`QUORUM_SYNC_DISABLED`.
fn disabled() -> bool {
    std::env::var("GHOST_MASTER_HEAL_DISABLED").ok().as_deref() == Some("1")
}

#[derive(Debug, Clone, Copy)]
struct Thresholds {
    dwell_secs: u64,
    stagger_secs: u64,
    max_restarts_per_window: u32,
    window_secs: u64,
}

#[derive(Debug, Clone, Copy)]
struct WatcherConfig {
    poll_secs: u64,
    thresholds: Thresholds,
}

impl WatcherConfig {
    fn from_env() -> Self {
        Self {
            poll_secs: u64::env_parse("GHOST_MASTER_POLL_SECONDS", DEFAULT_POLL_SECONDS),
            thresholds: Thresholds {
                dwell_secs: u64::env_parse("GHOST_MASTER_DWELL_SECONDS", DEFAULT_DWELL_SECONDS),
                stagger_secs: u64::env_parse(
                    "GHOST_MASTER_STAGGER_SECONDS",
                    DEFAULT_STAGGER_SECONDS,
                ),
                max_restarts_per_window: u32::env_parse(
                    "GHOST_MASTER_MAX_RESTARTS_PER_WINDOW",
                    DEFAULT_MAX_RESTARTS_PER_WINDOW,
                ),
                window_secs: u64::env_parse("GHOST_MASTER_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
            },
        }
    }
}

// ====================================================================
// Pure decision pieces (unit-tested directly)
// ====================================================================

/// Restart order across the cluster: the env-declared primary restarts
/// first (rank 0), replicas follow in their `SENTINEL_HOSTS` position
/// order (rank = position + 1, so no replica ever shares rank 0 with the
/// primary even when the list orders them differently). A host missing
/// from the list restarts last — it has the least claim to a
/// deterministic slot.
fn seed_rank(is_primary: bool, private_domain: &str, sentinel_hosts: &str) -> u32 {
    if is_primary {
        return 0;
    }
    let own = normalize_host(private_domain);
    let hosts: Vec<String> = sentinel_hosts
        .split(',')
        .filter_map(|entry| {
            let host = entry.trim().split(':').next().unwrap_or("");
            let normalized = normalize_host(host);
            (!normalized.is_empty()).then_some(normalized)
        })
        .collect();
    match hosts.iter().position(|host| *host == own) {
        Some(position) => position as u32 + 1,
        None => hosts.len() as u32 + 1,
    }
}

/// The address a strict majority of the membership agrees on. `membership`
/// is the number of Sentinels the answers were solicited from (self
/// included), so unreachable peers count against consensus — a partitioned
/// minority can collect its own answers forever and never clear this bar.
/// At least two agreeing Sentinels are required no matter how small the
/// membership: one Sentinel's say-so is exactly what this must never act on.
fn consensus_answer(answers: &[(String, u16)], membership: u32) -> Option<(String, u16)> {
    // (normalized key, votes, original answer)
    type Vote = ((String, u16), u32, (String, u16));
    let mut tally: Vec<Vote> = Vec::new();
    for answer in answers {
        let key = (normalize_host(&answer.0), answer.1);
        match tally.iter_mut().find(|(k, _, _)| *k == key) {
            Some((_, count, _)) => *count += 1,
            None => tally.push((key, 1, answer.clone())),
        }
    }
    tally
        .into_iter()
        .max_by_key(|(_, count, _)| *count)
        .filter(|(_, count, _)| *count >= 2 && count * 2 > membership)
        .map(|(_, _, original)| original)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GhostAction {
    /// Condition confirmed but the (rank-staggered) dwell is not served yet.
    Wait,
    Restart {
        attempt: u32,
    },
    GiveUp {
        attempts: u32,
    },
}

/// What a poll that CONFIRMED the ghost condition should do, given how long
/// the condition has held and how many restarts the persisted marker
/// already records. The stagger rides on the dwell: rank 0 fires at the
/// dwell, each higher rank one stagger later, so the fleet never restarts
/// in the same instant. The cap check runs only after the full hold — a
/// capped node that observes the condition again right after its own
/// restart (its peers have not restarted yet) must serve another whole
/// dwell before concluding the restart did not cure it.
fn decide(ghost_for_secs: u64, rank: u32, restarts_in_window: u32, t: &Thresholds) -> GhostAction {
    let hold = t.dwell_secs + u64::from(rank) * t.stagger_secs;
    if ghost_for_secs < hold {
        return GhostAction::Wait;
    }
    if restarts_in_window >= t.max_restarts_per_window {
        return GhostAction::GiveUp {
            attempts: restarts_in_window,
        };
    }
    GhostAction::Restart {
        attempt: restarts_in_window + 1,
    }
}

fn info_reports_master(info: &str) -> bool {
    info.lines().any(|line| line.trim_end() == "role:master")
}

fn flags_show_failover_in_progress(master_fields: &[String]) -> Option<bool> {
    field_value(master_fields, "flags")
        .map(|flags| flags.split(',').any(|flag| flag == "failover_in_progress"))
}

/// `(host, port)` pairs out of a `SENTINEL sentinels`/`SENTINEL replicas`
/// reply (one flat field-value array per instance).
fn addrs_from_instance_reply(entries: &[Vec<String>]) -> Vec<(String, u16)> {
    entries
        .iter()
        .filter_map(|entry| {
            let host = field_value(entry, "ip")?;
            let port = field_value(entry, "port")?.parse::<u16>().ok()?;
            (!host.is_empty()).then_some((host, port))
        })
        .collect()
}

/// Union of the declared peers and the gossip-known peers, self excluded,
/// deduplicated by normalized address. The union only ever grows the
/// consensus denominator: a scaled-up cluster's real membership makes the
/// majority bar stricter than the stale declared list alone would.
fn merge_peer_addrs(
    declared: &[(String, u16)],
    known: &[(String, u16)],
    private_domain: &str,
) -> Vec<(String, u16)> {
    let own = normalize_host(private_domain);
    let mut merged: Vec<(String, u16)> = Vec::new();
    for (host, port) in declared.iter().chain(known.iter()) {
        let normalized = normalize_host(host);
        if normalized.is_empty() || normalized == own {
            continue;
        }
        if !merged
            .iter()
            .any(|(h, p)| normalize_host(h) == normalized && *p == *port)
        {
            merged.push((host.clone(), *port));
        }
    }
    merged
}

// ====================================================================
// State file (the persisted restart marker)
// ====================================================================

fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Append a `restart=<epoch>` line. Additive on purpose: the file is the
/// proof of past restarts and survives the restart it records, which is the
/// whole point of the cap — pruning happens at read time via the window.
fn append_restart_marker(state_path: &str, now: i64) {
    let existing = std::fs::read_to_string(state_path).unwrap_or_default();
    let mut lines: Vec<String> = existing.lines().map(|s| s.to_string()).collect();
    lines.push(format!("restart={now}"));
    let mut out = lines.join("\n");
    out.push('\n');
    let _ = std::fs::write(state_path, out);
}

fn restarts_in_window(state_path: &str, now: i64, window_secs: u64) -> u32 {
    let Ok(content) = std::fs::read_to_string(state_path) else {
        return 0;
    };
    let cutoff = now - window_secs as i64;
    content
        .lines()
        .filter_map(|line| line.strip_prefix("restart="))
        .filter_map(|v| v.trim().parse::<i64>().ok())
        .filter(|t| *t >= cutoff)
        .count() as u32
}

// ====================================================================
// Observation (the async I/O half)
// ====================================================================

struct WatcherContext {
    sentinel_url: String,
    redis_url: String,
    master_name: String,
    redis_password: String,
    private_domain: String,
    /// Normalized declared member hostnames (`boot_role::declared_hosts`).
    declared_hosts: Vec<String>,
    /// Declared peer Sentinel addresses (`boot_role::peer_sentinel_addrs`).
    declared_peers: Vec<(String, u16)>,
    rank: u32,
    state_path: String,
    telemetry: Telemetry,
    restart_tx: mpsc::Sender<String>,
}

enum Verdict {
    /// Not the ghost condition this poll — includes every fail-closed
    /// ambiguity, so unobserved time never accrues toward the dwell.
    Clear(&'static str),
    Confirmed {
        master: (String, u16),
        /// Which wedge this poll saw (see the module doc): the whole quorum
        /// naming the ghost, or this node alone wedged outside a live
        /// consensus. Both count toward the same window — the second is
        /// what the first turns into as peers heal — so this is for the
        /// logs, never for the dwell.
        shape: &'static str,
    },
}

async fn observe(ctx: &WatcherContext) -> Verdict {
    // The local Sentinel's own answer is the cheap pre-filter: on a healthy
    // cluster it names a declared member (or this node) and the poll ends
    // after one loopback round-trip.
    let Some(mut sentinel_conn) = sentinel_query::connect(&ctx.sentinel_url, CALL_DEADLINE).await
    else {
        return Verdict::Clear("local sentinel unreachable");
    };
    let Some(local_answer) =
        sentinel_query::get_master_addr(&mut sentinel_conn, &ctx.master_name, CALL_DEADLINE).await
    else {
        return Verdict::Clear("local sentinel has no master answer");
    };
    let local_key = (normalize_host(&local_answer.0), local_answer.1);
    if local_key.0 == normalize_host(&ctx.private_domain) {
        return Verdict::Clear("local sentinel names this node");
    }
    if ctx.declared_hosts.contains(&local_key.0) {
        return Verdict::Clear("named master is a declared member");
    }

    // Never mid-election: Sentinel is already doing the recovery. An
    // unreadable flags reply is ambiguity, and ambiguity fails closed.
    let Some(master_fields) =
        sentinel_query::get_master_fields(&mut sentinel_conn, &ctx.master_name, CALL_DEADLINE)
            .await
    else {
        return Verdict::Clear("master flags unreadable");
    };
    match flags_show_failover_in_progress(&master_fields) {
        None => return Verdict::Clear("master flags unreadable"),
        Some(true) => return Verdict::Clear("failover in progress"),
        Some(false) => {}
    }

    // Quorum consensus over the full membership: declared peers united with
    // the peers the local Sentinel currently knows (scale-ups the env never
    // learned about grow the denominator, never shrink it).
    let known_peers: Vec<(String, u16)> = match redis::cmd("SENTINEL")
        .arg("sentinels")
        .arg(&ctx.master_name)
        .query_async::<Vec<Vec<String>>>(&mut sentinel_conn)
        .await
    {
        Ok(entries) => addrs_from_instance_reply(&entries),
        Err(_) => return Verdict::Clear("sentinel membership unreadable"),
    };
    let peers = merge_peer_addrs(&ctx.declared_peers, &known_peers, &ctx.private_domain);
    let membership = peers.len() as u32 + 1;
    if membership < 2 {
        return Verdict::Clear("no peer sentinels — not a cluster");
    }

    let mut set = tokio::task::JoinSet::new();
    for (host, port) in peers {
        let master_name = ctx.master_name.clone();
        let password = ctx.redis_password.clone();
        set.spawn(async move {
            sentinel_query::get_master_addr_with_auth_fallback(
                &host,
                port,
                &master_name,
                &password,
                CALL_DEADLINE,
            )
            .await
        });
    }
    let mut answers: Vec<(String, u16)> = vec![local_answer.clone()];
    while let Some(joined) = set.join_next().await {
        if let Ok(Some(answer)) = joined {
            answers.push(answer);
        }
    }
    let Some(consensus) = consensus_answer(&answers, membership) else {
        return Verdict::Clear("no quorum consensus on the master address");
    };

    // The boot path's own discriminator, unchanged: an address that
    // authenticates with the cluster's shared password is a live member —
    // the scaled-up master the declared topology undercounts — and must
    // never be treated as a ghost.
    if crate::boot_role::undeclared_master_is_member(
        &ctx.redis_password,
        &local_answer.0,
        local_answer.1,
    )
    .await
    {
        return Verdict::Clear("named master is a live cluster member");
    }

    // Never restart a node that is itself serving as master, whatever the
    // sentinels think.
    let Some(mut redis_conn) = sentinel_query::connect(&ctx.redis_url, CALL_DEADLINE).await else {
        return Verdict::Clear("local redis unreachable");
    };
    match tokio::time::timeout(
        CALL_DEADLINE,
        redis::cmd("INFO")
            .arg("replication")
            .query_async::<String>(&mut redis_conn),
    )
    .await
    {
        Ok(Ok(info)) if !info_reports_master(&info) => {}
        Ok(Ok(_)) => return Verdict::Clear("this node is master"),
        _ => return Verdict::Clear("local replication info unreadable"),
    }

    if (normalize_host(&consensus.0), consensus.1) != local_key {
        // The majority sees a different master than this node's own
        // Sentinel. When that consensus master is real — a declared member,
        // or a live one the declared topology undercounts — this node is
        // provably wedged on a ghost world the rest of the cluster has
        // already left (the tail of a staggered recovery, or a lone node
        // that resumed dead state into an otherwise healthy cluster).
        // Nothing else re-anchors it: Sentinel cannot fix a replica it
        // cannot discover, and the local watchers' fix target — the local
        // Sentinel's answer — IS the ghost. The healthy master here is
        // expected, not a veto; the restart is what re-attaches this node
        // to it. A consensus master that is neither declared nor live says
        // nothing trustworthy, and ambiguity fails closed.
        let consensus_is_declared = ctx.declared_hosts.contains(&normalize_host(&consensus.0));
        if consensus_is_declared
            || crate::boot_role::undeclared_master_is_member(
                &ctx.redis_password,
                &consensus.0,
                consensus.1,
            )
            .await
        {
            return Verdict::Confirmed {
                master: local_answer,
                shape: "wedged outside a live consensus",
            };
        }
        return Verdict::Clear("consensus names neither this node's master nor a live member");
    }

    // The whole world agrees on the ghost. The condition is only a wedge if
    // no data node of that world holds the master role: this node was
    // checked above, the consensus master is dead (the membership probe
    // just failed), and the replicas the local Sentinel lists are probed
    // here. A master among them means some recovery is already under way —
    // leave it alone.
    let replicas: Vec<(String, u16)> = match redis::cmd("SENTINEL")
        .arg("replicas")
        .arg(&ctx.master_name)
        .query_async::<Vec<Vec<String>>>(&mut sentinel_conn)
        .await
    {
        Ok(entries) => addrs_from_instance_reply(&entries),
        Err(_) => return Verdict::Clear("sentinel replica table unreadable"),
    };
    for (host, port) in replicas {
        let url = sentinel_query::build_redis_url(&host, port, &ctx.redis_password);
        let Some(mut conn) = sentinel_query::connect(&url, CALL_DEADLINE).await else {
            continue;
        };
        if let Ok(Ok(info)) = tokio::time::timeout(
            CALL_DEADLINE,
            redis::cmd("INFO")
                .arg("replication")
                .query_async::<String>(&mut conn),
        )
        .await
        {
            if info_reports_master(&info) {
                return Verdict::Clear("a member replica reports role:master");
            }
        }
    }

    Verdict::Confirmed {
        master: local_answer,
        shape: "quorum consensus names the ghost",
    }
}

// ====================================================================
// Watcher loop
// ====================================================================

struct WatcherState {
    /// Epoch of the first poll of the current unbroken run of confirmed
    /// observations. `None` when the last poll cleared.
    ghost_since: Option<i64>,
    gave_up_emitted: bool,
}

async fn iteration(ctx: &WatcherContext, state: &mut WatcherState, cfg: &WatcherConfig) {
    let now = now_epoch();
    match observe(ctx).await {
        Verdict::Clear(reason) => {
            if state.ghost_since.is_some() {
                info!(reason, "ghost-master: condition cleared — dwell reset");
            }
            state.ghost_since = None;
            state.gave_up_emitted = false;
        }
        Verdict::Confirmed { master, shape } => {
            let since = *state.ghost_since.get_or_insert_with(|| {
                info!(
                    master = %format!("{}:{}", master.0, master.1),
                    shape,
                    rank = ctx.rank,
                    hold_secs =
                        cfg.thresholds.dwell_secs
                            + u64::from(ctx.rank) * cfg.thresholds.stagger_secs,
                    "ghost-master: sentinel state names a master that is not a live \
                     cluster member — dwell started"
                );
                now
            });
            let ghost_for_secs = now.saturating_sub(since).max(0) as u64;
            let restarts = restarts_in_window(&ctx.state_path, now, cfg.thresholds.window_secs);
            match decide(ghost_for_secs, ctx.rank, restarts, &cfg.thresholds) {
                GhostAction::Wait => {}
                GhostAction::GiveUp { attempts } => {
                    if !state.gave_up_emitted {
                        warn!(
                            attempts,
                            window_secs = cfg.thresholds.window_secs,
                            "ghost-master: restart cap reached — staying up and serving probes"
                        );
                        ctx.telemetry.send(TelemetryEvent::GhostMasterGaveUp {
                            node: ctx.private_domain.clone(),
                            attempts,
                        });
                        state.gave_up_emitted = true;
                    }
                }
                GhostAction::Restart { attempt } => {
                    let master = format!("{}:{}", master.0, master.1);
                    // Persist the marker BEFORE acting so the restart this
                    // triggers can never reset its own cap.
                    append_restart_marker(&ctx.state_path, now);
                    warn!(
                        master = %master,
                        shape,
                        ghost_for_secs,
                        attempt,
                        "ghost-master: restarting through the boot path so the boot-time \
                         sanitizer can quarantine the dead sentinel state"
                    );
                    ctx.telemetry.send(TelemetryEvent::GhostMasterRestart {
                        node: ctx.private_domain.clone(),
                        attempt,
                        master: master.clone(),
                    });
                    let reason =
                        format!("ghost master {master} held for {ghost_for_secs}s ({shape})");
                    if ctx.restart_tx.send(reason).await.is_err() {
                        warn!("ghost-master: restart request could not be delivered");
                    }
                }
            }
        }
    }
}

/// Spawn the ghost-master watcher as a long-running background task. Only
/// meaningful on a Sentinel-managed node — the caller gates on
/// `sentinel_enabled`. Honors `GHOST_MASTER_HEAL_DISABLED=1`.
///
/// `local_sentinel_password` follows the same file-resolved contract as
/// every other local Sentinel client (see `sentinel_conf::conf_requires_auth`).
/// `restart_tx` delivers the one action this watcher ever takes to the
/// supervisor, which owns the child processes and the exit code.
pub fn spawn(
    config: &Config,
    telemetry: Telemetry,
    local_sentinel_password: String,
    restart_tx: mpsc::Sender<String>,
) {
    if disabled() {
        info!("ghost-master: GHOST_MASTER_HEAL_DISABLED=1, watcher inactive");
        return;
    }

    let cfg = WatcherConfig::from_env();
    let rank = seed_rank(
        config.is_primary(),
        &config.private_domain,
        &config.sentinel_hosts,
    );
    info!(
        poll_secs = cfg.poll_secs,
        dwell_secs = cfg.thresholds.dwell_secs,
        stagger_secs = cfg.thresholds.stagger_secs,
        max_restarts_per_window = cfg.thresholds.max_restarts_per_window,
        window_secs = cfg.thresholds.window_secs,
        rank,
        "ghost-master: starting watcher"
    );

    let ctx = WatcherContext {
        sentinel_url: sentinel_query::build_redis_url(
            "127.0.0.1",
            config.sentinel_port,
            &local_sentinel_password,
        ),
        redis_url: sentinel_query::build_redis_url(
            "127.0.0.1",
            config.redis_port,
            &config.redis_password,
        ),
        master_name: config.redis_master_name.clone(),
        redis_password: config.redis_password.clone(),
        private_domain: config.private_domain.clone(),
        declared_hosts: crate::boot_role::declared_hosts(config),
        declared_peers: crate::boot_role::peer_sentinel_addrs(config),
        rank,
        state_path: format!("{}/{}", config.data_dir, STATE_FILENAME),
        telemetry,
        restart_tx,
    };

    // Same respawn supervisor shape as link_heal/quorum: a panic surfaces
    // as a log line instead of silently losing the watcher.
    tokio::spawn(async move {
        let ctx = std::sync::Arc::new(ctx);
        loop {
            let ctx_for_run = std::sync::Arc::clone(&ctx);
            let handle = tokio::task::spawn(async move {
                let ctx = ctx_for_run;
                let mut state = WatcherState {
                    ghost_since: None,
                    gave_up_emitted: false,
                };
                // Give Sentinel its startup head start instead of logging a
                // guaranteed connection failure on the first poll — same
                // shape as quorum-sync.
                sleep(Duration::from_secs(cfg.poll_secs)).await;
                loop {
                    iteration(&ctx, &mut state, &cfg).await;
                    sleep(Duration::from_secs(cfg.poll_secs)).await;
                }
            });
            match handle.await {
                Ok(()) => warn!("ghost-master: run loop returned cleanly — respawning in 5s"),
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "ghost-master: run loop panicked — respawning in 5s")
                }
                Err(e) => warn!(error = %e, "ghost-master: join error — respawning in 5s"),
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

#[cfg(test)]
mod seed_rank_tests {
    use super::*;

    const HOSTS: &str =
        "redis-1.railway.internal:26379,redis-2.railway.internal:26379,redis-3.railway.internal:26379";

    #[test]
    fn the_env_primary_is_rank_zero() {
        assert_eq!(seed_rank(true, "redis-1.railway.internal", HOSTS), 0);
        // ...even when it sits later in the declared list.
        assert_eq!(seed_rank(true, "redis-3.railway.internal", HOSTS), 0);
    }

    #[test]
    fn replicas_rank_by_declared_position_after_the_primary() {
        assert_eq!(seed_rank(false, "redis-1.railway.internal", HOSTS), 1);
        assert_eq!(seed_rank(false, "redis-2.railway.internal", HOSTS), 2);
        assert_eq!(seed_rank(false, "redis-3.railway.internal", HOSTS), 3);
    }

    #[test]
    fn rank_lookup_is_normalized() {
        assert_eq!(seed_rank(false, "Redis-2.Railway.Internal.", HOSTS), 2);
    }

    #[test]
    fn an_undeclared_host_restarts_last() {
        assert_eq!(seed_rank(false, "redis-9.railway.internal", HOSTS), 4);
    }
}

#[cfg(test)]
mod consensus_tests {
    use super::*;

    fn addr(host: &str) -> (String, u16) {
        (host.to_string(), 6379)
    }

    #[test]
    fn a_strict_majority_is_consensus() {
        let answers = vec![addr("ghost"), addr("ghost"), addr("redis-1")];
        assert_eq!(consensus_answer(&answers, 3), Some(addr("ghost")));
    }

    #[test]
    fn votes_are_counted_normalized() {
        let answers = vec![
            addr("Ghost.Railway.Internal."),
            addr("ghost.railway.internal"),
        ];
        assert_eq!(
            consensus_answer(&answers, 3),
            Some(addr("Ghost.Railway.Internal."))
        );
    }

    #[test]
    fn a_minority_of_answers_is_never_consensus() {
        // 5 known sentinels, only 2 reachable (a partitioned minority): even
        // unanimous agreement among the reachable ones fails closed.
        let answers = vec![addr("ghost"), addr("ghost")];
        assert_eq!(consensus_answer(&answers, 5), None);
    }

    #[test]
    fn exactly_half_is_not_a_majority() {
        let answers = vec![addr("ghost"), addr("ghost")];
        assert_eq!(consensus_answer(&answers, 4), None);
    }

    #[test]
    fn a_single_agreeing_sentinel_is_never_enough() {
        // Membership of 1 would make one answer a "majority" — one
        // Sentinel's say-so must never fire a restart.
        let answers = vec![addr("ghost")];
        assert_eq!(consensus_answer(&answers, 1), None);
    }

    #[test]
    fn a_split_vote_is_no_consensus() {
        let answers = vec![addr("ghost"), addr("redis-1"), addr("redis-2")];
        assert_eq!(consensus_answer(&answers, 3), None);
    }

    #[test]
    fn no_answers_is_no_consensus() {
        assert_eq!(consensus_answer(&[], 3), None);
    }
}

#[cfg(test)]
mod decide_tests {
    use super::*;

    const T: Thresholds = Thresholds {
        dwell_secs: 900,
        stagger_secs: 60,
        max_restarts_per_window: 1,
        window_secs: 86400,
    };

    #[test]
    fn waits_out_the_dwell() {
        assert_eq!(decide(899, 0, 0, &T), GhostAction::Wait);
    }

    #[test]
    fn rank_zero_fires_at_the_dwell() {
        assert_eq!(decide(900, 0, 0, &T), GhostAction::Restart { attempt: 1 });
    }

    #[test]
    fn higher_ranks_hold_one_stagger_longer_each() {
        assert_eq!(decide(900, 1, 0, &T), GhostAction::Wait);
        assert_eq!(decide(959, 1, 0, &T), GhostAction::Wait);
        assert_eq!(decide(960, 1, 0, &T), GhostAction::Restart { attempt: 1 });
        assert_eq!(decide(1019, 2, 0, &T), GhostAction::Wait);
        assert_eq!(decide(1020, 2, 0, &T), GhostAction::Restart { attempt: 1 });
    }

    #[test]
    fn the_persisted_cap_stops_further_restarts() {
        assert_eq!(decide(900, 0, 1, &T), GhostAction::GiveUp { attempts: 1 });
    }

    #[test]
    fn the_cap_is_only_judged_after_a_full_hold() {
        // Right after its own restart a node can re-observe the condition
        // (its peers have not restarted yet) — it must dwell again, not
        // give up instantly.
        assert_eq!(decide(30, 0, 1, &T), GhostAction::Wait);
    }

    #[test]
    fn a_raised_cap_allows_another_attempt() {
        let t = Thresholds {
            max_restarts_per_window: 2,
            ..T
        };
        assert_eq!(decide(900, 0, 1, &t), GhostAction::Restart { attempt: 2 });
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn role_master_is_detected_with_crlf() {
        assert!(info_reports_master("# Replication\r\nrole:master\r\n"));
        assert!(!info_reports_master("# Replication\r\nrole:slave\r\n"));
        assert!(!info_reports_master(""));
    }

    #[test]
    fn failover_flag_is_matched_as_a_whole_token() {
        let fields = |flags: &str| vec!["flags".to_string(), flags.to_string()];
        assert_eq!(
            flags_show_failover_in_progress(&fields("master,failover_in_progress")),
            Some(true)
        );
        assert_eq!(
            flags_show_failover_in_progress(&fields("master,s_down,o_down")),
            Some(false)
        );
        assert_eq!(flags_show_failover_in_progress(&["name".to_string()]), None);
    }

    #[test]
    fn instance_addrs_need_both_fields() {
        let entry = |pairs: &[(&str, &str)]| -> Vec<String> {
            pairs
                .iter()
                .flat_map(|(k, v)| [k.to_string(), v.to_string()])
                .collect()
        };
        let entries = vec![
            entry(&[("ip", "redis-2"), ("port", "6379")]),
            entry(&[("ip", "redis-3")]),
            entry(&[("ip", ""), ("port", "6379")]),
            entry(&[("ip", "redis-4"), ("port", "junk")]),
        ];
        assert_eq!(
            addrs_from_instance_reply(&entries),
            vec![("redis-2".to_string(), 6379)]
        );
    }
}

#[cfg(test)]
mod merge_tests {
    use super::*;

    fn addr(host: &str, port: u16) -> (String, u16) {
        (host.to_string(), port)
    }

    #[test]
    fn union_dedupes_normalized_and_excludes_self() {
        let declared = vec![addr("redis-2", 26379), addr("redis-3", 26379)];
        let known = vec![
            addr("Redis-2.", 26379), // duplicate of a declared peer
            addr("redis-4", 26379),  // scale-up member
            addr("redis-1", 26379),  // self — excluded
        ];
        let merged = merge_peer_addrs(&declared, &known, "redis-1");
        assert_eq!(
            merged,
            vec![
                addr("redis-2", 26379),
                addr("redis-3", 26379),
                addr("redis-4", 26379)
            ]
        );
    }

    #[test]
    fn same_host_on_a_different_port_is_a_different_sentinel() {
        let merged = merge_peer_addrs(&[addr("redis-2", 26379)], &[addr("redis-2", 26380)], "self");
        assert_eq!(merged.len(), 2);
    }
}

#[cfg(test)]
mod state_file_tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn markers_persist_and_prune_at_read_time() {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap();
        append_restart_marker(path, 1_000);
        append_restart_marker(path, 50_000);
        assert_eq!(restarts_in_window(path, 50_100, 3_600), 1);
        assert_eq!(restarts_in_window(path, 50_100, 100_000), 2);
    }

    #[test]
    fn no_state_file_means_no_restarts() {
        assert_eq!(restarts_in_window("/nonexistent/path", 1_000, 3_600), 0);
    }
}
