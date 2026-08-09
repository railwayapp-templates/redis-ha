//! In-container self-heal for a replica whose link to the master is durably
//! down, or durably attached to the wrong master — the redis-ha analogue of
//! postgres-patroni's `self_heal` watcher.
//!
//! ## What Redis already handles (we do NOT duplicate)
//! A dropped replication link is normal and Redis retries it on its own — a
//! failover, a partial resync, or a master restart all drop
//! `master_link_status` to `down`, and a healthy replica clears it within
//! seconds once the target is reachable again.
//!
//! ## The gap this module fills
//! Four states a running Redis will not retry out of by itself:
//!  - **Pinned to a dead master.** Sentinel reconfigures every replica's
//!    `REPLICAOF` after a failover, but that update can fail to land on a
//!    node that was partitioned during the switch. The node keeps retrying a
//!    master that no longer exists.
//!  - **Wedged on an unreadable partial-resync backlog**, with no sync
//!    attempt in flight and the link simply staying down.
//!  - **Elected but never told.** Sentinel picks this same node as the new
//!    master and records that decision before its `REPLICAOF NO ONE` reaches
//!    it — if that node is unreachable at that exact moment, Sentinel's
//!    answer to `get-master-addr-by-name` already names it while its own
//!    `INFO replication` still reads `role:slave`, replicating a master that
//!    no longer exists as one. Reissuing `REPLICAOF` against that address
//!    would just point it at itself; the correct action is `REPLICAOF NO
//!    ONE`.
//!  - **Attached to the wrong master, link healthy.** A replica that
//!    completed its sync against a node that had just been demoted — a first
//!    boot racing a failover is how it happens in practice. Sentinel builds
//!    its slave table from the *master's* INFO, and a node chained under a
//!    demoted ex-master never appears there, so Sentinel never learns it
//!    exists and `+fix-slave-config` can never fire. The link is up, data
//!    flows through the chained ex-master, every health probe passes — and
//!    the node is silently outside the failover topology forever.
//!
//! All are invisible to a redeploy-free wait: nothing on the node is going
//! to change the target it is attached to, or nudge it into trying again.
//!
//! ## Detection
//! Poll local `INFO replication` (link status, whether a sync is currently in
//! flight, and which master this replica is attached to) plus local
//! Sentinel's `SENTINEL get-master-addr-by-name` — the authoritative current
//! master. The link states are judged from the link fields alone; the
//! wrong-master state is the replica's own `master_host`/`master_port`
//! disagreeing with Sentinel's answer. The fix target always comes from
//! Sentinel — the replica's own config is exactly what's suspect here.
//!
//! "Durable" means the broken observation repeated on every poll across a
//! dwell window — not a stale first-seen. The two failure families accrue
//! separate windows: a stalled link must stay stalled (down, no sync in
//! flight) for `LINK_HEAL_DWELL_SECONDS`; a wrong-master attachment must
//! disagree with Sentinel's answer for `LINK_HEAL_WRONG_MASTER_DWELL_SECONDS`
//! — long enough that "Sentinel is mid-failover and hasn't repointed us yet"
//! never qualifies, since Sentinel repoints the replicas it knows about
//! within seconds of a switch.
//!
//! ## Action
//! `REPLICAOF <sentinel_host> <sentinel_port>` on the local connection.
//! Redis discards whatever link state it was holding and restarts the
//! handshake from scratch — the same effect a full container redeploy
//! achieves by restarting the process, without the redeploy's cost or blast
//! radius. Targeting Sentinel's answer (rather than replaying the replica's
//! own possibly-stale config) fixes the pinned-to-dead-master case in the
//! same action as the wedged-backlog case. When Sentinel's answer names this
//! node itself, the action is `REPLICAOF NO ONE` instead — completing the
//! promotion it never received rather than replicating from itself.
//!
//! ## Safety
//! - Never acts on a master role — it has no link to lose, and there is
//!   nothing to reissue.
//! - Never acts while a sync is already in flight — that IS the recovery in
//!   progress; reissuing REPLICAOF would abort it and restart from zero.
//! - Backoff between attempts; capped attempts per rolling window, then it
//!   emits `LinkHealGaveUp` once and leaves the node for a human — or for
//!   backboard's redis-ha monitor, whose own broken-link redeploy exists
//!   precisely as the fallback for a node stuck past what this local watcher
//!   can fix (or one running an image old enough not to have it).
//!
//! ## State persistence
//! `<data_dir>/.link_heal_state` (key=value lines, mirroring
//! postgres-patroni's `.self_heal_state`). Carries `last_action_at` and a
//! rolling attempt history so backoff and the per-window cap survive
//! container restarts.
//!
//! ## Supervisor
//! Same shape as postgres-patroni's self-heal watcher and this crate's own
//! `enable_aof_after_rdb_load`: an outer respawn loop wraps the main loop in
//! `tokio::task::spawn` so a panic surfaces as a log line instead of aborting
//! redis-wrapper. 5s delay between respawns.

use common::{ConfigExt, RailwayEnv, Telemetry, TelemetryEvent};
use redis::aio::MultiplexedConnection;
use redis::Client;
use std::env;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{info, warn};

const STATE_FILENAME: &str = ".link_heal_state";

const DEFAULT_POLL_SECONDS: u64 = 15;
// Redis clears a dropped link on its own within seconds for every routine
// cause (failover, partial resync, a master restart). 20 minutes of
// consecutive brokenness is long enough that none of those explain it.
const DEFAULT_DWELL_SECONDS: u64 = 20 * 60;
// REPLICAOF is cheap and non-destructive (unlike postgres's reinitialize), so
// a shorter backoff than the postgres watcher's is fine.
const DEFAULT_ACTION_BACKOFF_SECONDS: u64 = 5 * 60;
const DEFAULT_MAX_ATTEMPTS_PER_WINDOW: u32 = 5;
const DEFAULT_WINDOW_SECONDS: u64 = 24 * 60 * 60;
// Sentinel repoints the replicas it knows about within seconds of a
// switch-master, so a mismatch that survives five minutes of consecutive
// polls is a replica Sentinel does not know exists — not a failover in
// flight. Shorter than the stalled-link dwell because a healthy-looking
// wrong attachment has no self-recovery to wait out.
const DEFAULT_WRONG_MASTER_DWELL_SECONDS: u64 = 5 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Master,
    Replica,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplicationSnapshot {
    role: Role,
    /// Only meaningful when `role == Replica`.
    link_down: bool,
    /// Only meaningful when `role == Replica`.
    sync_in_progress: bool,
    /// The master this replica is attached to (`master_host`/`master_port`).
    /// Only meaningful when `role == Replica`.
    master_addr: Option<(String, u16)>,
}

/// Parse the fields we need out of `INFO replication`. Real Redis output is
/// CRLF-terminated; `trim_end` on each line handles that.
fn parse_replication_info(info: &str) -> ReplicationSnapshot {
    let mut role = Role::Unknown;
    let mut link_down = false;
    let mut sync_in_progress = false;
    let mut master_host: Option<String> = None;
    let mut master_port: Option<u16> = None;
    for line in info.lines() {
        let line = line.trim_end();
        if let Some(v) = line.strip_prefix("role:") {
            role = match v {
                "master" => Role::Master,
                "slave" => Role::Replica,
                _ => Role::Unknown,
            };
        } else if let Some(v) = line.strip_prefix("master_link_status:") {
            link_down = v == "down";
        } else if let Some(v) = line.strip_prefix("master_sync_in_progress:") {
            sync_in_progress = v == "1";
        } else if let Some(v) = line.strip_prefix("master_host:") {
            let v = v.trim();
            if !v.is_empty() {
                master_host = Some(v.to_string());
            }
        } else if let Some(v) = line.strip_prefix("master_port:") {
            master_port = v.trim().parse::<u16>().ok();
        }
    }
    ReplicationSnapshot {
        role,
        link_down,
        sync_in_progress,
        master_addr: match (master_host, master_port) {
            (Some(host), Some(port)) => Some((host, port)),
            _ => None,
        },
    }
}

/// Whether the master this replica is attached to disagrees with Sentinel's
/// answer. Indeterminable (`None` on either side) is never a disagreement —
/// acting on a maybe is how a transient becomes an outage.
fn attached_to_wrong_master(
    master_addr: &Option<(String, u16)>,
    target: &Option<(String, u16)>,
) -> bool {
    match (master_addr, target) {
        (Some((mh, mp)), Some((th, tp))) => {
            crate::boot_role::normalize_host(mh) != crate::boot_role::normalize_host(th)
                || mp != tp
        }
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct Thresholds {
    dwell_secs: u64,
    wrong_master_dwell_secs: u64,
    action_backoff_secs: u64,
    max_attempts_per_window: u32,
    window_secs: u64,
}

#[derive(Debug, Clone)]
struct LinkHealInputs {
    now: i64,
    role: Role,
    link_down: bool,
    sync_in_progress: bool,
    /// Seconds this node has read (Replica, link down, no sync in flight) on
    /// every consecutive poll. `0` when not currently stalled.
    stalled_for_secs: u64,
    /// Whether the attached master disagrees with Sentinel's answer.
    wrong_master: bool,
    /// Seconds the disagreement has held on every consecutive poll. `0` when
    /// not currently disagreeing.
    mismatch_for_secs: u64,
    /// Sentinel's current answer for the master address. `None` when
    /// Sentinel is unreachable or gave no answer.
    target: Option<(String, u16)>,
    /// This node's own private-network address, to detect when Sentinel's
    /// answer names this same node — a promotion whose `REPLICAOF NO ONE`
    /// never landed, not a repoint target.
    own_private_domain: String,
    last_action_at: Option<i64>,
    action_attempts_in_window: u32,
    recovery_seen_after_action: bool,
    thresholds: Thresholds,
}

/// Which durable failure the action is answering — for the log line and for
/// tests; the corrective command is the same either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealReason {
    StalledLink,
    WrongMaster,
}

#[derive(Debug, Clone, PartialEq)]
enum LinkHealAction {
    NoOp,
    Wait,
    Reheal { attempt: u32, host: String, port: u16, reason: HealReason },
    /// Sentinel's answer is this node itself: complete the promotion via
    /// `REPLICAOF NO ONE` rather than repointing at our own address.
    PromoteSelf { attempt: u32, reason: HealReason },
    EmitRecovered { recovered_in_secs: u64, attempts: u32 },
    EmitGaveUp { attempts: u32 },
}

/// Pure (zero-I/O) decision function, unit-tested directly.
fn decide_link_heal(s: &LinkHealInputs) -> LinkHealAction {
    // Safety: only ever act on a replica. A master has no link to lose, and
    // Unknown means the INFO parse didn't recognize the role — never act on
    // a maybe.
    if !matches!(s.role, Role::Replica) {
        return LinkHealAction::NoOp;
    }

    // Recovery transition: we acted, and the replica reads healthy now —
    // link up AND attached to the master Sentinel names.
    if s.recovery_seen_after_action {
        let recovered_in_secs = s
            .last_action_at
            .map(|t| (s.now.saturating_sub(t)).max(0) as u64)
            .unwrap_or(0);
        return LinkHealAction::EmitRecovered {
            recovered_in_secs,
            attempts: s.action_attempts_in_window,
        };
    }

    // Escalation cap: stop nudging it; emit a giveup once.
    if s.action_attempts_in_window >= s.thresholds.max_attempts_per_window {
        return LinkHealAction::EmitGaveUp {
            attempts: s.action_attempts_in_window,
        };
    }

    // Backoff: respect minimum interval between attempts.
    if let Some(t) = s.last_action_at {
        let elapsed = s.now.saturating_sub(t).max(0) as u64;
        if elapsed < s.thresholds.action_backoff_secs {
            return LinkHealAction::Wait;
        }
    }

    // Safety: never act while a sync is already in flight — that IS the
    // recovery attempt, whether it's a natural retry or one we triggered.
    // The dwell windows keep accruing meanwhile, so a replica syncing from
    // the wrong master gets acted on the moment the sync lands.
    if s.sync_in_progress {
        return LinkHealAction::NoOp;
    }

    let stalled_fired = s.link_down && s.stalled_for_secs >= s.thresholds.dwell_secs;
    let wrong_master_fired =
        s.wrong_master && s.mismatch_for_secs >= s.thresholds.wrong_master_dwell_secs;

    if !stalled_fired && !wrong_master_fired {
        // Broken but still inside a dwell → keep watching. Healthy → nothing.
        return if s.link_down || s.wrong_master {
            LinkHealAction::Wait
        } else {
            LinkHealAction::NoOp
        };
    }

    // A down link is the stronger observation: when both fired, report the
    // stall — the wrong-master mismatch may just be its consequence.
    let reason = if stalled_fired {
        HealReason::StalledLink
    } else {
        HealReason::WrongMaster
    };

    let Some((host, port)) = s.target.clone() else {
        // Durably broken but no safe target to reissue REPLICAOF against —
        // wait rather than guess.
        return LinkHealAction::Wait;
    };

    if crate::boot_role::normalize_host(&host)
        == crate::boot_role::normalize_host(&s.own_private_domain)
    {
        return LinkHealAction::PromoteSelf {
            attempt: s.action_attempts_in_window + 1,
            reason,
        };
    }

    LinkHealAction::Reheal {
        attempt: s.action_attempts_in_window + 1,
        host,
        port,
        reason,
    }
}

// ====================================================================
// Stall-dwell tracking
// ====================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StallWindow {
    since: i64,
}

/// Advance the stall dwell window. The window opens the first time this node
/// reads (Replica, link down, no sync in flight) and keeps its original
/// `since` for as long as that observation repeats every poll — any poll
/// that doesn't match (recovered, or Redis is actively retrying) clears it,
/// so the dwell measures consecutive brokenness, never a stale first-seen.
fn accrue_stall_window(
    observed_stalled: bool,
    window: Option<StallWindow>,
    now: i64,
) -> Option<StallWindow> {
    if !observed_stalled {
        return None;
    }
    window.or(Some(StallWindow { since: now }))
}

// ====================================================================
// Redis / Sentinel I/O
// ====================================================================

async fn connect(url: &str) -> Option<MultiplexedConnection> {
    match Client::open(url) {
        Ok(client) => match client.get_multiplexed_async_connection().await {
            Ok(conn) => Some(conn),
            Err(e) => {
                warn!(error = %e, "link-heal: connection failed");
                None
            }
        },
        Err(e) => {
            warn!(error = %e, "link-heal: invalid URL");
            None
        }
    }
}

async fn fetch_replication_snapshot(conn: &mut MultiplexedConnection) -> Option<ReplicationSnapshot> {
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(conn)
        .await
        .map_err(|e| warn!(error = %e, "link-heal: INFO replication failed"))
        .ok()?;
    Some(parse_replication_info(&info))
}

/// Sentinel's current answer for the master's address — the authoritative
/// target, since a partitioned node's own `master_host` can be stale.
async fn sentinel_master_addr(
    conn: &mut MultiplexedConnection,
    master_name: &str,
) -> Option<(String, u16)> {
    let parts: Vec<String> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg(master_name)
        .query_async(conn)
        .await
        .map_err(|e| warn!(error = %e, "link-heal: SENTINEL get-master-addr-by-name failed"))
        .ok()?;
    if parts.len() != 2 {
        warn!("link-heal: unexpected SENTINEL get-master-addr-by-name response shape");
        return None;
    }
    let port = parts[1].parse::<u16>().ok()?;
    Some((parts[0].clone(), port))
}

async fn issue_replicaof(
    conn: &mut MultiplexedConnection,
    host: &str,
    port: u16,
) -> Result<(), redis::RedisError> {
    redis::cmd("REPLICAOF")
        .arg(host)
        .arg(port)
        .query_async::<()>(conn)
        .await
}

async fn issue_replicaof_no_one(conn: &mut MultiplexedConnection) -> Result<(), redis::RedisError> {
    redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query_async::<()>(conn)
        .await
}

// ====================================================================
// State-file helpers (same key=value shape as postgres-patroni's watcher)
// ====================================================================

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn read_state_field(state_path: &str, field: &str) -> Option<String> {
    let content = fs::read_to_string(state_path).ok()?;
    let prefix = format!("{field}=");
    content
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .next_back()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn write_state_field(state_path: &str, field: &str, value: &str) -> std::io::Result<()> {
    let prefix = format!("{field}=");
    let existing = fs::read_to_string(state_path).unwrap_or_default();
    let mut new_lines: Vec<String> = existing
        .lines()
        .filter(|line| !line.starts_with(&prefix))
        .map(|s| s.to_string())
        .collect();
    new_lines.push(format!("{field}={value}"));
    let mut out = new_lines.join("\n");
    out.push('\n');
    fs::write(state_path, out)
}

fn clear_state_field(state_path: &str, field: &str) -> std::io::Result<()> {
    let prefix = format!("{field}=");
    let Ok(existing) = fs::read_to_string(state_path) else {
        return Ok(());
    };
    let new_lines: Vec<String> = existing
        .lines()
        .filter(|line| !line.starts_with(&prefix))
        .map(|s| s.to_string())
        .collect();
    let mut out = new_lines.join("\n");
    if !out.is_empty() {
        out.push('\n');
    }
    fs::write(state_path, out)
}

/// Append an `attempt=<epoch>` line. Purely additive; pruning happens at
/// read-time via the rolling window.
fn append_attempt(state_path: &str, now: i64) {
    let existing = fs::read_to_string(state_path).unwrap_or_default();
    let mut lines: Vec<String> = existing.lines().map(|s| s.to_string()).collect();
    lines.push(format!("attempt={now}"));
    let mut out = lines.join("\n");
    out.push('\n');
    let _ = fs::write(state_path, out);
}

fn recent_action_count(state_path: &str, now: i64, window_secs: u64) -> u32 {
    let Ok(content) = fs::read_to_string(state_path) else {
        return 0;
    };
    let cutoff = now - window_secs as i64;
    content
        .lines()
        .filter_map(|line| line.strip_prefix("attempt="))
        .filter_map(|v| v.trim().parse::<i64>().ok())
        .filter(|t| *t >= cutoff)
        .count() as u32
}

// ====================================================================
// Supervisor (spawn + respawn loop)
// ====================================================================

/// True when the operator kill switch `LINK_HEAL_DISABLED=1` is set.
fn disabled() -> bool {
    env::var("LINK_HEAL_DISABLED").ok().as_deref() == Some("1")
}

#[derive(Debug, Clone)]
struct WatcherConfig {
    poll_secs: u64,
    thresholds: Thresholds,
}

impl WatcherConfig {
    fn from_env() -> Self {
        Self {
            poll_secs: u64::env_parse("LINK_HEAL_POLL_SECONDS", DEFAULT_POLL_SECONDS),
            thresholds: Thresholds {
                dwell_secs: u64::env_parse("LINK_HEAL_DWELL_SECONDS", DEFAULT_DWELL_SECONDS),
                wrong_master_dwell_secs: u64::env_parse(
                    "LINK_HEAL_WRONG_MASTER_DWELL_SECONDS",
                    DEFAULT_WRONG_MASTER_DWELL_SECONDS,
                ),
                action_backoff_secs: u64::env_parse(
                    "LINK_HEAL_ACTION_BACKOFF_SECONDS",
                    DEFAULT_ACTION_BACKOFF_SECONDS,
                ),
                max_attempts_per_window: u32::env_parse(
                    "LINK_HEAL_MAX_ATTEMPTS_PER_WINDOW",
                    DEFAULT_MAX_ATTEMPTS_PER_WINDOW,
                ),
                window_secs: u64::env_parse("LINK_HEAL_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
            },
        }
    }
}

/// Spawn the link-heal watcher as a long-running background task. Honors
/// `LINK_HEAL_DISABLED=1` as an operator kill switch. Only meaningful on a
/// Sentinel-managed node — the caller gates on `sentinel_enabled`.
///
/// `local_sentinel_password` is `""` unless the co-located Sentinel's
/// on-disk conf currently carries `requirepass` — resolved by the caller
/// from the file (see `sentinel_conf::conf_requires_auth`), never assumed
/// from the default, since a preserved conf that predates Sentinel auth
/// has no `requirepass` regardless of what new clusters now get, and
/// authenticating against a Sentinel that requires none is a hard
/// connection failure, not a no-op.
pub fn spawn(
    data_dir: String,
    redis_port: u16,
    redis_password: String,
    sentinel_port: u16,
    master_name: String,
    telemetry: Telemetry,
    local_sentinel_password: String,
) {
    if disabled() {
        info!("link-heal: LINK_HEAL_DISABLED=1, watcher inactive");
        return;
    }

    let cfg = WatcherConfig::from_env();
    info!(
        poll_secs = cfg.poll_secs,
        dwell_secs = cfg.thresholds.dwell_secs,
        wrong_master_dwell_secs = cfg.thresholds.wrong_master_dwell_secs,
        action_backoff_secs = cfg.thresholds.action_backoff_secs,
        max_attempts_per_window = cfg.thresholds.max_attempts_per_window,
        window_secs = cfg.thresholds.window_secs,
        "link-heal: starting watcher"
    );

    let redis_url = format!("redis://:{redis_password}@127.0.0.1:{redis_port}");
    let sentinel_url =
        crate::sentinel_query::sentinel_url("127.0.0.1", sentinel_port, &local_sentinel_password);
    let state_path = format!("{data_dir}/{STATE_FILENAME}");

    tokio::spawn(async move {
        loop {
            let ru = redis_url.clone();
            let su = sentinel_url.clone();
            let mn = master_name.clone();
            let sp = state_path.clone();
            let t = telemetry.clone();
            let c = cfg.clone();
            let h = tokio::task::spawn(async move { run(ru, su, mn, sp, t, c).await });
            match h.await {
                Ok(()) => warn!("link-heal: run loop returned cleanly — respawning in 5s"),
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "link-heal: run loop panicked — respawning in 5s")
                }
                Err(e) => warn!(error = %e, "link-heal: join error — respawning in 5s"),
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn run(
    redis_url: String,
    sentinel_url: String,
    master_name: String,
    state_path: String,
    telemetry: Telemetry,
    cfg: WatcherConfig,
) {
    let mut stall_window: Option<StallWindow> = None;
    let mut mismatch_window: Option<StallWindow> = None;
    // Rebuilt from disk so a container restart between an action and
    // stabilization still emits LinkHealRecovered when the link comes back.
    let mut action_pending_recovery: Option<i64> =
        read_state_field(&state_path, "last_action_at").and_then(|s| s.parse::<i64>().ok());
    // Dedupe LinkHealGaveUp: decide_link_heal returns EmitGaveUp on every
    // iteration while the cap is tripped.
    let mut gave_up_emitted = false;

    loop {
        iteration(
            &redis_url,
            &sentinel_url,
            &master_name,
            &state_path,
            &mut stall_window,
            &mut mismatch_window,
            &mut action_pending_recovery,
            &mut gave_up_emitted,
            &telemetry,
            &cfg,
        )
        .await;
        sleep(Duration::from_secs(cfg.poll_secs)).await;
    }
}

async fn iteration(
    redis_url: &str,
    sentinel_url: &str,
    master_name: &str,
    state_path: &str,
    stall_window: &mut Option<StallWindow>,
    mismatch_window: &mut Option<StallWindow>,
    action_pending_recovery: &mut Option<i64>,
    gave_up_emitted: &mut bool,
    telemetry: &Telemetry,
    cfg: &WatcherConfig,
) {
    let now = now_epoch();

    let Some(mut redis_conn) = connect(redis_url).await else {
        return;
    };
    let Some(snapshot) = fetch_replication_snapshot(&mut redis_conn).await else {
        return;
    };

    if !matches!(snapshot.role, Role::Replica) {
        *stall_window = None;
        *mismatch_window = None;
        return;
    }

    let observed_stalled = snapshot.link_down && !snapshot.sync_in_progress;
    *stall_window = accrue_stall_window(observed_stalled, *stall_window, now);
    let stalled_for_secs = stall_window
        .map(|w| now.saturating_sub(w.since).max(0) as u64)
        .unwrap_or(0);

    // Cheap local call; only needed as a fix target, but fetching it
    // unconditionally keeps the decision function's inputs simple and the
    // cost is one extra local round-trip per poll.
    let target = match connect(sentinel_url).await {
        Some(mut sentinel_conn) => sentinel_master_addr(&mut sentinel_conn, master_name).await,
        None => None,
    };

    // The wrong-master window accrues even while a sync is in flight — a
    // replica syncing FROM the wrong master is still wrong — but the decision
    // function refuses to act until the sync lands.
    let wrong_master = attached_to_wrong_master(&snapshot.master_addr, &target);
    *mismatch_window = accrue_stall_window(wrong_master, *mismatch_window, now);
    let mismatch_for_secs = mismatch_window
        .map(|w| now.saturating_sub(w.since).max(0) as u64)
        .unwrap_or(0);

    let last_action_at =
        read_state_field(state_path, "last_action_at").and_then(|s| s.parse::<i64>().ok());
    let action_attempts_in_window = recent_action_count(state_path, now, cfg.thresholds.window_secs);
    if action_attempts_in_window < cfg.thresholds.max_attempts_per_window {
        *gave_up_emitted = false;
    }

    // Recovered means healthy, not merely connected: the link is up AND the
    // attachment agrees with Sentinel. A wrong-master heal starts with the
    // link already up, so link state alone would declare victory instantly.
    let recovery_seen_after_action =
        action_pending_recovery.is_some() && !snapshot.link_down && !wrong_master;
    let node = RailwayEnv::private_domain();

    let snapshot_inputs = LinkHealInputs {
        now,
        role: snapshot.role,
        link_down: snapshot.link_down,
        sync_in_progress: snapshot.sync_in_progress,
        stalled_for_secs,
        wrong_master,
        mismatch_for_secs,
        target,
        own_private_domain: node.clone(),
        last_action_at,
        action_attempts_in_window,
        recovery_seen_after_action,
        thresholds: cfg.thresholds.clone(),
    };
    let action = decide_link_heal(&snapshot_inputs);

    match action {
        LinkHealAction::NoOp | LinkHealAction::Wait => {}
        LinkHealAction::Reheal { attempt, host, port, reason } => {
            match reason {
                HealReason::StalledLink => info!(
                    host = %host,
                    port,
                    attempt,
                    stalled_for_secs,
                    "link-heal: reissuing REPLICAOF on a durably broken link"
                ),
                HealReason::WrongMaster => info!(
                    host = %host,
                    port,
                    attempt,
                    mismatch_for_secs,
                    attached_to = ?snapshot.master_addr,
                    "link-heal: repointing a replica durably attached to the wrong master"
                ),
            }
            // Persist before the call so backoff/cap apply even if the
            // REPLICAOF call itself hangs or fails.
            let _ = write_state_field(state_path, "last_action_at", &now.to_string());
            append_attempt(state_path, now);
            *action_pending_recovery = Some(now);

            let master = format!("{host}:{port}");
            match issue_replicaof(&mut redis_conn, &host, port).await {
                Ok(()) => {
                    telemetry.send(TelemetryEvent::LinkHealTriggered {
                        node: node.clone(),
                        attempt,
                        master,
                    });
                }
                Err(e) => {
                    warn!(error = %e, "link-heal: REPLICAOF call failed");
                    telemetry.send(TelemetryEvent::LinkHealRequestFailed {
                        node: node.clone(),
                        attempt,
                        master,
                        error: e.to_string(),
                    });
                }
            }
        }
        LinkHealAction::PromoteSelf { attempt, reason } => {
            info!(
                attempt,
                stalled_for_secs,
                mismatch_for_secs,
                ?reason,
                "link-heal: completing a promotion that never landed via REPLICAOF NO ONE"
            );
            let _ = write_state_field(state_path, "last_action_at", &now.to_string());
            append_attempt(state_path, now);
            *action_pending_recovery = Some(now);

            let master = "NO ONE".to_string();
            match issue_replicaof_no_one(&mut redis_conn).await {
                Ok(()) => {
                    telemetry.send(TelemetryEvent::LinkHealTriggered {
                        node: node.clone(),
                        attempt,
                        master,
                    });
                }
                Err(e) => {
                    warn!(error = %e, "link-heal: REPLICAOF NO ONE call failed");
                    telemetry.send(TelemetryEvent::LinkHealRequestFailed {
                        node: node.clone(),
                        attempt,
                        master,
                        error: e.to_string(),
                    });
                }
            }
        }
        LinkHealAction::EmitRecovered {
            recovered_in_secs,
            attempts,
        } => {
            info!(recovered_in_secs, attempts, "link-heal: replica recovered");
            telemetry.send(TelemetryEvent::LinkHealRecovered {
                node,
                recovered_in_secs,
                attempts,
            });
            *action_pending_recovery = None;
            let _ = clear_state_field(state_path, "last_action_at");
            let _ = clear_state_field(state_path, "attempt");
        }
        LinkHealAction::EmitGaveUp { attempts } => {
            if !*gave_up_emitted {
                warn!(attempts, "link-heal: giving up, leaving node for a human");
                telemetry.send(TelemetryEvent::LinkHealGaveUp { node, attempts });
                *gave_up_emitted = true;
            }
        }
    }
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    fn info(role: &str, extra: &str) -> String {
        format!("# Replication\r\nrole:{role}\r\n{extra}")
    }

    #[test]
    fn master_has_no_link_fields() {
        let s = parse_replication_info(&info("master", "connected_slaves:2\r\n"));
        assert_eq!(s.role, Role::Master);
        assert!(!s.link_down);
        assert!(!s.sync_in_progress);
    }

    #[test]
    fn healthy_replica() {
        let s = parse_replication_info(&info(
            "slave",
            "master_link_status:up\r\nmaster_sync_in_progress:0\r\n",
        ));
        assert_eq!(s.role, Role::Replica);
        assert!(!s.link_down);
        assert!(!s.sync_in_progress);
    }

    #[test]
    fn replica_with_broken_link_idle() {
        let s = parse_replication_info(&info(
            "slave",
            "master_link_status:down\r\nmaster_sync_in_progress:0\r\n",
        ));
        assert!(s.link_down);
        assert!(!s.sync_in_progress);
    }

    #[test]
    fn replica_actively_resyncing() {
        let s = parse_replication_info(&info(
            "slave",
            "master_link_status:down\r\nmaster_sync_in_progress:1\r\n",
        ));
        assert!(s.link_down);
        assert!(s.sync_in_progress);
    }

    #[test]
    fn missing_role_is_unknown() {
        let s = parse_replication_info("# Replication\r\nconnected_slaves:0\r\n");
        assert_eq!(s.role, Role::Unknown);
    }

    #[test]
    fn replica_reports_its_attached_master() {
        let s = parse_replication_info(&info(
            "slave",
            "master_host:redis-1.railway.internal\r\nmaster_port:6379\r\nmaster_link_status:up\r\n",
        ));
        assert_eq!(
            s.master_addr,
            Some(("redis-1.railway.internal".to_string(), 6379))
        );
    }

    #[test]
    fn master_addr_needs_both_fields() {
        let s = parse_replication_info(&info("slave", "master_host:redis-1\r\n"));
        assert_eq!(s.master_addr, None);
    }
}

#[cfg(test)]
mod wrong_master_tests {
    use super::*;

    fn addr(host: &str, port: u16) -> Option<(String, u16)> {
        Some((host.to_string(), port))
    }

    #[test]
    fn agreement_is_not_wrong() {
        assert!(!attached_to_wrong_master(
            &addr("redis-2.railway.internal", 6379),
            &addr("redis-2.railway.internal", 6379)
        ));
    }

    #[test]
    fn host_comparison_is_normalized() {
        assert!(!attached_to_wrong_master(
            &addr("Redis-2.railway.internal.", 6379),
            &addr("redis-2.railway.internal", 6379)
        ));
    }

    #[test]
    fn different_host_is_wrong() {
        assert!(attached_to_wrong_master(
            &addr("redis-1.railway.internal", 6379),
            &addr("redis-2.railway.internal", 6379)
        ));
    }

    #[test]
    fn different_port_is_wrong() {
        assert!(attached_to_wrong_master(
            &addr("redis-2.railway.internal", 6380),
            &addr("redis-2.railway.internal", 6379)
        ));
    }

    #[test]
    fn indeterminable_is_never_wrong() {
        assert!(!attached_to_wrong_master(&None, &addr("redis-2", 6379)));
        assert!(!attached_to_wrong_master(&addr("redis-1", 6379), &None));
        assert!(!attached_to_wrong_master(&None, &None));
    }
}

#[cfg(test)]
mod stall_window_tests {
    use super::*;

    #[test]
    fn not_stalled_clears_the_window() {
        let w = Some(StallWindow { since: 100 });
        assert_eq!(accrue_stall_window(false, w, 200), None);
    }

    #[test]
    fn first_stalled_observation_opens_the_window() {
        assert_eq!(
            accrue_stall_window(true, None, 100),
            Some(StallWindow { since: 100 })
        );
    }

    #[test]
    fn consecutive_stalled_observations_keep_the_original_since() {
        let w = Some(StallWindow { since: 100 });
        assert_eq!(accrue_stall_window(true, w, 500), Some(StallWindow { since: 100 }));
    }
}

#[cfg(test)]
mod decide_tests {
    use super::*;

    fn base() -> LinkHealInputs {
        LinkHealInputs {
            now: 10_000,
            role: Role::Replica,
            link_down: true,
            sync_in_progress: false,
            stalled_for_secs: 9_999_999,
            wrong_master: false,
            mismatch_for_secs: 0,
            target: Some(("master.railway.internal".to_string(), 6379)),
            own_private_domain: "self.railway.internal".to_string(),
            last_action_at: None,
            action_attempts_in_window: 0,
            recovery_seen_after_action: false,
            thresholds: Thresholds {
                dwell_secs: 1200,
                wrong_master_dwell_secs: 300,
                action_backoff_secs: 300,
                max_attempts_per_window: 5,
                window_secs: 86400,
            },
        }
    }

    /// A wrong-master state: link healthy, attached master disagreeing with
    /// Sentinel past its dwell.
    fn wrong_master_base() -> LinkHealInputs {
        let mut s = base();
        s.link_down = false;
        s.stalled_for_secs = 0;
        s.wrong_master = true;
        s.mismatch_for_secs = 9_999_999;
        s
    }

    #[test]
    fn never_acts_on_a_master() {
        let mut s = base();
        s.role = Role::Master;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn never_acts_on_unknown_role() {
        let mut s = base();
        s.role = Role::Unknown;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn healthy_link_is_a_noop() {
        let mut s = base();
        s.link_down = false;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn never_acts_while_a_sync_is_in_flight() {
        let mut s = base();
        s.sync_in_progress = true;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn waits_out_the_dwell() {
        let mut s = base();
        s.stalled_for_secs = 10;
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);
    }

    #[test]
    fn waits_without_a_safe_target() {
        let mut s = base();
        s.target = None;
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);
    }

    #[test]
    fn reheals_once_every_guard_is_satisfied() {
        let s = base();
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::Reheal {
                attempt: 1,
                host: "master.railway.internal".to_string(),
                port: 6379,
                reason: HealReason::StalledLink
            }
        );
    }

    #[test]
    fn promotes_self_when_sentinel_names_this_node() {
        let mut s = base();
        s.target = Some((s.own_private_domain.clone(), 6379));
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::PromoteSelf {
                attempt: 1,
                reason: HealReason::StalledLink
            }
        );
    }

    #[test]
    fn promote_self_still_respects_backoff() {
        let mut s = base();
        s.target = Some((s.own_private_domain.clone(), 6379));
        s.last_action_at = Some(9_900);
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);
    }

    #[test]
    fn respects_backoff_between_attempts() {
        let mut s = base();
        s.last_action_at = Some(9_900);
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);
    }

    #[test]
    fn acts_again_once_backoff_elapses() {
        let mut s = base();
        s.last_action_at = Some(9_000);
        s.action_attempts_in_window = 1;
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::Reheal {
                attempt: 2,
                host: "master.railway.internal".to_string(),
                port: 6379,
                reason: HealReason::StalledLink
            }
        );
    }

    // --- wrong-master (healthy link, wrong attachment) ---

    #[test]
    fn wrong_master_reheals_with_the_link_up() {
        let s = wrong_master_base();
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::Reheal {
                attempt: 1,
                host: "master.railway.internal".to_string(),
                port: 6379,
                reason: HealReason::WrongMaster
            }
        );
    }

    #[test]
    fn wrong_master_waits_out_its_own_dwell() {
        let mut s = wrong_master_base();
        s.mismatch_for_secs = 60;
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);
    }

    #[test]
    fn wrong_master_never_acts_while_a_sync_is_in_flight() {
        let mut s = wrong_master_base();
        s.sync_in_progress = true;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn wrong_master_promotes_self_when_sentinel_names_this_node() {
        let mut s = wrong_master_base();
        s.target = Some((s.own_private_domain.clone(), 6379));
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::PromoteSelf {
                attempt: 1,
                reason: HealReason::WrongMaster
            }
        );
    }

    #[test]
    fn wrong_master_respects_backoff_and_cap() {
        let mut s = wrong_master_base();
        s.last_action_at = Some(9_900);
        assert_eq!(decide_link_heal(&s), LinkHealAction::Wait);

        let mut s = wrong_master_base();
        s.action_attempts_in_window = 5;
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::EmitGaveUp { attempts: 5 }
        );
    }

    #[test]
    fn a_down_link_wins_the_reason_when_both_fired() {
        let mut s = base();
        s.wrong_master = true;
        s.mismatch_for_secs = 9_999_999;
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::Reheal {
                attempt: 1,
                host: "master.railway.internal".to_string(),
                port: 6379,
                reason: HealReason::StalledLink
            }
        );
    }

    #[test]
    fn healthy_and_agreeing_is_a_noop_even_past_dwells() {
        let mut s = wrong_master_base();
        s.wrong_master = false;
        assert_eq!(decide_link_heal(&s), LinkHealAction::NoOp);
    }

    #[test]
    fn stops_at_the_attempt_cap() {
        let mut s = base();
        s.action_attempts_in_window = 5;
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::EmitGaveUp { attempts: 5 }
        );
    }

    #[test]
    fn recovery_wins_over_every_other_branch() {
        let mut s = base();
        s.recovery_seen_after_action = true;
        s.last_action_at = Some(9_400);
        s.action_attempts_in_window = 2;
        assert_eq!(
            decide_link_heal(&s),
            LinkHealAction::EmitRecovered {
                recovered_in_secs: 600,
                attempts: 2
            }
        );
    }
}

#[cfg(test)]
mod state_file_tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn state_path() -> (NamedTempFile, String) {
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_str().unwrap().to_string();
        (f, path)
    }

    #[test]
    fn write_then_read_round_trips() {
        let (_f, path) = state_path();
        write_state_field(&path, "last_action_at", "12345").unwrap();
        assert_eq!(read_state_field(&path, "last_action_at"), Some("12345".to_string()));
    }

    #[test]
    fn write_overwrites_the_prior_value() {
        let (_f, path) = state_path();
        write_state_field(&path, "last_action_at", "1").unwrap();
        write_state_field(&path, "last_action_at", "2").unwrap();
        assert_eq!(read_state_field(&path, "last_action_at"), Some("2".to_string()));
    }

    #[test]
    fn clear_removes_the_field() {
        let (_f, path) = state_path();
        write_state_field(&path, "last_action_at", "1").unwrap();
        clear_state_field(&path, "last_action_at").unwrap();
        assert_eq!(read_state_field(&path, "last_action_at"), None);
    }

    #[test]
    fn recent_action_count_excludes_attempts_outside_the_window() {
        let (_f, path) = state_path();
        append_attempt(&path, 1000);
        append_attempt(&path, 50_000);
        assert_eq!(recent_action_count(&path, 50_100, 3600), 1);
    }

    #[test]
    fn recent_action_count_is_zero_with_no_state_file() {
        assert_eq!(recent_action_count("/nonexistent/path", 1000, 3600), 0);
    }
}
