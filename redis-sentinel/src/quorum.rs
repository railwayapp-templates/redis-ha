//! Keeps the local Sentinel's odown quorum at a majority of the Sentinels it
//! actually knows — the registered membership — instead of a constant frozen
//! into `sentinel.conf` on first boot.
//!
//! ## Why the conf value goes stale
//! The wrapper writes `sentinel.conf` once and Sentinel owns it afterwards,
//! so the quorum a node monitors with is whatever the cluster looked like on
//! that node's FIRST boot, forever. A scale-up restamps the `SENTINEL_QUORUM`
//! env on every node, but a preserved conf never reads env again: after a
//! 3→5 scale the two new nodes monitor with quorum 3 while the three
//! original nodes keep quorum 2 — a cluster that cannot even agree on what
//! agreement means, and whose old majority makes odown (and therefore
//! failover) fire earlier than a 5-node cluster should.
//!
//! `SENTINEL_HOSTS` cannot be the source of truth either: it is authored
//! with the template's initial layout and scale-up deliberately does not
//! restamp it (gossip discovers the new peers). The registered membership —
//! what postgres-ha gets for free from etcd, where consensus size follows
//! DCS membership rather than any stamped constant — lives in Sentinel's own
//! gossip state. So that is what this watcher reads.
//!
//! ## Detection and action
//! Poll the local Sentinel: `SENTINEL sentinels <master>` for the peers it
//! knows, `SENTINEL master <master>` for the quorum it currently enforces.
//! Membership counts this node plus every known peer not flagged `s_down` —
//! the sdown filter is what lets a scale-down shrink the count back, since
//! Sentinel never forgets a peer on its own, it only marks it down. Desired
//! quorum is a strict majority, `n/2 + 1`.
//!
//! A drift must hold, at the same desired value, across a dwell window
//! (default 5 minutes) before the watcher issues
//! `SENTINEL SET <master> quorum <n>` — gossip discovery right after boot
//! grows the count one peer at a time, and acting on each step would churn
//! the conf for nothing. `SENTINEL SET` applies to the local Sentinel only
//! and persists via its own conf rewrite; every node runs this watcher, so
//! every node converges on its own.
//!
//! ## Safety
//! - Quorum gates odown only. Authorizing a failover always requires a
//!   majority of ALL known Sentinels regardless of quorum, so the worst a
//!   wrong value here can do is make odown eager or sluggish — never a
//!   unilateral failover.
//! - Never acts when it knows no live peer (`n < 2`): that is a boot
//!   transient, a partition, or not actually a cluster — all states where
//!   rewriting quorum answers the wrong question.
//! - Never sets a quorum below 2 — quorum 1 lets a single isolated Sentinel
//!   declare odown on its own say-so.
//!
//! ## Syncing the split-brain fence (`min-replicas-to-write`)
//! The fence in redis.conf is stamped at boot from `SENTINEL_QUORUM` and
//! goes stale exactly the way the sentinel quorum does: after a 3→5 scale
//! the founding nodes still require 1 acking replica, which only fences a
//! FULLY isolated master — a partition that traps one replica with the old
//! master leaves both sides writable until the network heals. The watcher
//! converges the local Redis to majority − 1 via `CONFIG SET`, behind the
//! same dwell.
//!
//! One deliberate asymmetry: the odown quorum follows the LIVE count (the
//! sdown filter is what lets scale-downs shrink it, and a wrong quorum is
//! harmless — failover still needs a majority of all known Sentinels), but
//! the fence follows the KNOWN count, sdown peers included. On the minority
//! side of a partition every lost peer reads `s_down`; a live-count fence
//! would lower itself after one dwell and re-admit writes on exactly the
//! master the majority side is failing over past. Known membership only
//! shrinks through the prune below, which refuses to run on a minority.
//!
//! `CONFIG SET` does not persist, and does not need to: redis.conf is
//! regenerated from env on every boot, and the watcher re-converges a node
//! whose env stamp is stale after one dwell.
//!
//! ## Pruning dead Sentinels (`SENTINEL RESET`)
//! The quorum above is safe against scale-downs because of the sdown
//! filter, but Sentinel's **failover-leader majority** is not: it counts
//! every Sentinel ever known, and Sentinel never forgets a peer on its own.
//! After a 5→3 scale-down the denominator stays 5 — the 3 survivors must
//! vote unanimously, and losing any one of them during an incident makes
//! failover impossible exactly when it is needed.
//!
//! `SENTINEL RESET <master>` is the only way to forget dead peers, and it is
//! a blunt one: it briefly wipes the local Sentinel's replica and peer state
//! too (re-learned from the master's INFO within ~10s and from hello gossip
//! within seconds). So the prune fires only when every one of these holds:
//!  - some known peer has been continuously `s_down` for the prune dwell
//!    (default 30 minutes — no redeploy or restart explains that), and
//!  - the live members still form a strict majority of everything this
//!    Sentinel knows, OR every s_down peer's announced hostname has answered
//!    authoritative NXDOMAIN on every probe for the whole dwell (see
//!    `crate::dns_probe`). A scale-down leaving a live majority passes the
//!    first arm; a multi-pair scale-down that deletes a majority at once
//!    (7→3) passes the second, because on Railway NXDOMAIN means the
//!    control plane affirms nothing runs behind that name — a partition
//!    yields answers or SERVFAIL, never NXDOMAIN, so the minority side of
//!    one can never satisfy either arm and keeps its fence up while the
//!    other side fails over. A majority that NXDOMAINed continuously for
//!    the whole dwell ran no containers for all of it: there was no second
//!    writer to protect against, and a node that later returns rejoins as
//!    a replica through boot-role resolution, and
//!  - the master reads healthy from here (no `s_down`/`o_down`/
//!    `failover_in_progress` flag) — never during an incident, when the
//!    wiped state would be needed most, and
//!  - the last reset is at least the backoff ago (default 1 hour), so a
//!    peer that never comes back cannot make this node reset in a loop.
//!
//! Nodes prune independently on their own dwell clocks (they booted at
//! different times), so the cluster never resets in lockstep.
//!
//! ## Supervisor
//! Same shape as `link_heal`: an outer respawn loop wraps the poll loop so a
//! panic surfaces as a log line instead of aborting redis-wrapper.

use common::ConfigExt;
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

const DEFAULT_POLL_SECONDS: u64 = 60;
const DEFAULT_DWELL_SECONDS: u64 = 5 * 60;
// A peer gone for half an hour is a removed node, not a restart: redeploys
// and crash-restarts on the platform complete in minutes.
const DEFAULT_PRUNE_DWELL_SECONDS: u64 = 30 * 60;
const DEFAULT_PRUNE_BACKOFF_SECONDS: u64 = 60 * 60;
const CALL_DEADLINE: Duration = Duration::from_secs(5);
// DNS probes are loopback-to-local-resolver; anything slower than this is
// "no answer", which the verdict already treats as "keep the fence".
const PROBE_DEADLINE: Duration = Duration::from_secs(3);

/// The strict majority of a membership of `live_other_sentinels + 1`, or
/// `None` when this node knows no live peer and must not act.
fn desired_quorum(live_other_sentinels: u32) -> Option<u32> {
    let n = live_other_sentinels + 1;
    if n < 2 {
        return None;
    }
    Some((n / 2 + 1).max(2))
}

/// The split-brain fence a membership of `known_other_sentinels + 1`
/// requires: majority − 1 over the KNOWN membership, sdown peers included —
/// see the module doc for why the fence must not follow the live count.
/// `None` when this node knows no peer at all: a boot transient or not a
/// cluster, and in both cases whatever redis.conf stamped stays put.
fn desired_fence(known_other_sentinels: u32) -> Option<u32> {
    let n = known_other_sentinels + 1;
    if n < 2 {
        return None;
    }
    Some(crate::redis_conf::min_replicas_to_write((n / 2 + 1).max(2)))
}

/// Whether the live members (this node plus every non-sdown peer) still
/// form a strict majority of everything this Sentinel knows — the gate that
/// keeps the minority side of a partition from pruning the majority.
fn live_is_majority_of_known(peers: &[PeerSentinel]) -> bool {
    let known = peers.len() as u32 + 1;
    let live = peers.iter().filter(|peer| !peer.s_down).count() as u32 + 1;
    live * 2 > known
}

/// A quorum disagreement being watched across the dwell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Drift {
    desired: u32,
    since: i64,
}

/// Advance the drift window. Returns the new window plus the value to SET
/// now, if the same desired quorum has disagreed with the current one for
/// the whole dwell. Any change in the desired value restarts the window —
/// the dwell measures a *stable* disagreement, not the churn of gossip
/// discovery mid-boot.
fn step_drift(
    current_quorum: u32,
    desired: Option<u32>,
    drift: Option<Drift>,
    now: i64,
    dwell_secs: u64,
) -> (Option<Drift>, Option<u32>) {
    let Some(desired) = desired else {
        return (None, None);
    };
    if desired == current_quorum {
        return (None, None);
    }
    match drift {
        Some(d) if d.desired == desired => {
            if now.saturating_sub(d.since) >= dwell_secs as i64 {
                (Some(d), Some(desired))
            } else {
                (Some(d), None)
            }
        }
        _ => (Some(Drift { desired, since: now }), None),
    }
}

/// The value for `key` in a flat field-value reply (`SENTINEL master`).
/// `pub(crate)`: `demote_on_shutdown` reuses this to read the same
/// `SENTINEL MASTER` reply's `flags`/`ip`/`port` fields.
pub(crate) fn field_value(fields: &[String], key: &str) -> Option<String> {
    fields
        .chunks(2)
        .find(|pair| pair.len() == 2 && pair[0] == key)
        .map(|pair| pair[1].clone())
}

/// A known peer Sentinel, as the local Sentinel sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerSentinel {
    /// Stable identity for dwell tracking: the runid when Sentinel reports
    /// one, `ip:port` otherwise. A peer with no identity at all still gets a
    /// key (":"), which only ever matters if Sentinel returns a degenerate
    /// entry — and then all of them collapse onto one dwell, which is fine.
    id: String,
    /// The flags field is a comma-joined token list
    /// ("sentinel,s_down,disconnected"); matching whole tokens keeps a
    /// hypothetical flag containing the substring from counting as down. A
    /// missing flags field counts as down — never treat unparseable as alive.
    s_down: bool,
    /// The address the peer announced itself under — a hostname, since every
    /// node runs `sentinel announce-hostnames yes`. This is the name the
    /// deletion probe resolves; empty when Sentinel reported none, which
    /// simply means that peer can never be proven Gone.
    host: String,
}

fn parse_peer_sentinels(entries: &[Vec<String>]) -> Vec<PeerSentinel> {
    entries
        .iter()
        .map(|entry| {
            let host = field_value(entry, "ip").unwrap_or_default();
            let id = field_value(entry, "runid")
                .filter(|runid| !runid.is_empty())
                .unwrap_or_else(|| {
                    format!(
                        "{}:{}",
                        host,
                        field_value(entry, "port").unwrap_or_default()
                    )
                });
            let s_down = !field_value(entry, "flags")
                .is_some_and(|flags| !flags.split(',').any(|flag| flag == "s_down"));
            PeerSentinel { id, s_down, host }
        })
        .collect()
}

/// Whether the master itself reads healthy from this Sentinel — the gate
/// that keeps a prune from wiping local state mid-incident.
fn master_is_healthy(master_fields: &[String]) -> bool {
    field_value(master_fields, "flags").is_some_and(|flags| {
        !flags
            .split(',')
            .any(|flag| flag == "s_down" || flag == "o_down" || flag == "failover_in_progress")
    })
}

/// Advance the per-peer sdown dwell windows. Peers that recovered or
/// disappeared drop out; peers newly seen down open a window keyed to their
/// identity. Returns true when at least one peer has been continuously down
/// for the whole dwell — the signal that a `SENTINEL RESET` would actually
/// forget something.
fn step_prune(
    peers: &[PeerSentinel],
    down_since: &mut std::collections::HashMap<String, i64>,
    now: i64,
    dwell_secs: u64,
) -> bool {
    down_since.retain(|id, _| peers.iter().any(|p| p.s_down && p.id == *id));
    for peer in peers {
        if peer.s_down {
            down_since.entry(peer.id.clone()).or_insert(now);
        }
    }
    down_since
        .values()
        .any(|since| now.saturating_sub(*since) >= dwell_secs as i64)
}

/// Advance the per-peer continuous-NXDOMAIN windows from this poll's probe
/// verdicts, which cover exactly the currently-`s_down` peers. A window
/// survives only while its peer stays in that set AND this poll's verdict is
/// Gone again — any recovery, ambiguity, or disappearance deletes it.
fn step_gone(
    verdicts: &[(String, crate::dns_probe::NameVerdict)],
    gone_since: &mut std::collections::HashMap<String, i64>,
    now: i64,
) {
    use crate::dns_probe::NameVerdict::Gone;
    gone_since.retain(|id, _| verdicts.iter().any(|(vid, v)| vid == id && *v == Gone));
    for (id, verdict) in verdicts {
        if *verdict == Gone {
            gone_since.entry(id.clone()).or_insert(now);
        }
    }
}

/// The minority-prune waiver: every peer currently `s_down` has answered
/// authoritative NXDOMAIN on every probe for the whole prune dwell. That is
/// the one state where "the majority is missing" cannot be a partition —
/// the missing services no longer exist — so forgetting them un-fences a
/// cluster whose other side is provably not there. A single s_down peer
/// that still resolves (or merely fails to answer) vetoes the waiver: it
/// may be the majority side of a partition, mid-failover past this master.
fn all_sdown_peers_gone_past_dwell(
    peers: &[PeerSentinel],
    gone_since: &std::collections::HashMap<String, i64>,
    now: i64,
    dwell_secs: u64,
) -> bool {
    let sdown: Vec<&PeerSentinel> = peers.iter().filter(|p| p.s_down).collect();
    !sdown.is_empty()
        && sdown.iter().all(|peer| {
            gone_since
                .get(&peer.id)
                .is_some_and(|since| now.saturating_sub(*since) >= dwell_secs as i64)
        })
}

fn now_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// True when the operator kill switch `QUORUM_SYNC_DISABLED=1` is set.
fn disabled() -> bool {
    env::var("QUORUM_SYNC_DISABLED").ok().as_deref() == Some("1")
}

#[derive(Debug, Clone, Copy)]
struct WatcherConfig {
    poll_secs: u64,
    dwell_secs: u64,
    prune_dwell_secs: u64,
    prune_backoff_secs: u64,
    prune_disabled: bool,
}

/// Everything the watcher remembers between polls. Cleared wholesale on any
/// observation failure — unobserved time never counts toward a dwell.
#[derive(Debug, Default)]
struct WatcherState {
    drift: Option<Drift>,
    fence_drift: Option<Drift>,
    down_since: std::collections::HashMap<String, i64>,
    /// Per-peer continuous-NXDOMAIN windows, keyed like `down_since`. An
    /// entry exists only while every consecutive probe of that peer's
    /// hostname came back authoritatively Gone — any other verdict (records,
    /// NODATA, SERVFAIL, timeout) deletes it, so unobserved or ambiguous
    /// time never counts toward the deletion dwell.
    gone_since: std::collections::HashMap<String, i64>,
    last_reset_at: Option<i64>,
}

impl WatcherState {
    fn clear_observations(&mut self) {
        self.drift = None;
        self.fence_drift = None;
        self.down_since.clear();
        self.gone_since.clear();
    }
}

/// Spawn the quorum-sync watcher as a long-running background task. Only
/// meaningful on a Sentinel-managed node — the caller gates on
/// `sentinel_enabled`. Talks to the colocated Sentinel for the quorum and to
/// the colocated Redis for the fence, both over loopback.
pub fn spawn(
    sentinel_port: u16,
    redis_port: u16,
    redis_password: String,
    master_name: String,
    private_domain: String,
) {
    if disabled() {
        info!("quorum-sync: QUORUM_SYNC_DISABLED=1, watcher inactive");
        return;
    }

    let cfg = WatcherConfig {
        poll_secs: u64::env_parse("QUORUM_SYNC_POLL_SECONDS", DEFAULT_POLL_SECONDS),
        dwell_secs: u64::env_parse("QUORUM_SYNC_DWELL_SECONDS", DEFAULT_DWELL_SECONDS),
        prune_dwell_secs: u64::env_parse(
            "SENTINEL_PRUNE_DWELL_SECONDS",
            DEFAULT_PRUNE_DWELL_SECONDS,
        ),
        prune_backoff_secs: u64::env_parse(
            "SENTINEL_PRUNE_BACKOFF_SECONDS",
            DEFAULT_PRUNE_BACKOFF_SECONDS,
        ),
        prune_disabled: env::var("SENTINEL_PRUNE_DISABLED").ok().as_deref() == Some("1"),
    };
    info!(
        poll_secs = cfg.poll_secs,
        dwell_secs = cfg.dwell_secs,
        prune_dwell_secs = cfg.prune_dwell_secs,
        prune_backoff_secs = cfg.prune_backoff_secs,
        prune_disabled = cfg.prune_disabled,
        "quorum-sync: starting watcher"
    );

    // Sentinel has no auth by default.
    let sentinel_url = format!("redis://127.0.0.1:{sentinel_port}");
    let redis_url = format!("redis://:{redis_password}@127.0.0.1:{redis_port}");

    tokio::spawn(async move {
        loop {
            let surl = sentinel_url.clone();
            let rurl = redis_url.clone();
            let name = master_name.clone();
            let domain = private_domain.clone();
            let handle = tokio::task::spawn(async move { run(surl, rurl, name, domain, cfg).await });
            match handle.await {
                Ok(()) => warn!("quorum-sync: run loop returned cleanly — respawning in 5s"),
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "quorum-sync: run loop panicked — respawning in 5s")
                }
                Err(e) => warn!(error = %e, "quorum-sync: join error — respawning in 5s"),
            }
            sleep(Duration::from_secs(5)).await;
        }
    });
}

/// Retrofit a preserved sentinel.conf (written by an image predating
/// `sentinel announce-ip`, and never regenerated — Sentinel owns the file
/// after first boot) at runtime: without it this Sentinel keeps gossiping
/// its container IP, which changes on every redeploy and can never satisfy
/// the deletion probe on peers. `SENTINEL CONFIG SET` is persisted by
/// Sentinel's own conf rewrite, peers absorb the address switch keyed by
/// runid, and on a fresh conf that already carries both directives this is
/// a literal no-op. Best-effort: a failure leaves the probe's IP-literal
/// guard as the backstop.
async fn ensure_announce_identity(sentinel_url: &str, private_domain: &str) {
    let Some(mut conn) = crate::sentinel_query::connect(sentinel_url, CALL_DEADLINE).await else {
        warn!("quorum-sync: sentinel unreachable for the announce-identity retrofit");
        return;
    };
    for (key, value) in [
        ("announce-hostnames", "yes"),
        ("announce-ip", private_domain),
    ] {
        if let Err(e) = redis::cmd("SENTINEL")
            .arg("CONFIG")
            .arg("SET")
            .arg(key)
            .arg(value)
            .query_async::<()>(&mut conn)
            .await
        {
            warn!(error = %e, key, "quorum-sync: SENTINEL CONFIG SET failed");
        }
    }
}

async fn run(
    sentinel_url: String,
    redis_url: String,
    master_name: String,
    private_domain: String,
    cfg: WatcherConfig,
) {
    let mut state = WatcherState::default();
    // Give Sentinel its startup head start instead of logging a guaranteed
    // connection failure on the first poll.
    sleep(Duration::from_secs(cfg.poll_secs)).await;
    ensure_announce_identity(&sentinel_url, &private_domain).await;
    loop {
        iteration(&sentinel_url, &redis_url, &master_name, &mut state, &cfg).await;
        sleep(Duration::from_secs(cfg.poll_secs)).await;
    }
}

async fn iteration(
    sentinel_url: &str,
    redis_url: &str,
    master_name: &str,
    state: &mut WatcherState,
    cfg: &WatcherConfig,
) {
    let now = now_epoch();

    let Some(mut conn) = crate::sentinel_query::connect(sentinel_url, CALL_DEADLINE).await else {
        // No Sentinel, no opinion — and no stale windows either.
        state.clear_observations();
        return;
    };

    let master_fields: Vec<String> = match redis::cmd("SENTINEL")
        .arg("master")
        .arg(master_name)
        .query_async(&mut conn)
        .await
    {
        Ok(fields) => fields,
        Err(e) => {
            warn!(error = %e, "quorum-sync: SENTINEL master failed");
            state.clear_observations();
            return;
        }
    };
    let Some(current_quorum) = field_value(&master_fields, "quorum").and_then(|q| q.parse().ok())
    else {
        warn!("quorum-sync: no quorum field in SENTINEL master reply");
        state.clear_observations();
        return;
    };

    let sentinel_entries: Vec<Vec<String>> = match redis::cmd("SENTINEL")
        .arg("sentinels")
        .arg(master_name)
        .query_async(&mut conn)
        .await
    {
        Ok(entries) => entries,
        Err(e) => {
            warn!(error = %e, "quorum-sync: SENTINEL sentinels failed");
            state.clear_observations();
            return;
        }
    };

    let peers = parse_peer_sentinels(&sentinel_entries);
    let live_others = peers.iter().filter(|peer| !peer.s_down).count() as u32;
    let desired = desired_quorum(live_others);

    let had_drift = state.drift.is_some();
    let (next_drift, set_now) = step_drift(current_quorum, desired, state.drift, now, cfg.dwell_secs);
    state.drift = next_drift;

    if let (false, Some(d)) = (had_drift, state.drift.as_ref()) {
        info!(
            current_quorum,
            desired = d.desired,
            live_sentinels = live_others + 1,
            dwell_secs = cfg.dwell_secs,
            "quorum-sync: quorum is not a majority of the known sentinels — dwell started"
        );
    }

    if let Some(new_quorum) = set_now {
        set_quorum(&mut conn, master_name, current_quorum, new_quorum, live_others, state).await;
    }

    sync_fence(redis_url, peers.len() as u32, state, cfg, now).await;

    prune_dead_sentinels(&mut conn, master_name, &peers, &master_fields, state, cfg, now).await;
}

/// Converge the local Redis's `min-replicas-to-write` to majority − 1 of the
/// KNOWN membership, behind the same dwell as the quorum. Runs on every node
/// — the fence is inert on a replica and live the moment Sentinel promotes
/// it, the same reason every node carries masterauth.
async fn sync_fence(
    redis_url: &str,
    known_others: u32,
    state: &mut WatcherState,
    cfg: &WatcherConfig,
    now: i64,
) {
    let Some(mut conn) = crate::sentinel_query::connect(redis_url, CALL_DEADLINE).await else {
        // No Redis, no opinion — and no stale window either.
        state.fence_drift = None;
        return;
    };

    let reply: Vec<String> = match redis::cmd("CONFIG")
        .arg("GET")
        .arg("min-replicas-to-write")
        .query_async(&mut conn)
        .await
    {
        Ok(reply) => reply,
        Err(e) => {
            warn!(error = %e, "quorum-sync: CONFIG GET min-replicas-to-write failed");
            state.fence_drift = None;
            return;
        }
    };
    let Some(current) = field_value(&reply, "min-replicas-to-write").and_then(|v| v.parse().ok())
    else {
        warn!("quorum-sync: no min-replicas-to-write in CONFIG GET reply");
        state.fence_drift = None;
        return;
    };

    let desired = desired_fence(known_others);

    let had_drift = state.fence_drift.is_some();
    let (next_drift, set_now) =
        step_drift(current, desired, state.fence_drift, now, cfg.dwell_secs);
    state.fence_drift = next_drift;

    if let (false, Some(d)) = (had_drift, state.fence_drift.as_ref()) {
        info!(
            current_fence = current,
            desired = d.desired,
            known_sentinels = known_others + 1,
            dwell_secs = cfg.dwell_secs,
            "quorum-sync: fence is not majority − 1 of the known sentinels — dwell started"
        );
    }

    let Some(new_fence) = set_now else { return };
    match redis::cmd("CONFIG")
        .arg("SET")
        .arg("min-replicas-to-write")
        .arg(new_fence)
        .query_async::<()>(&mut conn)
        .await
    {
        Ok(()) => {
            info!(
                old = current,
                new = new_fence,
                known_sentinels = known_others + 1,
                "quorum-sync: updated min-replicas-to-write to majority − 1 of the known sentinels"
            );
            state.fence_drift = None;
        }
        Err(e) => {
            // Keep the window: the next poll retries immediately, dwell
            // already served.
            warn!(error = %e, "quorum-sync: CONFIG SET min-replicas-to-write failed");
        }
    }
}

async fn set_quorum(
    conn: &mut redis::aio::MultiplexedConnection,
    master_name: &str,
    current_quorum: u32,
    new_quorum: u32,
    live_others: u32,
    state: &mut WatcherState,
) {
    match redis::cmd("SENTINEL")
        .arg("SET")
        .arg(master_name)
        .arg("quorum")
        .arg(new_quorum)
        .query_async::<()>(conn)
        .await
    {
        Ok(()) => {
            info!(
                old = current_quorum,
                new = new_quorum,
                live_sentinels = live_others + 1,
                "quorum-sync: updated odown quorum to a majority of the known sentinels"
            );
            state.drift = None;
        }
        Err(e) => {
            // Keep the window: the next poll retries immediately, dwell
            // already served.
            warn!(error = %e, "quorum-sync: SENTINEL SET quorum failed");
        }
    }
}

/// Forget peers that have been down past the prune dwell, by resetting the
/// local Sentinel's view of the master set. Gated on a healthy master and a
/// long backoff — see the module doc.
#[allow(clippy::too_many_arguments)]
async fn prune_dead_sentinels(
    conn: &mut redis::aio::MultiplexedConnection,
    master_name: &str,
    peers: &[PeerSentinel],
    master_fields: &[String],
    state: &mut WatcherState,
    cfg: &WatcherConfig,
    now: i64,
) {
    if cfg.prune_disabled {
        return;
    }
    let due = step_prune(peers, &mut state.down_since, now, cfg.prune_dwell_secs);

    // Probe every s_down peer's announced hostname and advance the
    // continuous-NXDOMAIN windows on every poll, not just once the sdown
    // dwell is served — deletion evidence accumulates in parallel with it.
    let mut verdicts = Vec::new();
    for peer in peers.iter().filter(|p| p.s_down) {
        let verdict = if peer.host.is_empty() {
            crate::dns_probe::NameVerdict::ExistsOrUnknown
        } else {
            crate::dns_probe::probe_name(&peer.host, PROBE_DEADLINE).await
        };
        verdicts.push((peer.id.clone(), verdict));
    }
    step_gone(&verdicts, &mut state.gone_since, now);

    if !due {
        return;
    }
    if !live_is_majority_of_known(peers)
        && !all_sdown_peers_gone_past_dwell(peers, &state.gone_since, now, cfg.prune_dwell_secs)
    {
        // The peers due for pruning may be the majority side of a partition,
        // not removed nodes — forgetting them would shrink the fence until
        // this node's master re-admitted writes the other side has already
        // failed over past. The windows stay open; a real scale-down passes
        // this gate as soon as the survivors are the majority of what's left,
        // and a deleted majority (a multi-pair scale-down like 7→3) passes
        // once every missing peer has answered NXDOMAIN for the whole dwell —
        // the one proof that there is no other side to fail over to.
        return;
    }
    if !master_is_healthy(master_fields) {
        // Mid-incident is exactly when the state RESET wipes is needed;
        // the windows stay open and the prune re-fires once things settle.
        return;
    }
    if let Some(t) = state.last_reset_at {
        if now.saturating_sub(t) < cfg.prune_backoff_secs as i64 {
            return;
        }
    }

    let dead: Vec<&str> = state
        .down_since
        .iter()
        .filter(|(_, since)| now.saturating_sub(**since) >= cfg.prune_dwell_secs as i64)
        .map(|(id, _)| id.as_str())
        .collect();

    match redis::cmd("SENTINEL")
        .arg("RESET")
        .arg(master_name)
        .query_async::<i64>(conn)
        .await
    {
        Ok(_) => {
            info!(
                pruned = ?dead,
                dwell_secs = cfg.prune_dwell_secs,
                "quorum-sync: reset the local sentinel to forget peers down past the dwell"
            );
            state.last_reset_at = Some(now);
            // Fresh discovery starts now; a peer that is genuinely dead
            // stays forgotten, one that was merely slow re-registers via
            // gossip within seconds and never re-enters a window.
            state.down_since.clear();
            state.gone_since.clear();
        }
        Err(e) => {
            warn!(error = %e, "quorum-sync: SENTINEL RESET failed");
        }
    }
}

#[cfg(test)]
mod desired_tests {
    use super::*;

    #[test]
    fn alone_means_no_opinion() {
        assert_eq!(desired_quorum(0), None);
    }

    #[test]
    fn majorities() {
        assert_eq!(desired_quorum(1), Some(2)); // 2 sentinels
        assert_eq!(desired_quorum(2), Some(2)); // 3 sentinels
        assert_eq!(desired_quorum(3), Some(3)); // 4 sentinels
        assert_eq!(desired_quorum(4), Some(3)); // 5 sentinels
        assert_eq!(desired_quorum(6), Some(4)); // 7 sentinels
    }

    #[test]
    fn fence_alone_means_no_opinion() {
        assert_eq!(desired_fence(0), None);
    }

    #[test]
    fn fence_is_majority_minus_one_of_the_known_membership() {
        assert_eq!(desired_fence(1), Some(1)); // 2 known
        assert_eq!(desired_fence(2), Some(1)); // 3 known
        assert_eq!(desired_fence(4), Some(2)); // 5 known
        assert_eq!(desired_fence(6), Some(3)); // 7 known
    }
}

#[cfg(test)]
mod prune_gate_tests {
    use super::*;

    fn peer(id: &str, s_down: bool) -> PeerSentinel {
        PeerSentinel {
            id: id.to_string(),
            s_down,
            host: format!("{id}.railway.internal"),
        }
    }

    // 5→3 scale-down: 3 live of 5 known — a real removal may be forgotten.
    #[test]
    fn a_live_majority_may_prune() {
        let peers = [
            peer("a", false),
            peer("b", false),
            peer("c", true),
            peer("d", true),
        ];
        assert!(live_is_majority_of_known(&peers));
    }

    // The minority side of a 5-node partition (this node + one peer): the
    // sdown peers are the live majority, not removed nodes.
    #[test]
    fn a_live_minority_may_not_prune() {
        let peers = [
            peer("a", false),
            peer("b", true),
            peer("c", true),
            peer("d", true),
        ];
        assert!(!live_is_majority_of_known(&peers));
    }

    // A fully isolated node (every peer sdown) must never forget the cluster.
    #[test]
    fn an_isolated_node_may_not_prune() {
        let peers = [peer("a", true), peer("b", true)];
        assert!(!live_is_majority_of_known(&peers));
    }

    // Exactly half is not a majority: 2 live of 4 known blocks.
    #[test]
    fn exactly_half_is_not_a_majority() {
        let peers = [peer("a", false), peer("b", true), peer("c", true)];
        assert!(!live_is_majority_of_known(&peers));
    }
}

#[cfg(test)]
mod gone_waiver_tests {
    use super::*;
    use crate::dns_probe::NameVerdict::{ExistsOrUnknown, Gone};
    use std::collections::HashMap;

    const DWELL: u64 = 1800;

    fn peer(id: &str, s_down: bool) -> PeerSentinel {
        PeerSentinel {
            id: id.to_string(),
            s_down,
            host: format!("{id}.railway.internal"),
        }
    }

    #[test]
    fn a_gone_verdict_opens_and_keeps_a_window() {
        let mut gone = HashMap::new();
        step_gone(&[("a".into(), Gone)], &mut gone, 1000);
        step_gone(&[("a".into(), Gone)], &mut gone, 2000);
        assert_eq!(gone.get("a"), Some(&1000));
    }

    #[test]
    fn any_other_verdict_deletes_the_window() {
        let mut gone = HashMap::from([("a".to_string(), 1000_i64)]);
        step_gone(&[("a".into(), ExistsOrUnknown)], &mut gone, 2000);
        assert!(gone.is_empty());
        // And so does dropping out of the s_down set entirely.
        let mut gone = HashMap::from([("a".to_string(), 1000_i64)]);
        step_gone(&[], &mut gone, 2000);
        assert!(gone.is_empty());
    }

    // The 7→3 case: every missing peer NXDOMAIN for the whole dwell.
    #[test]
    fn waiver_passes_when_every_sdown_peer_is_gone_past_the_dwell() {
        let peers = [
            peer("live", false),
            peer("b", true),
            peer("c", true),
            peer("d", true),
        ];
        let gone = HashMap::from([
            ("b".to_string(), 1000_i64),
            ("c".to_string(), 1200_i64),
            ("d".to_string(), 1400_i64),
        ]);
        assert!(all_sdown_peers_gone_past_dwell(
            &peers,
            &gone,
            1400 + DWELL as i64,
            DWELL
        ));
        // One window still short of the dwell blocks it.
        assert!(!all_sdown_peers_gone_past_dwell(
            &peers,
            &gone,
            1200 + DWELL as i64,
            DWELL
        ));
    }

    // The partition case: one missing peer still resolves — it may be the
    // majority side mid-failover, so the waiver must veto.
    #[test]
    fn waiver_fails_while_any_sdown_peer_still_resolves() {
        let peers = [peer("live", false), peer("b", true), peer("c", true)];
        let gone = HashMap::from([("b".to_string(), 1000_i64)]);
        assert!(!all_sdown_peers_gone_past_dwell(&peers, &gone, 999_999, DWELL));
    }

    #[test]
    fn waiver_needs_at_least_one_sdown_peer() {
        let peers = [peer("live", false)];
        assert!(!all_sdown_peers_gone_past_dwell(
            &peers,
            &HashMap::new(),
            999_999,
            DWELL
        ));
    }
}

#[cfg(test)]
mod step_tests {
    use super::*;

    const DWELL: u64 = 300;

    #[test]
    fn agreement_clears_everything() {
        let (drift, set) = step_drift(3, Some(3), Some(Drift { desired: 3, since: 0 }), 1000, DWELL);
        assert_eq!(drift, None);
        assert_eq!(set, None);
    }

    #[test]
    fn no_opinion_clears_everything() {
        let (drift, set) = step_drift(2, None, Some(Drift { desired: 3, since: 0 }), 1000, DWELL);
        assert_eq!(drift, None);
        assert_eq!(set, None);
    }

    #[test]
    fn disagreement_opens_a_window_without_acting() {
        let (drift, set) = step_drift(2, Some(3), None, 1000, DWELL);
        assert_eq!(drift, Some(Drift { desired: 3, since: 1000 }));
        assert_eq!(set, None);
    }

    #[test]
    fn stable_disagreement_fires_after_the_dwell() {
        let window = Some(Drift { desired: 3, since: 1000 });
        let (_, set) = step_drift(2, Some(3), window, 1000 + DWELL as i64, DWELL);
        assert_eq!(set, Some(3));
    }

    #[test]
    fn still_inside_the_dwell_keeps_waiting() {
        let window = Some(Drift { desired: 3, since: 1000 });
        let (drift, set) = step_drift(2, Some(3), window, 1100, DWELL);
        assert_eq!(drift, window);
        assert_eq!(set, None);
    }

    #[test]
    fn a_changed_desired_value_restarts_the_window() {
        let window = Some(Drift { desired: 3, since: 1000 });
        let (drift, set) = step_drift(2, Some(4), window, 5000, DWELL);
        assert_eq!(drift, Some(Drift { desired: 4, since: 5000 }));
        assert_eq!(set, None);
    }
}

#[cfg(test)]
mod count_tests {
    use super::*;

    fn entry(pairs: &[(&str, &str)]) -> Vec<String> {
        pairs
            .iter()
            .flat_map(|(k, v)| [k.to_string(), v.to_string()])
            .collect()
    }

    fn live(entries: &[Vec<String>]) -> u32 {
        parse_peer_sentinels(entries)
            .iter()
            .filter(|peer| !peer.s_down)
            .count() as u32
    }

    #[test]
    fn counts_live_sentinels_only() {
        let entries = vec![
            entry(&[("name", "a"), ("flags", "sentinel")]),
            entry(&[("name", "b"), ("flags", "sentinel,s_down,disconnected")]),
            entry(&[("name", "c"), ("flags", "sentinel,disconnected")]),
        ];
        assert_eq!(live(&entries), 2);
    }

    #[test]
    fn missing_flags_field_counts_as_down() {
        let entries = vec![entry(&[("name", "a")])];
        assert_eq!(live(&entries), 0);
    }

    #[test]
    fn empty_membership_is_zero() {
        assert_eq!(live(&[]), 0);
    }

    #[test]
    fn identity_prefers_runid_then_ip_port() {
        let entries = vec![
            entry(&[("runid", "abc123"), ("flags", "sentinel")]),
            entry(&[("runid", ""), ("ip", "host-2"), ("port", "26379"), ("flags", "sentinel")]),
        ];
        let peers = parse_peer_sentinels(&entries);
        assert_eq!(peers[0].id, "abc123");
        assert_eq!(peers[1].id, "host-2:26379");
    }
}

#[cfg(test)]
mod master_health_tests {
    use super::*;

    fn fields(flags: &str) -> Vec<String> {
        vec!["name".into(), "mymaster".into(), "flags".into(), flags.into()]
    }

    #[test]
    fn plain_master_is_healthy() {
        assert!(master_is_healthy(&fields("master")));
    }

    #[test]
    fn sdown_odown_or_failover_is_not() {
        assert!(!master_is_healthy(&fields("master,s_down")));
        assert!(!master_is_healthy(&fields("master,o_down,s_down")));
        assert!(!master_is_healthy(&fields("master,failover_in_progress")));
    }

    #[test]
    fn missing_flags_is_not_healthy() {
        assert!(!master_is_healthy(&["name".to_string(), "mymaster".to_string()]));
    }
}

#[cfg(test)]
mod prune_tests {
    use super::*;
    use std::collections::HashMap;

    const DWELL: u64 = 1800;

    fn peer(id: &str, s_down: bool) -> PeerSentinel {
        PeerSentinel {
            id: id.to_string(),
            s_down,
            host: format!("{id}.railway.internal"),
        }
    }

    #[test]
    fn a_down_peer_opens_a_window_without_firing() {
        let mut down = HashMap::new();
        let due = step_prune(&[peer("a", true)], &mut down, 1000, DWELL);
        assert!(!due);
        assert_eq!(down.get("a"), Some(&1000));
    }

    #[test]
    fn fires_once_the_dwell_is_served() {
        let mut down = HashMap::from([("a".to_string(), 1000_i64)]);
        assert!(step_prune(
            &[peer("a", true)],
            &mut down,
            1000 + DWELL as i64,
            DWELL
        ));
    }

    #[test]
    fn recovery_clears_the_window() {
        let mut down = HashMap::from([("a".to_string(), 1000_i64)]);
        let due = step_prune(&[peer("a", false)], &mut down, 999_999, DWELL);
        assert!(!due);
        assert!(down.is_empty());
    }

    #[test]
    fn disappearance_clears_the_window() {
        let mut down = HashMap::from([("a".to_string(), 1000_i64)]);
        let due = step_prune(&[], &mut down, 999_999, DWELL);
        assert!(!due);
        assert!(down.is_empty());
    }

    #[test]
    fn windows_are_tracked_per_peer() {
        let mut down = HashMap::new();
        step_prune(&[peer("a", true), peer("b", false)], &mut down, 1000, DWELL);
        // b goes down later; a recovers. Only b's window should remain, at
        // its own start time.
        let due = step_prune(&[peer("a", false), peer("b", true)], &mut down, 2000, DWELL);
        assert!(!due);
        assert_eq!(down.get("a"), None);
        assert_eq!(down.get("b"), Some(&2000));
        // b alone serves out its dwell and fires.
        assert!(step_prune(
            &[peer("a", false), peer("b", true)],
            &mut down,
            2000 + DWELL as i64,
            DWELL
        ));
    }

    #[test]
    fn an_existing_window_keeps_its_original_since() {
        let mut down = HashMap::new();
        step_prune(&[peer("a", true)], &mut down, 1000, DWELL);
        step_prune(&[peer("a", true)], &mut down, 5000, DWELL);
        assert_eq!(down.get("a"), Some(&1000));
    }
}
