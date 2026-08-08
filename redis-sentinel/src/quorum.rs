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

/// The strict majority of a membership of `live_other_sentinels + 1`, or
/// `None` when this node knows no live peer and must not act.
fn desired_quorum(live_other_sentinels: u32) -> Option<u32> {
    let n = live_other_sentinels + 1;
    if n < 2 {
        return None;
    }
    Some((n / 2 + 1).max(2))
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
fn field_value(fields: &[String], key: &str) -> Option<String> {
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
}

fn parse_peer_sentinels(entries: &[Vec<String>]) -> Vec<PeerSentinel> {
    entries
        .iter()
        .map(|entry| {
            let id = field_value(entry, "runid")
                .filter(|runid| !runid.is_empty())
                .unwrap_or_else(|| {
                    format!(
                        "{}:{}",
                        field_value(entry, "ip").unwrap_or_default(),
                        field_value(entry, "port").unwrap_or_default()
                    )
                });
            let s_down = !field_value(entry, "flags")
                .is_some_and(|flags| !flags.split(',').any(|flag| flag == "s_down"));
            PeerSentinel { id, s_down }
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
    down_since: std::collections::HashMap<String, i64>,
    last_reset_at: Option<i64>,
}

impl WatcherState {
    fn clear_observations(&mut self) {
        self.drift = None;
        self.down_since.clear();
    }
}

/// Spawn the quorum-sync watcher as a long-running background task. Only
/// meaningful on a Sentinel-managed node — the caller gates on
/// `sentinel_enabled`.
pub fn spawn(sentinel_port: u16, master_name: String) {
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

    tokio::spawn(async move {
        loop {
            let url = sentinel_url.clone();
            let name = master_name.clone();
            let handle = tokio::task::spawn(async move { run(url, name, cfg).await });
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

async fn run(sentinel_url: String, master_name: String, cfg: WatcherConfig) {
    let mut state = WatcherState::default();
    // Give Sentinel its startup head start instead of logging a guaranteed
    // connection failure on the first poll.
    sleep(Duration::from_secs(cfg.poll_secs)).await;
    loop {
        iteration(&sentinel_url, &master_name, &mut state, &cfg).await;
        sleep(Duration::from_secs(cfg.poll_secs)).await;
    }
}

async fn iteration(
    sentinel_url: &str,
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

    prune_dead_sentinels(&mut conn, master_name, &peers, &master_fields, state, cfg, now).await;
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
    if !due {
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
