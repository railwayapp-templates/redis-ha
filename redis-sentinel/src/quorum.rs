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
//! ## Known limit
//! Sentinel's failover-leader majority counts every Sentinel it has EVER
//! known, including scale-down leftovers stuck in permanent sdown. Only
//! `SENTINEL RESET` forgets them; issuing resets is deliberately out of
//! scope here (it briefly wipes replica/peer state too). The quorum this
//! watcher maintains stays correct because of the sdown filter — the
//! election-majority denominator is the piece that stays inflated.
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

/// How many of the known peer Sentinels are alive — not flagged `s_down`.
/// The flags field is a comma-joined token list ("sentinel,s_down,disconnected");
/// matching whole tokens keeps a hypothetical flag containing the substring
/// from counting as down.
fn live_sentinel_count(entries: &[Vec<String>]) -> u32 {
    entries
        .iter()
        .filter(|entry| {
            field_value(entry, "flags")
                .is_some_and(|flags| !flags.split(',').any(|flag| flag == "s_down"))
        })
        .count() as u32
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

/// Spawn the quorum-sync watcher as a long-running background task. Only
/// meaningful on a Sentinel-managed node — the caller gates on
/// `sentinel_enabled`.
pub fn spawn(sentinel_port: u16, master_name: String) {
    if disabled() {
        info!("quorum-sync: QUORUM_SYNC_DISABLED=1, watcher inactive");
        return;
    }

    let poll_secs = u64::env_parse("QUORUM_SYNC_POLL_SECONDS", DEFAULT_POLL_SECONDS);
    let dwell_secs = u64::env_parse("QUORUM_SYNC_DWELL_SECONDS", DEFAULT_DWELL_SECONDS);
    info!(poll_secs, dwell_secs, "quorum-sync: starting watcher");

    // Sentinel has no auth by default.
    let sentinel_url = format!("redis://127.0.0.1:{sentinel_port}");

    tokio::spawn(async move {
        loop {
            let url = sentinel_url.clone();
            let name = master_name.clone();
            let handle =
                tokio::task::spawn(async move { run(url, name, poll_secs, dwell_secs).await });
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

async fn run(sentinel_url: String, master_name: String, poll_secs: u64, dwell_secs: u64) {
    let mut drift: Option<Drift> = None;
    // Give Sentinel its startup head start instead of logging a guaranteed
    // connection failure on the first poll.
    sleep(Duration::from_secs(poll_secs)).await;
    loop {
        iteration(&sentinel_url, &master_name, &mut drift, dwell_secs).await;
        sleep(Duration::from_secs(poll_secs)).await;
    }
}

async fn iteration(
    sentinel_url: &str,
    master_name: &str,
    drift: &mut Option<Drift>,
    dwell_secs: u64,
) {
    let now = now_epoch();

    let Some(mut conn) = crate::sentinel_query::connect(sentinel_url, CALL_DEADLINE).await else {
        // No Sentinel, no opinion — and no stale window either.
        *drift = None;
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
            *drift = None;
            return;
        }
    };
    let Some(current_quorum) = field_value(&master_fields, "quorum").and_then(|q| q.parse().ok())
    else {
        warn!("quorum-sync: no quorum field in SENTINEL master reply");
        *drift = None;
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
            *drift = None;
            return;
        }
    };

    let live_others = live_sentinel_count(&sentinel_entries);
    let desired = desired_quorum(live_others);

    let had_drift = drift.is_some();
    let (next_drift, set_now) = step_drift(current_quorum, desired, *drift, now, dwell_secs);
    *drift = next_drift;

    if let (false, Some(d)) = (had_drift, drift.as_ref()) {
        info!(
            current_quorum,
            desired = d.desired,
            live_sentinels = live_others + 1,
            dwell_secs,
            "quorum-sync: quorum is not a majority of the known sentinels — dwell started"
        );
    }

    let Some(new_quorum) = set_now else { return };

    match redis::cmd("SENTINEL")
        .arg("SET")
        .arg(master_name)
        .arg("quorum")
        .arg(new_quorum)
        .query_async::<()>(&mut conn)
        .await
    {
        Ok(()) => {
            info!(
                old = current_quorum,
                new = new_quorum,
                live_sentinels = live_others + 1,
                "quorum-sync: updated odown quorum to a majority of the known sentinels"
            );
            *drift = None;
        }
        Err(e) => {
            // Keep the window: the next poll retries immediately, dwell
            // already served.
            warn!(error = %e, "quorum-sync: SENTINEL SET quorum failed");
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

    #[test]
    fn counts_live_sentinels_only() {
        let entries = vec![
            entry(&[("name", "a"), ("flags", "sentinel")]),
            entry(&[("name", "b"), ("flags", "sentinel,s_down,disconnected")]),
            entry(&[("name", "c"), ("flags", "sentinel,disconnected")]),
        ];
        assert_eq!(live_sentinel_count(&entries), 2);
    }

    #[test]
    fn missing_flags_field_counts_as_down() {
        let entries = vec![entry(&[("name", "a")])];
        assert_eq!(live_sentinel_count(&entries), 0);
    }

    #[test]
    fn empty_membership_is_zero() {
        assert_eq!(live_sentinel_count(&[]), 0);
    }
}
