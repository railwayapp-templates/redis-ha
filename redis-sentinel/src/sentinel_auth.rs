//! First-boot Sentinel auth: reuse the cluster's shared `REDIS_PASSWORD` as
//! the Sentinel password, ON by default for new clusters, posture-matched
//! against the peers so a scale-up can never create a mixed-auth cluster.
//!
//! ## Why the posture matters more than the default
//! `requirepass` on a Sentinel is a listener-side setting that can only land
//! in sentinel.conf before Sentinel first starts: `SENTINEL CONFIG SET
//! requirepass` is refused ("ERR Invalid argument 'requirepass' to SENTINEL
//! CONFIG SET" — verified against redis 8.2.1; plain `CONFIG` is not even a
//! command in sentinel mode), and the wrapper preserves the conf after first
//! boot. Whatever auth posture a node first-boots with is therefore its
//! posture until the file is regenerated.
//!
//! Sentinels authenticate to each other on port 26379 with the same single
//! password, and a cluster split across postures cannot exchange failover
//! votes across that line: the authed node rejects the open nodes'
//! credential-less RPCs (`NOAUTH`), and the open nodes hard-fail on the
//! authed node's outbound AUTH ("Client sent AUTH, but no password is set").
//! A node scaled up onto an existing unauthenticated cluster MUST therefore
//! boot unauthenticated too, or it partitions itself out of failover
//! authorization while looking perfectly healthy on the data port.
//!
//! ## The decision
//! On the one boot that generates sentinel.conf, probe the peers this node
//! is joining (`SENTINEL_HOSTS` minus self) with a credential-less PING
//! ([`crate::sentinel_query::probe_unauthenticated`]):
//!  - ANY peer answers openly → boot OPEN. The cluster currently runs
//!    without auth, and one open peer is enough: writing `requirepass`
//!    would cut this node off from it (and, posture being cluster-wide,
//!    from all of them).
//!  - peers answer only with NOAUTH-class refusals → boot AUTHED, matching
//!    the cluster.
//!  - no peer answers at all → a genuinely fresh cluster (the env-primary
//!    booting first, or a whole new deployment) → AUTHED. This is the
//!    default-on win: every new cluster gets Sentinel auth with zero
//!    platform involvement, because the password is the `REDIS_PASSWORD`
//!    the template already stamps.
//!
//! ## Limits
//! - The probe sees the cluster as it is DURING this one boot. A scale-up
//!   racing a whole-cluster outage (every peer down at the instant the new
//!   node first-boots) reads as "no peer answered" and boots authed into
//!   what may be an open cluster. Each peer is probed twice to shave the
//!   mid-restart race, but a true simultaneous outage is indistinguishable
//!   from a fresh cluster by construction. The escape hatch is the same as
//!   for any wrong first-boot posture: remove the node's sentinel.conf (or
//!   set [`SENTINEL_AUTH_ENV`]`=false`) and restart.
//! - Existing unauthenticated clusters stay unauthenticated: their
//!   preserved confs are never rewritten by the wrapper, and any node they
//!   scale up posture-matches to open. Upgrading such a cluster to auth is
//!   a deliberate, whole-cluster operation — regenerate every node's
//!   sentinel.conf in one maintenance window — not something a rolling
//!   restart can ever converge to, precisely because each restarted node
//!   would keep matching the still-open majority.
//!
//! ## Kill switch
//! [`SENTINEL_AUTH_ENV`] — same convention as [`crate::boot_role::enabled`]:
//! only the literal `false` (trimmed, case-insensitive) disables it. Off,
//! the generated conf carries no auth lines and every internal client stays
//! credential-less on the sentinel port: exactly the pre-auth behavior.

use crate::boot_role::{enabled, peer_sentinel_addrs};
use crate::config::Config;
use crate::sentinel_query::{probe_unauthenticated, UnauthedProbe};
use common::ConfigExt;
use tracing::info;

/// Operator kill switch for Sentinel auth. Only the literal `false`
/// disables it; anything else (unset, empty, garbage) leaves it on.
pub const SENTINEL_AUTH_ENV: &str = "SENTINEL_AUTH";

/// Per-peer probe bound, in milliseconds.
const DEFAULT_PROBE_TIMEOUT_MS: u64 = 2000;

/// The auth posture of the cluster this node is joining, as observed by the
/// first-boot probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPosture {
    /// At least one peer serves credential-less clients.
    Open,
    /// At least one peer refused the credential-less probe with a
    /// NOAUTH-class error, and none answered openly.
    RequiresAuth,
    /// No peer gave a usable answer — or there are no peers to ask.
    NoAnswer,
}

/// Collapse per-peer probe results into the cluster's posture. Any open
/// answer wins: introducing `requirepass` next to even one open member is
/// exactly the mixed-auth split this module exists to prevent, so on an
/// (already inconsistent) cluster showing both answers this node joins the
/// open side rather than deepening the split.
pub fn aggregate_posture(probes: &[UnauthedProbe]) -> PeerPosture {
    if probes.iter().any(|p| *p == UnauthedProbe::Open) {
        return PeerPosture::Open;
    }
    if probes.iter().any(|p| *p == UnauthedProbe::RequiresAuth) {
        return PeerPosture::RequiresAuth;
    }
    PeerPosture::NoAnswer
}

/// Pure decision (mirrors `decide_empty_primary_boot` / `decide_link_heal`):
/// whether the sentinel.conf this FIRST boot is about to generate carries
/// `requirepass` / `sentinel sentinel-pass`. The empty-password arm is
/// defensive — `REDIS_PASSWORD` is required config — but auth with an empty
/// password must never be emitted.
pub fn decide_first_boot_auth(
    auth_enabled: bool,
    redis_password: &str,
    posture: PeerPosture,
) -> bool {
    if !auth_enabled || redis_password.is_empty() {
        return false;
    }
    match posture {
        PeerPosture::Open => false,
        PeerPosture::RequiresAuth | PeerPosture::NoAnswer => true,
    }
}

/// The single log line stating the decision and where it came from. Static
/// by construction — the password can never appear in it.
fn decision_log_line(auth_enabled: bool, posture: PeerPosture, auth_on: bool) -> &'static str {
    if !auth_enabled {
        return "sentinel auth: disabled by kill switch — generating an open sentinel.conf";
    }
    match (posture, auth_on) {
        (PeerPosture::Open, _) => {
            "sentinel auth: a peer sentinel answered without credentials — matching the \
             cluster's open posture (no requirepass, avoiding a mixed-auth cluster)"
        }
        (PeerPosture::RequiresAuth, _) => {
            "sentinel auth: peers require auth — writing requirepass to match the cluster"
        }
        (PeerPosture::NoAnswer, true) => {
            "sentinel auth: no peer answered — fresh cluster, auth on by default \
             (requirepass = the cluster's REDIS_PASSWORD)"
        }
        (PeerPosture::NoAnswer, false) => {
            "sentinel auth: off (no usable password)"
        }
    }
}

/// Probe every peer Sentinel's auth posture concurrently, each attempt
/// bounded by `SENTINEL_AUTH_PROBE_TIMEOUT_MS`. Two attempts per peer, for
/// the same reason `undeclared_master_is_member` makes two: boot is exactly
/// when a peer container may be mid-restart, and one refused handshake must
/// not be read as "no cluster here".
async fn probe_peers(config: &Config) -> PeerPosture {
    let peers = peer_sentinel_addrs(config);
    if peers.is_empty() {
        return PeerPosture::NoAnswer;
    }
    let deadline = std::time::Duration::from_millis(u64::env_parse(
        "SENTINEL_AUTH_PROBE_TIMEOUT_MS",
        DEFAULT_PROBE_TIMEOUT_MS,
    ));

    let mut set = tokio::task::JoinSet::new();
    for (host, port) in peers {
        set.spawn(async move {
            for _ in 0..2 {
                let probe = probe_unauthenticated(&host, port, deadline).await;
                if probe != UnauthedProbe::NoAnswer {
                    return probe;
                }
            }
            UnauthedProbe::NoAnswer
        });
    }

    let mut probes: Vec<UnauthedProbe> = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(probe) = joined {
            probes.push(probe);
        }
    }
    aggregate_posture(&probes)
}

/// The password the sentinel.conf this boot is about to generate protects
/// itself with: `""` (no auth lines — the crate-wide "empty = off"
/// convention) or the cluster's shared `REDIS_PASSWORD`. Only meaningful on
/// a FIRST boot: the caller generates a conf only when none exists, and a
/// preserved conf keeps whatever posture it already has (see the module
/// doc).
pub async fn first_boot_sentinel_password(config: &Config) -> String {
    let auth_enabled = enabled(std::env::var(SENTINEL_AUTH_ENV).ok().as_deref());
    let posture = if auth_enabled {
        probe_peers(config).await
    } else {
        // No probe when the switch is off — the answer cannot change.
        PeerPosture::NoAnswer
    };
    let auth_on = decide_first_boot_auth(auth_enabled, &config.redis_password, posture);
    info!("{}", decision_log_line(auth_enabled, posture, auth_on));
    if auth_on {
        config.redis_password.clone()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sentinel_query::UnauthedProbe::{NoAnswer, Open, RequiresAuth};

    // --- decide_first_boot_auth: the posture matrix ---

    #[test]
    fn a_fresh_cluster_gets_auth_by_default() {
        assert!(decide_first_boot_auth(true, "pw", PeerPosture::NoAnswer));
    }

    #[test]
    fn an_open_cluster_is_joined_open() {
        // The whole point: scaling up an existing unauthenticated cluster
        // must not mint the one node whose sentinel rejects everyone else's
        // failover votes.
        assert!(!decide_first_boot_auth(true, "pw", PeerPosture::Open));
    }

    #[test]
    fn an_authed_cluster_is_joined_authed() {
        assert!(decide_first_boot_auth(true, "pw", PeerPosture::RequiresAuth));
    }

    #[test]
    fn the_kill_switch_forces_open_regardless_of_posture() {
        assert!(!decide_first_boot_auth(false, "pw", PeerPosture::NoAnswer));
        assert!(!decide_first_boot_auth(false, "pw", PeerPosture::RequiresAuth));
        assert!(!decide_first_boot_auth(false, "pw", PeerPosture::Open));
    }

    #[test]
    fn an_empty_password_never_emits_auth() {
        // Defensive: REDIS_PASSWORD is required config, but requirepass with
        // an empty value must never be generated.
        assert!(!decide_first_boot_auth(true, "", PeerPosture::NoAnswer));
        assert!(!decide_first_boot_auth(true, "", PeerPosture::RequiresAuth));
    }

    // --- aggregate_posture ---

    #[test]
    fn no_probes_is_no_answer() {
        assert_eq!(aggregate_posture(&[]), PeerPosture::NoAnswer);
        assert_eq!(
            aggregate_posture(&[NoAnswer, NoAnswer]),
            PeerPosture::NoAnswer
        );
    }

    #[test]
    fn one_open_peer_makes_the_cluster_open() {
        assert_eq!(
            aggregate_posture(&[NoAnswer, Open, NoAnswer]),
            PeerPosture::Open
        );
    }

    #[test]
    fn refusals_without_any_open_answer_mean_auth() {
        assert_eq!(
            aggregate_posture(&[NoAnswer, RequiresAuth]),
            PeerPosture::RequiresAuth
        );
    }

    #[test]
    fn a_mixed_cluster_is_joined_on_the_open_side() {
        // Both answers present means the cluster is already split; joining
        // authed would deepen the split, joining open never widens it.
        assert_eq!(
            aggregate_posture(&[RequiresAuth, Open]),
            PeerPosture::Open
        );
    }

    // --- decision_log_line: static by construction, one line per outcome ---

    #[test]
    fn every_decision_logs_a_password_free_line() {
        // The lines are &'static str — they cannot embed the password. This
        // pins the mapping so a future edit cannot quietly log through a
        // formatting path instead.
        assert!(decision_log_line(false, PeerPosture::NoAnswer, false).contains("kill switch"));
        assert!(decision_log_line(true, PeerPosture::Open, false).contains("open posture"));
        assert!(decision_log_line(true, PeerPosture::RequiresAuth, true).contains("to match"));
        assert!(decision_log_line(true, PeerPosture::NoAnswer, true).contains("default"));
    }
}
