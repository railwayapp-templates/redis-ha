//! Keeps the co-located Sentinel's credentials equal to this node's pinned
//! data password, without waiting for the control plane.
//!
//! A rotation (`credentials.rs`) sets three things on Sentinel at runtime —
//! the default user's inbound password (`ACL SETUSER`), the outbound peer
//! credential (`SENTINEL CONFIG SET sentinel-pass`) and the monitored
//! master's `auth-pass` — and Sentinel persists each one on its own conf
//! rewrite. Anything that later writes one of them from a stale copy, or a
//! Sentinel that comes back from a restart with a conf the rotation never
//! reached, leaves a Sentinel that authenticates to nobody: every peer
//! `s_down`, `SENTINEL CKQUORUM` answering NOQUORUM, the rotation's `verify`
//! refusing forever, until an operator runs `SENTINEL CONFIG SET
//! sentinel-pass` by hand. This loop is that operator.
//!
//! Every few seconds (`CREDENTIAL_RECONCILE_SECONDS`, default 5, `0` turns
//! the loop off) it reads the live `sentinel-pass`, the master's `auth-pass`
//! as Sentinel last persisted it (`SENTINEL MASTER` does not expose the
//! password; the conf does, and Sentinel flushes it on every `SENTINEL SET`)
//! and whether Sentinel still accepts the pin, compares each with
//! `persisted_requirepass`, and re-applies only what differs. Nothing is
//! logged while everything agrees; one line without secrets records a
//! repair.
//!
//! A rotation in flight (`.railway_rotation` present) stages a second
//! password: PREPARE moves `sentinel-pass` and `auth-pass` to the new value
//! while the pin is still the old one, and MEMBER moves the pin. For the
//! journal's lifetime both values are acceptable and a repair targets the
//! staged one — the direction PREPARE already committed to. A tick that
//! finds a rotation operation running skips rather than interleave with it.
//!
//! Only a Sentinel whose conf requires auth is reconciled: an open Sentinel
//! has no inbound password to keep, and giving it a `sentinel-pass` would
//! make it AUTH against open peers, which Redis treats as a hard failure.
use crate::{
    credentials::{staged_rotation, ROTATION},
    redis_conf::{persisted_requirepass, unquote_conf_value},
    sentinel_query::build_redis_url,
};
use common::config::ConfigExt;
use redis::{aio::MultiplexedConnection, Client};
use std::{fmt, time::Duration};
use tokio::time::{sleep, timeout};
use tracing::{info, warn};

const DEFAULT_INTERVAL_SECONDS: u64 = 5;
const CALL_DEADLINE: Duration = Duration::from_secs(5);

/// What the loop could observe on the live Sentinel and its persisted conf.
/// `None` means "could not read", which is never treated as a difference.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Observed {
    /// `SENTINEL CONFIG GET sentinel-pass`; `Some("")` when unset.
    pub sentinel_pass: Option<String>,
    /// `sentinel auth-pass <master> <value>` from sentinel.conf.
    pub auth_pass: Option<String>,
    /// Whether an `AUTH <pin>` against Sentinel succeeds.
    pub accepts_pin: Option<bool>,
}

/// One command the loop will send. Values are the passwords to apply;
/// `Debug` never prints them so a failed test assertion stays secret-free.
#[derive(Clone, PartialEq, Eq)]
pub enum Repair {
    /// `ACL SETUSER default ><pin>`: Sentinel must accept the pin inbound.
    AcceptInbound(String),
    /// `SENTINEL CONFIG SET sentinel-pass <value>`.
    SentinelPass(String),
    /// `SENTINEL SET <master> auth-pass <value>`.
    AuthPass(String),
}
impl fmt::Debug for Repair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
impl Repair {
    fn name(&self) -> &'static str {
        match self {
            Repair::AcceptInbound(_) => "inbound-password",
            Repair::SentinelPass(_) => "sentinel-pass",
            Repair::AuthPass(_) => "auth-pass",
        }
    }
}

/// Pure decision: live values against the pin (and the staged password of
/// a rotation in flight) → the commands that bring Sentinel back in line.
/// `pin` is the password redis.conf carries; `staged` the `.railway_rotation`
/// target when it differs from the pin.
pub fn plan(observed: &Observed, pin: &str, staged: Option<&str>) -> Vec<Repair> {
    let staged = staged.filter(|s| *s != pin && !s.is_empty());
    let acceptable = |value: &str| value == pin || staged == Some(value);
    let target = staged.unwrap_or(pin).to_string();
    let mut repairs = Vec::new();
    if observed.accepts_pin == Some(false) {
        repairs.push(Repair::AcceptInbound(pin.to_string()));
    }
    if let Some(value) = observed.sentinel_pass.as_deref() {
        if !acceptable(value) {
            repairs.push(Repair::SentinelPass(target.clone()));
        }
    }
    if let Some(value) = observed.auth_pass.as_deref() {
        if !acceptable(value) {
            repairs.push(Repair::AuthPass(target));
        }
    }
    repairs
}

/// The master's `auth-pass` as Sentinel last wrote it. Sentinel itself
/// writes the value unquoted; the rotation's rewrite writes it quoted.
pub fn conf_auth_pass(contents: &str, master_name: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut fields = line.trim().splitn(4, char::is_whitespace);
        let keyword = fields.next()?;
        let option = fields.next()?;
        let name = fields.next()?;
        if !keyword.eq_ignore_ascii_case("sentinel")
            || !option.eq_ignore_ascii_case("auth-pass")
            || name != master_name
        {
            return None;
        }
        Some(unquote_conf_value(fields.next().unwrap_or("").trim()))
    })
}

async fn connect(port: u16, password: &str) -> Option<MultiplexedConnection> {
    let client = Client::open(build_redis_url("127.0.0.1", port, password)).ok()?;
    let mut conn = timeout(CALL_DEADLINE, client.get_multiplexed_async_connection())
        .await
        .ok()?
        .ok()?;
    timeout(
        CALL_DEADLINE,
        redis::cmd("PING").query_async::<String>(&mut conn),
    )
    .await
    .ok()?
    .ok()?;
    Some(conn)
}

/// A session on the local Sentinel with whichever password it still accepts,
/// tried in the order the rotation moves them: the pin, the staged target,
/// the password the rotation started from, the one this boot was given.
async fn connect_with_any(port: u16, candidates: &[&str]) -> Option<(MultiplexedConnection, bool)> {
    let mut seen: Vec<&str> = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.is_empty() || seen.contains(candidate) {
            continue;
        }
        seen.push(candidate);
        if let Some(conn) = connect(port, candidate).await {
            return Some((conn, index == 0));
        }
    }
    None
}

async fn observe(
    conn: &mut MultiplexedConnection,
    accepts_pin: bool,
    data_dir: &str,
    master_name: &str,
) -> Observed {
    let sentinel_pass = timeout(
        CALL_DEADLINE,
        redis::cmd("SENTINEL")
            .arg("CONFIG")
            .arg("GET")
            .arg("sentinel-pass")
            .query_async::<Vec<String>>(conn),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .and_then(|reply| crate::quorum::field_value(&reply, "sentinel-pass"));
    let auth_pass = std::fs::read_to_string(format!("{data_dir}/sentinel.conf"))
        .ok()
        .and_then(|contents| conf_auth_pass(&contents, master_name));
    Observed {
        sentinel_pass,
        auth_pass,
        accepts_pin: Some(accepts_pin),
    }
}

async fn apply(
    conn: &mut MultiplexedConnection,
    master_name: &str,
    repairs: &[Repair],
) -> Result<(), redis::RedisError> {
    for repair in repairs {
        let cmd = match repair {
            Repair::AcceptInbound(pin) => {
                let mut cmd = redis::cmd("ACL");
                cmd.arg("SETUSER").arg("default").arg(format!(">{pin}"));
                cmd
            }
            Repair::SentinelPass(value) => {
                let mut cmd = redis::cmd("SENTINEL");
                cmd.arg("CONFIG").arg("SET").arg("sentinel-pass").arg(value);
                cmd
            }
            Repair::AuthPass(value) => {
                let mut cmd = redis::cmd("SENTINEL");
                cmd.arg("SET").arg(master_name).arg("auth-pass").arg(value);
                cmd
            }
        };
        timeout(CALL_DEADLINE, cmd.query_async::<()>(conn))
            .await
            .map_err(|_| {
                redis::RedisError::from((redis::ErrorKind::IoError, "sentinel call timed out"))
            })??;
    }
    timeout(
        CALL_DEADLINE,
        redis::cmd("SENTINEL")
            .arg("FLUSHCONFIG")
            .query_async::<()>(conn),
    )
    .await
    .map_err(|_| redis::RedisError::from((redis::ErrorKind::IoError, "sentinel call timed out")))?
}

/// One pass: observe, decide, repair. Returns the repairs it applied (names
/// only matter to callers). Public so the rotation's MEMBER step can run the
/// same convergence right after it restarts Sentinel.
pub async fn reconcile_once(
    data_dir: &str,
    sentinel_port: u16,
    master_name: &str,
    boot_password: &str,
) -> Vec<Repair> {
    let Some(pin) = persisted_requirepass(data_dir) else {
        return Vec::new();
    };
    let staged = staged_rotation(data_dir);
    let (staged_current, staged_new) = staged
        .as_ref()
        .map(|(current, new)| (current.as_str(), new.as_str()))
        .unwrap_or(("", ""));
    let Some((mut conn, accepts_pin)) = connect_with_any(
        sentinel_port,
        &[pin.as_str(), staged_new, staged_current, boot_password],
    )
    .await
    else {
        return Vec::new();
    };
    let observed = observe(&mut conn, accepts_pin, data_dir, master_name).await;
    let repairs = plan(&observed, &pin, Some(staged_new));
    if repairs.is_empty() {
        return repairs;
    }
    let names: Vec<_> = repairs.iter().map(Repair::name).collect();
    match apply(&mut conn, master_name, &repairs).await {
        Ok(()) => info!(
            repaired = ?names,
            rotation_in_flight = staged.is_some(),
            "credential-reconcile: Sentinel credentials differed from the pinned password; re-applied"
        ),
        Err(error) => warn!(
            error = %error,
            attempted = ?names,
            "credential-reconcile: re-applying Sentinel credentials failed"
        ),
    }
    repairs
}

/// Run the loop for the life of the process. `sentinel_requires_auth` is the
/// file-resolved posture (`sentinel_conf::conf_requires_auth`); an open
/// Sentinel is left alone, see the module doc.
pub fn spawn(
    data_dir: String,
    sentinel_port: u16,
    master_name: String,
    boot_password: String,
    sentinel_requires_auth: bool,
) {
    let interval = u64::env_parse("CREDENTIAL_RECONCILE_SECONDS", DEFAULT_INTERVAL_SECONDS);
    if interval == 0 {
        info!("credential-reconcile: CREDENTIAL_RECONCILE_SECONDS=0, loop inactive");
        return;
    }
    if !sentinel_requires_auth {
        info!("credential-reconcile: the local Sentinel runs open, nothing to reconcile");
        return;
    }
    info!(interval_secs = interval, "credential-reconcile: starting");
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(interval)).await;
            // A rotation operation owns Sentinel while it runs (PREPARE's
            // staged overlap, MEMBER's restart); converge between them.
            let Ok(_guard) = ROTATION.try_lock() else {
                continue;
            };
            reconcile_once(&data_dir, sentinel_port, &master_name, &boot_password).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed(sentinel_pass: &str, auth_pass: &str, accepts_pin: bool) -> Observed {
        Observed {
            sentinel_pass: Some(sentinel_pass.to_string()),
            auth_pass: Some(auth_pass.to_string()),
            accepts_pin: Some(accepts_pin),
        }
    }

    #[test]
    fn everything_at_the_pin_is_quiet() {
        assert!(plan(&observed("pin", "pin", true), "pin", None).is_empty());
        assert!(plan(&observed("pin", "pin", true), "pin", Some("pin")).is_empty());
    }

    // The reported case: a stale writer put the previous password back into
    // sentinel-pass after the rotation finished (no journal left).
    #[test]
    fn a_stale_sentinel_pass_is_set_back_to_the_pin() {
        assert_eq!(
            plan(&observed("old", "pin", true), "pin", None),
            vec![Repair::SentinelPass("pin".into())]
        );
    }

    #[test]
    fn an_unset_sentinel_pass_is_made_explicit() {
        assert_eq!(
            plan(&observed("", "pin", true), "pin", None),
            vec![Repair::SentinelPass("pin".into())]
        );
    }

    #[test]
    fn a_stale_auth_pass_follows_the_pin() {
        assert_eq!(
            plan(&observed("pin", "old", true), "pin", None),
            vec![Repair::AuthPass("pin".into())]
        );
    }

    #[test]
    fn a_sentinel_that_rejects_the_pin_is_taught_it_first() {
        assert_eq!(
            plan(&observed("old", "old", false), "pin", None),
            vec![
                Repair::AcceptInbound("pin".into()),
                Repair::SentinelPass("pin".into()),
                Repair::AuthPass("pin".into()),
            ]
        );
    }

    // Between PREPARE and MEMBER the pin is still the old password while
    // Sentinel already carries the staged one: that is not drift.
    #[test]
    fn a_rotation_in_flight_accepts_both_passwords() {
        assert!(plan(&observed("new", "new", true), "old", Some("new")).is_empty());
        assert!(plan(&observed("old", "old", true), "old", Some("new")).is_empty());
        assert!(plan(&observed("new", "old", true), "old", Some("new")).is_empty());
    }

    // A restart between PREPARE and MEMBER that lost the runtime SET is
    // repaired in the direction PREPARE committed to, not back to the pin.
    #[test]
    fn a_repair_during_a_rotation_targets_the_staged_password() {
        assert_eq!(
            plan(&observed("stale", "stale", true), "old", Some("new")),
            vec![
                Repair::SentinelPass("new".into()),
                Repair::AuthPass("new".into())
            ]
        );
    }

    #[test]
    fn unreadable_values_are_never_a_difference() {
        let unread = Observed::default();
        assert!(plan(&unread, "pin", None).is_empty());
        let only_inbound = Observed {
            accepts_pin: Some(false),
            ..Observed::default()
        };
        assert_eq!(
            plan(&only_inbound, "pin", None),
            vec![Repair::AcceptInbound("pin".into())]
        );
    }

    #[test]
    fn repairs_debug_without_their_values() {
        let rendered = format!("{:?}", plan(&observed("old", "old", false), "s3cr3t", None));
        assert!(!rendered.contains("s3cr3t"));
        assert!(rendered.contains("sentinel-pass"));
    }

    #[test]
    fn auth_pass_is_read_as_sentinel_or_the_rewrite_wrote_it() {
        let by_sentinel = "sentinel monitor mymaster h 6379 2\nsentinel auth-pass mymaster plain\n";
        assert_eq!(
            conf_auth_pass(by_sentinel, "mymaster").as_deref(),
            Some("plain")
        );
        let by_rewrite = "sentinel auth-pass mymaster \"p:a ss\\\"x\"\n";
        assert_eq!(
            conf_auth_pass(by_rewrite, "mymaster").as_deref(),
            Some("p:a ss\"x")
        );
        assert_eq!(conf_auth_pass(by_sentinel, "other"), None);
        assert_eq!(
            conf_auth_pass("sentinel auth-user mymaster u\n", "mymaster"),
            None
        );
    }
}
