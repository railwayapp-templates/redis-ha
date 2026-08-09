//! Timeout-bounded async helpers for querying a Sentinel — local or peer.
//!
//! `link_heal` talks only to the colocated Sentinel over loopback, where a
//! dead endpoint fails fast; these helpers exist for the callers that cannot
//! afford to block on a peer that may not exist yet (boot-time role
//! resolution) or that poll on a schedule (quorum sync). Every call is capped
//! by an explicit deadline so a hung TCP handshake to a half-up peer delays
//! boot by the timeout, never indefinitely.

use redis::aio::MultiplexedConnection;
use redis::Client;
use std::time::Duration;
use tokio::time::timeout;

/// Build a `redis://` URL for a Sentinel endpoint, the one place that decides
/// whether a Sentinel connection carries AUTH.
///
/// `password` follows the crate-wide "empty = off" convention: an empty
/// password produces the exact unauthenticated URL every caller built by
/// hand before this helper existed, and a non-empty one — the cluster's
/// shared `REDIS_PASSWORD`, when the local sentinel.conf carries
/// `requirepass` (see `sentinel_auth`) — sends AUTH. Every internal client
/// of a Sentinel endpoint — the boot-time peer query, the quorum-sync
/// watcher, link-heal, the health server, demote-on-shutdown — goes through
/// this so there is one formula to update instead of six, and one place
/// that can truthfully claim "the password never appears in a log line" by
/// construction (the `redis` crate never logs the URLs it's given).
pub fn sentinel_url(host: &str, port: u16, password: &str) -> String {
    if password.is_empty() {
        format!("redis://{host}:{port}")
    } else {
        format!("redis://:{password}@{host}:{port}")
    }
}

/// How a Sentinel endpoint answered a credential-less probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnauthedProbe {
    /// The endpoint accepted a command without credentials — it currently
    /// enforces no client auth.
    Open,
    /// The endpoint answered, and refused the credential-less command with
    /// the NOAUTH-class error a `requirepass`-protected instance gives.
    RequiresAuth,
    /// No usable answer: refused connection, timeout, bad address. Says
    /// nothing about the endpoint's auth posture.
    NoAnswer,
}

/// Whether `err` is the refusal a `requirepass`-protected instance gives a
/// credential-less command. Verified against redis 8.2.1 with redis-rs
/// 0.27.6: the unauthenticated connection handshake itself succeeds (the
/// only setup commands are `CLIENT SETINFO`, whose results redis-rs
/// ignores), and the first real command comes back
/// `-NOAUTH Authentication required.`, which redis-rs surfaces as an
/// extension error with code `NOAUTH` — there is no dedicated `ErrorKind`
/// for it.
fn is_noauth(err: &redis::RedisError) -> bool {
    err.code() == Some("NOAUTH")
}

/// Probe a Sentinel endpoint's auth posture: connect with no credentials
/// and PING, bounding each step by `deadline`.
///
/// This is the discriminator `sentinel_auth` matches a first boot against:
/// a PONG proves the endpoint serves credential-less clients (an
/// unauthenticated cluster), a NOAUTH-class refusal proves it requires auth,
/// and anything else — refused connection, timeout — proves only that this
/// endpoint had no answer to give.
pub async fn probe_unauthenticated(host: &str, port: u16, deadline: Duration) -> UnauthedProbe {
    let url = sentinel_url(host, port, "");
    let Some(mut conn) = connect(&url, deadline).await else {
        return UnauthedProbe::NoAnswer;
    };
    match timeout(deadline, redis::cmd("PING").query_async::<String>(&mut conn)).await {
        Ok(Ok(reply)) if reply.eq_ignore_ascii_case("pong") => UnauthedProbe::Open,
        Ok(Err(err)) if is_noauth(&err) => UnauthedProbe::RequiresAuth,
        _ => UnauthedProbe::NoAnswer,
    }
}

/// Connect to a Redis/Sentinel endpoint, bounding the whole attempt (TCP +
/// protocol handshake) by `deadline`. `None` on refusal, timeout, or a bad
/// URL — callers treat all three as "this endpoint has no answer".
pub async fn connect(url: &str, deadline: Duration) -> Option<MultiplexedConnection> {
    let client = Client::open(url).ok()?;
    match timeout(deadline, client.get_multiplexed_async_connection()).await {
        Ok(Ok(conn)) => Some(conn),
        _ => None,
    }
}

/// Authenticated PING against a Redis data endpoint, bounded by `deadline`.
/// True only when the whole handshake — TCP, AUTH from the URL's password,
/// and the PING itself — succeeds. Callers use this as proof that the
/// address hosts a live instance that accepts this cluster's credentials:
/// a deleted host (no DNS), a wedged one (timeout), and a foreign service
/// reusing the hostname (AUTH refused) all come back `false`.
pub async fn authenticated_ping(url: &str, deadline: Duration) -> bool {
    let Some(mut conn) = connect(url, deadline).await else {
        return false;
    };
    matches!(
        timeout(deadline, redis::cmd("PING").query_async::<String>(&mut conn)).await,
        Ok(Ok(reply)) if reply.eq_ignore_ascii_case("pong")
    )
}

/// `SENTINEL get-master-addr-by-name <master_name>`, bounded by `deadline`,
/// with the server's error surfaced so a caller can tell an auth refusal
/// apart from "no answer". A timeout or a malformed/nil reply is `Ok(None)`
/// — states that retrying with credentials cannot fix.
async fn request_master_addr(
    conn: &mut MultiplexedConnection,
    master_name: &str,
    deadline: Duration,
) -> Result<Option<(String, u16)>, redis::RedisError> {
    let reply = timeout(
        deadline,
        redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(master_name)
            .query_async::<Vec<String>>(conn),
    )
    .await;
    let parts = match reply {
        Ok(Ok(parts)) => parts,
        Ok(Err(err)) => return Err(err),
        Err(_) => return Ok(None),
    };
    if parts.len() != 2 {
        return Ok(None);
    }
    let Ok(port) = parts[1].parse::<u16>() else {
        return Ok(None);
    };
    if parts[0].is_empty() {
        return Ok(None);
    }
    Ok(Some((parts[0].clone(), port)))
}

/// `SENTINEL get-master-addr-by-name <master_name>`, bounded by `deadline`.
/// `None` when the Sentinel is unreachable, answers nil (it does not know the
/// master set), or the reply has an unexpected shape.
pub async fn get_master_addr(
    conn: &mut MultiplexedConnection,
    master_name: &str,
    deadline: Duration,
) -> Option<(String, u16)> {
    request_master_addr(conn, master_name, deadline)
        .await
        .ok()
        .flatten()
}

/// Boot-time `get-master-addr-by-name` against a peer whose auth posture is
/// unknown: try without credentials first — today's behavior and the
/// open-cluster common case — and when that attempt is refused with a
/// NOAUTH-class error, retry once WITH `password` (the cluster's shared
/// `REDIS_PASSWORD`, which is also the Sentinel password whenever Sentinel
/// auth is on — see `sentinel_auth`). Without the retry, an authed
/// cluster's boot-role resolution would silently lose every peer answer.
///
/// A network-level failure (refused connection, timeout) is never retried
/// with credentials: auth cannot fix an endpoint that did not answer. And
/// the credentialed retry against an endpoint that turns out to require
/// none would be a hard connection failure anyway ("Client sent AUTH, but
/// no password is set") — which cannot happen here, since only a NOAUTH
/// refusal reaches the retry.
pub async fn get_master_addr_with_auth_fallback(
    host: &str,
    port: u16,
    master_name: &str,
    password: &str,
    deadline: Duration,
) -> Option<(String, u16)> {
    let open_url = sentinel_url(host, port, "");
    let mut conn = connect(&open_url, deadline).await?;
    match request_master_addr(&mut conn, master_name, deadline).await {
        Ok(answer) => return answer,
        Err(err) if is_noauth(&err) && !password.is_empty() => {}
        Err(_) => return None,
    }
    let authed_url = sentinel_url(host, port, password);
    let mut conn = connect(&authed_url, deadline).await?;
    request_master_addr(&mut conn, master_name, deadline)
        .await
        .ok()
        .flatten()
}

/// `SENTINEL MASTER <master_name>`, bounded by `deadline` — the flat
/// field-value reply carrying `ip`, `port`, `flags` (including
/// `failover_in_progress` while a failover this Sentinel knows about is
/// still running), `quorum`, and more. `None` on any failure to answer;
/// callers must treat that as ambiguous, never as "failover finished" or
/// "still running" — see `quorum::field_value` for reading fields out of it.
pub async fn get_master_fields(
    conn: &mut MultiplexedConnection,
    master_name: &str,
    deadline: Duration,
) -> Option<Vec<String>> {
    let reply = timeout(
        deadline,
        redis::cmd("SENTINEL")
            .arg("master")
            .arg(master_name)
            .query_async::<Vec<String>>(conn),
    )
    .await;
    match reply {
        Ok(Ok(fields)) => Some(fields),
        _ => None,
    }
}

#[cfg(test)]
mod sentinel_url_tests {
    use super::*;

    #[test]
    fn empty_password_is_todays_unauthenticated_url() {
        assert_eq!(
            sentinel_url("redis-1.railway.internal", 26379, ""),
            "redis://redis-1.railway.internal:26379"
        );
    }

    #[test]
    fn a_password_is_embedded_as_the_url_auth_component() {
        assert_eq!(
            sentinel_url("redis-1.railway.internal", 26379, "s3cr3t"),
            "redis://:s3cr3t@redis-1.railway.internal:26379"
        );
    }

    #[test]
    fn loopback_host_works_the_same_way() {
        assert_eq!(sentinel_url("127.0.0.1", 26379, ""), "redis://127.0.0.1:26379");
        assert_eq!(
            sentinel_url("127.0.0.1", 26379, "pw"),
            "redis://:pw@127.0.0.1:26379"
        );
    }

    #[test]
    fn the_password_never_appears_unescaped_elsewhere_in_the_url() {
        // Guards against a future edit accidentally duplicating the
        // password into the host/port portion of the URL.
        let url = sentinel_url("redis-2.railway.internal", 26379, "hunter2");
        assert_eq!(url.matches("hunter2").count(), 1);
    }
}
