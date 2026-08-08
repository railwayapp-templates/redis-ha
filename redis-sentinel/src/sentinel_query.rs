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
/// `password` follows the crate-wide "empty = off" convention (mirrors
/// `Config::sentinel_password`): Sentinel has no auth by default, and
/// nothing on the platform stamps `SENTINEL_PASSWORD` yet, so an empty
/// password must produce the exact unauthenticated URL every caller built by
/// hand before this helper existed. Every internal client of a Sentinel
/// endpoint — the boot-time peer query, the quorum-sync watcher, link-heal,
/// the health server — goes through this so there is one formula to update
/// instead of five, and one place that can truthfully claim "the password
/// never appears in a log line" by construction (the `redis` crate never
/// logs the URLs it's given).
pub fn sentinel_url(host: &str, port: u16, password: &str) -> String {
    if password.is_empty() {
        format!("redis://{host}:{port}")
    } else {
        format!("redis://:{password}@{host}:{port}")
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

/// `SENTINEL get-master-addr-by-name <master_name>`, bounded by `deadline`.
/// `None` when the Sentinel is unreachable, answers nil (it does not know the
/// master set), or the reply has an unexpected shape.
pub async fn get_master_addr(
    conn: &mut MultiplexedConnection,
    master_name: &str,
    deadline: Duration,
) -> Option<(String, u16)> {
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
        _ => return None,
    };
    if parts.len() != 2 {
        return None;
    }
    let port = parts[1].parse::<u16>().ok()?;
    if parts[0].is_empty() {
        return None;
    }
    Some((parts[0].clone(), port))
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
