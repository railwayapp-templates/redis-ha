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
