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

/// Builds a `redis://` URL for any endpoint this crate connects to — a
/// Sentinel port or the local Redis data port alike — the one place that
/// decides whether the connection carries AUTH and the one place the
/// password is escaped into the URL's userinfo component.
///
/// `password` follows the crate-wide "empty = off" convention: an empty
/// password produces the exact unauthenticated URL every caller built by
/// hand before this helper existed, and a non-empty one sends AUTH. Every
/// internal client of a redis:// endpoint — the boot-time peer query, the
/// quorum-sync watcher, link-heal, the health server, demote-on-shutdown,
/// process-manager, boot-role's dataset probe — goes through this so there
/// is one formula to update instead of eight, and one place that can
/// truthfully claim "the password never appears in a log line" by
/// construction (the `redis` crate never logs the URLs it's given).
///
/// Routes through `url::Url::set_password` rather than interpolating the raw
/// password into the string: a password containing `@`, `:`, `/`, `#`, `%`,
/// or whitespace — entirely possible, since a converted cluster inherits
/// whatever password the customer's standalone Redis already had — breaks a
/// hand-built URL's parsing (wrong host, truncated password, or an outright
/// unparseable string), not just cosmetically. `url` percent-encodes exactly
/// the userinfo-illegal byte set, and the `redis` crate parses URLs through
/// the same crate, so what this builds is guaranteed to parse back to the
/// original password, not merely "usually work."
pub fn build_redis_url(host: &str, port: u16, password: &str) -> String {
    // A bare IPv6 literal in a URL authority is a parse error waiting to
    // happen — the colons read as the port separator. Sentinel's
    // `get-master-addr-by-name` and peer gossip hand back whatever address
    // was announced, brackets included or not, so bracket it here once for
    // every caller. Hostnames and IPv4 never contain ':' and pass through.
    let authority = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    if password.is_empty() {
        return format!("redis://{authority}:{port}");
    }
    let mut url = match url::Url::parse(&format!("redis://{authority}:{port}")) {
        Ok(url) => url,
        Err(err) => {
            // The host came from the network (a Sentinel answer, a peer's
            // gossip) and is not URL-authority material — empty, whitespace,
            // illegal characters. Hand the raw URL back so the redis client
            // surfaces the same parse failure as a connection error at the
            // call site; panicking here would take the whole wrapper down.
            tracing::warn!(
                host = %host,
                error = %err,
                "host is not a parseable URL authority; passing it through for the client to reject"
            );
            return format!("redis://{authority}:{port}");
        }
    };
    url.set_password(Some(password))
        .expect("a URL with a host always accepts a password");
    url.to_string()
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
    let url = build_redis_url(host, port, "");
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
    let open_url = build_redis_url(host, port, "");
    let mut conn = connect(&open_url, deadline).await?;
    match request_master_addr(&mut conn, master_name, deadline).await {
        Ok(answer) => return answer,
        Err(err) if is_noauth(&err) && !password.is_empty() => {}
        Err(_) => return None,
    }
    let authed_url = build_redis_url(host, port, password);
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
mod build_redis_url_tests {
    use super::*;
    use redis::IntoConnectionInfo;

    #[test]
    fn empty_password_is_todays_unauthenticated_url() {
        assert_eq!(
            build_redis_url("redis-1.railway.internal", 26379, ""),
            "redis://redis-1.railway.internal:26379"
        );
    }

    #[test]
    fn a_plain_password_is_embedded_as_the_url_auth_component() {
        assert_eq!(
            build_redis_url("redis-1.railway.internal", 26379, "s3cr3t"),
            "redis://:s3cr3t@redis-1.railway.internal:26379"
        );
    }

    #[test]
    fn loopback_host_works_the_same_way() {
        assert_eq!(
            build_redis_url("127.0.0.1", 26379, ""),
            "redis://127.0.0.1:26379"
        );
        assert_eq!(
            build_redis_url("127.0.0.1", 26379, "pw"),
            "redis://:pw@127.0.0.1:26379"
        );
    }

    #[test]
    fn the_password_never_appears_unescaped_elsewhere_in_the_url() {
        // Guards against a future edit accidentally duplicating the
        // password into the host/port portion of the URL.
        let url = build_redis_url("redis-2.railway.internal", 26379, "hunter2");
        assert_eq!(url.matches("hunter2").count(), 1);
    }

    // --- hosts that used to panic (audit R-1) ---
    //
    // `Url::parse(...).expect()` turned a bare IPv6 literal — exactly what
    // `SENTINEL get-master-addr-by-name` returns when a peer announces one —
    // into a wrapper-killing panic (`InvalidPort`), and any other
    // non-authority host (empty, whitespace) into one too. These pin the
    // two replacement behaviors: bracket-and-work for IPv6, pass-through
    // for garbage.

    #[test]
    fn a_bare_ipv6_literal_is_bracketed_and_round_trips() {
        assert_eq!(
            build_redis_url("fd12::10", 26379, ""),
            "redis://[fd12::10]:26379"
        );
        let url = build_redis_url("fd12::10", 26379, "pw");
        assert_eq!(url, "redis://:pw@[fd12::10]:26379");
        let info = url
            .as_str()
            .into_connection_info()
            .unwrap_or_else(|e| panic!("built URL {url:?} failed to parse: {e}"));
        match info.addr {
            redis::ConnectionAddr::Tcp(host, _) => assert_eq!(host, "fd12::10"),
            other => panic!("unexpected addr for a redis:// URL: {other:?}"),
        }
    }

    #[test]
    fn an_already_bracketed_ipv6_literal_is_not_double_bracketed() {
        assert_eq!(
            build_redis_url("[fd12::10]", 26379, ""),
            "redis://[fd12::10]:26379"
        );
    }

    #[test]
    fn a_non_authority_host_never_panics_and_stays_parseable_by_the_client() {
        // Garbage from the network must come back as a URL string, not a
        // panic; the redis crate then rejects it at connection-info parse.
        for host in ["", "host with space"] {
            let url = build_redis_url(host, 26379, "pw");
            assert!(
                url.starts_with("redis://"),
                "unexpected URL {url:?} for host {host:?}"
            );
            assert!(url.as_str().into_connection_info().is_err());
        }
    }

    // --- passwords with characters that break a hand-interpolated URL ---
    //
    // Every case here round-trips through the `redis` crate's own parser
    // (the actual consumer, not just a string-shape assertion) to prove the
    // built URL is not just well-formed but decodes back to the exact
    // original password.

    fn round_tripped_password(host: &str, port: u16, password: &str) -> String {
        let url = build_redis_url(host, port, password);
        let info = url
            .as_str()
            .into_connection_info()
            .unwrap_or_else(|e| panic!("built URL {url:?} failed to parse: {e}"));
        info.redis.password.unwrap_or_default()
    }

    #[test]
    fn a_password_containing_an_at_sign_round_trips() {
        // Unescaped, "@" is the userinfo/host delimiter — the naive
        // `format!("redis://:{password}@{host}")` would truncate the
        // password at the FIRST "@" and misparse the remainder as part of
        // the host.
        let pw = "p@ss@word";
        assert_eq!(
            round_tripped_password("redis-1.railway.internal", 6379, pw),
            pw
        );
    }

    #[test]
    fn a_password_containing_a_colon_round_trips() {
        let pw = "pass:word";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }

    #[test]
    fn a_password_containing_a_slash_round_trips() {
        let pw = "pass/word";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }

    #[test]
    fn a_password_containing_a_percent_sign_round_trips() {
        // The escape character itself — a naive percent-encoder that forgot
        // to encode literal "%" would produce a URL whose decoder
        // misinterprets whatever follows it as an escape sequence.
        let pw = "50%off";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }

    #[test]
    fn a_password_containing_whitespace_round_trips() {
        let pw = "pass word";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }

    #[test]
    fn a_password_containing_a_hash_round_trips() {
        // Unescaped, "#" starts the URL fragment — everything after it would
        // silently vanish from the parsed password.
        let pw = "pass#word";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }

    #[test]
    fn a_password_that_is_only_special_characters_round_trips() {
        let pw = "@:/?#%";
        assert_eq!(round_tripped_password("127.0.0.1", 6379, pw), pw);
    }
}
