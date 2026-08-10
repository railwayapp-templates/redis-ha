//! HTTP health server embedded in each Redis node.
//!
//! Exposes two endpoints that HAProxy uses for intelligent routing:
//!
//!   GET /health  → 200 if Redis is up and responding to PING, 503 otherwise.
//!   GET /role    → 200 {"role":"master"} only if BOTH conditions hold:
//!                    1. local Redis reports role:master
//!                    2. local Sentinel confirms this node is the current master
//!                  503 in all other cases, including when Sentinel is unreachable.
//!
//! ## What the /role dual check actually fences
//! `SENTINEL get-master-addr-by-name` answers from the local Sentinel's own
//! in-memory state, not a quorum vote — losing contact with the majority
//! does not, by itself, make a Sentinel stop naming its current master. An
//! isolated master's local Sentinel keeps answering with that master's own
//! address for as long as it has not yet observed a newer epoch, so /role
//! does not flip to 503 the instant a partition opens. It flips once this
//! node's own Sentinel learns of the switch — typically after the partition
//! heals and it resyncs with the majority, or once it observes the new
//! master being announced through gossip.
//!
//! The write-safety backstop for the partition window itself is
//! `min-replicas-to-write` in redis.conf: an isolated master loses its
//! replica acks and Redis rejects writes on its own once
//! min-replicas-max-lag seconds pass, independent of Sentinel or HAProxy.
//! /role's job is narrower than fencing the partition immediately — it keeps
//! HAProxy from continuing to route to a node once its own Sentinel has
//! registered the demotion, closing the window where Redis would otherwise
//! still answer role:master to a probe with nothing else checking it.
//!
//! ## Supervision
//! `spawn` wraps the server in the same respawn shape as `link_heal`/
//! `quorum`: an outer loop wraps each attempt in `tokio::task::spawn`, so a
//! bind failure, a serve error, or a panic surfaces as a warn log (and,
//! deduped, a `ComponentError` telemetry event) instead of silently leaving
//! /health and /role unreachable forever — which would otherwise pull this
//! node from BOTH HAProxy backends permanently, with nothing else watching
//! this task (`supervise` only watches the redis and sentinel processes).

use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use common::{Telemetry, TelemetryEvent};
use redis::{aio::MultiplexedConnection, Client};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    redis_url: String,
    sentinel_url: String,
    /// Our own private hostname, used to verify Sentinel's master-addr answer.
    private_domain: String,
    /// The Sentinel master name this cluster runs under (`REDIS_MASTER_NAME`,
    /// default `mymaster`). A cluster with a custom name never matches a
    /// hardcoded "mymaster", so this must come from config, not a literal.
    redis_master_name: String,
    redis_conn: Arc<Mutex<Option<MultiplexedConnection>>>,
    sentinel_conn: Arc<Mutex<Option<MultiplexedConnection>>>,
}

impl AppState {
    fn new(
        redis_url: String,
        sentinel_url: String,
        private_domain: String,
        redis_master_name: String,
    ) -> Self {
        Self {
            redis_url,
            sentinel_url,
            private_domain,
            redis_master_name,
            redis_conn: Arc::new(Mutex::new(None)),
            sentinel_conn: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_redis_conn(&self) -> Option<MultiplexedConnection> {
        get_or_connect(&self.redis_conn, &self.redis_url, "Redis").await
    }

    async fn get_sentinel_conn(&self) -> Option<MultiplexedConnection> {
        get_or_connect(&self.sentinel_conn, &self.sentinel_url, "Sentinel").await
    }
}

async fn get_or_connect(
    slot: &Arc<Mutex<Option<MultiplexedConnection>>>,
    url: &str,
    label: &str,
) -> Option<MultiplexedConnection> {
    let mut guard = slot.lock().await;
    if guard.is_none() {
        match Client::open(url) {
            Ok(client) => match client.get_multiplexed_async_connection().await {
                Ok(conn) => *guard = Some(conn),
                Err(e) => warn!(error = %e, label, "connection failed"),
            },
            Err(e) => warn!(error = %e, label, "invalid URL"),
        }
    }
    guard.clone()
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    match timeout(Duration::from_secs(2), ping_redis(&state)).await {
        Ok(true) => (StatusCode::OK, Json(json!({"status": "ok"}))),
        _ => {
            *state.redis_conn.lock().await = None;
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"status": "down"})))
        }
    }
}

/// Split-brain-safe master check.
///
/// Returns 200 only when both conditions hold:
///   1. Local Redis reports role:master.
///   2. Local Sentinel's `SENTINEL get-master-addr-by-name` confirms this
///      node's hostname as the current master.
///
/// Condition (2) is not a quorum check: `get-master-addr-by-name` answers
/// from the local Sentinel's own state, and that state keeps naming this
/// node for as long as this Sentinel has not itself observed a newer epoch —
/// an isolated master's Sentinel does not necessarily flip that answer the
/// moment a partition opens. We still treat an unreachable Sentinel as 503
/// (fail-closed), which covers the case where this node cannot reach its own
/// colocated Sentinel at all, but that is narrower than fencing every
/// partition immediately. The write-safety backstop for the partition
/// window is `min-replicas-to-write` at the Redis layer (see the module
/// doc); this check's job is to stop HAProxy from routing here once this
/// node's own Sentinel has registered the demotion, not to detect the
/// partition itself.
async fn role(State(state): State<AppState>) -> impl IntoResponse {
    match timeout(Duration::from_secs(2), is_sentinel_confirmed_master(&state)).await {
        Ok(true) => (StatusCode::OK, Json(json!({"role": "master"}))),
        Ok(false) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"role": "replica"}))),
        Err(_) => {
            // Timeout — treat as unhealthy
            *state.redis_conn.lock().await = None;
            *state.sentinel_conn.lock().await = None;
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"role": "unknown", "reason": "timeout"})))
        }
    }
}

async fn ping_redis(state: &AppState) -> bool {
    match state.get_redis_conn().await {
        Some(mut conn) => {
            let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
            matches!(result, Ok(s) if s == "PONG")
        }
        None => false,
    }
}

/// Check (1): local Redis says role:master.
async fn local_role_is_master(state: &AppState) -> bool {
    let Some(mut conn) = state.get_redis_conn().await else { return false };
    let Ok(info): redis::RedisResult<String> = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut conn)
        .await
    else {
        *state.redis_conn.lock().await = None;
        return false;
    };
    info.lines().any(|l| l.trim() == "role:master")
}

/// Whether Sentinel's `get-master-addr-by-name` answer names this node,
/// comparing hosts the same tolerant way every other node-identity check in
/// this crate does (`boot_role::normalize_host`): case-insensitive and
/// tolerant of a trailing root dot. Pure and zero-I/O so it can be
/// unit-tested directly — a byte-exact comparison here would 503 this
/// node's /role forever the moment DNS or Sentinel's own announce-hostnames
/// gossip varies the case or trailing-dot shape of the hostname it reports,
/// which nothing about this cluster's health actually depends on.
fn answer_confirms_self(answer_host: &str, private_domain: &str) -> bool {
    crate::boot_role::normalize_host(answer_host) == crate::boot_role::normalize_host(private_domain)
}

/// Check (2): Sentinel confirms this node is the current master.
///
/// Fails closed: if Sentinel is unreachable, returns false.
async fn sentinel_confirms_master(state: &AppState, master_name: &str) -> bool {
    let Some(mut conn) = state.get_sentinel_conn().await else {
        warn!("sentinel unreachable — failing closed for /role");
        return false;
    };

    // Returns a two-element bulk array: [host, port]
    let result: redis::RedisResult<Vec<String>> = redis::cmd("SENTINEL")
        .arg("get-master-addr-by-name")
        .arg(master_name)
        .query_async(&mut conn)
        .await;

    match result {
        Ok(parts) if parts.len() == 2 => {
            let master_host = &parts[0];
            let confirmed = answer_confirms_self(master_host, &state.private_domain);
            if !confirmed {
                info!(
                    sentinel_master = %master_host,
                    this_node = %state.private_domain,
                    "sentinel says master is elsewhere — returning 503"
                );
            }
            confirmed
        }
        Ok(_) => {
            warn!("unexpected sentinel response shape");
            *state.sentinel_conn.lock().await = None;
            false
        }
        Err(e) => {
            warn!(error = %e, "sentinel get-master-addr-by-name failed");
            *state.sentinel_conn.lock().await = None;
            false
        }
    }
}

async fn is_sentinel_confirmed_master(state: &AppState) -> bool {
    // Fast path: skip the Sentinel round-trip if local Redis already says replica.
    if !local_role_is_master(state).await {
        return false;
    }
    // Sentinel confirmation is the actual fence.
    sentinel_confirms_master(state, &state.redis_master_name).await
}

/// Bind and serve once. Returns `Err` on a bind failure or a serve error
/// instead of `expect()`-ing — the caller (`spawn`) retries on `Err` rather
/// than letting either kill the task permanently.
///
/// `local_sentinel_password` is `""` unless the co-located Sentinel's
/// on-disk conf currently carries `requirepass` — resolved by the caller
/// from the file (see `sentinel_conf::conf_requires_auth`), never assumed
/// from the default, since a preserved conf that predates Sentinel auth
/// has no `requirepass` regardless of what new clusters now get, and
/// authenticating against a Sentinel that requires none is a hard
/// connection failure, not a no-op.
async fn run_health_server(
    health_port: u16,
    redis_port: u16,
    sentinel_port: u16,
    redis_password: String,
    private_domain: String,
    redis_master_name: String,
    local_sentinel_password: String,
) -> anyhow::Result<()> {
    let redis_url =
        crate::sentinel_query::build_redis_url("127.0.0.1", redis_port, &redis_password);
    let sentinel_url = crate::sentinel_query::build_redis_url(
        "127.0.0.1",
        sentinel_port,
        &local_sentinel_password,
    );
    let state = AppState::new(redis_url, sentinel_url, private_domain, redis_master_name);

    let app = Router::new()
        .route("/health", get(health))
        .route("/role", get(role))
        .with_state(state);

    // Bind the IPv6 unspecified address rather than 0.0.0.0: Railway's private
    // network is IPv6 (fd12::... hostnames), and a IPv4-only listener refuses
    // every connection HAProxy's health check makes over it. Linux dual-stack
    // sockets accept IPv4-mapped connections on the same listener by default.
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], health_port));
    info!(port = health_port, "health server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("health server bind failed")?;

    axum::serve(listener, app)
        .await
        .context("health server failed")?;

    Ok(())
}

// A task that stayed up at least this long before failing was healthy in
// between — the next failure is a new incident, not a continuation of the
// same crash loop, and earns its own telemetry event.
const HEALTHY_RUN_THRESHOLD: Duration = Duration::from_secs(60);
// Same respawn delay as link_heal/quorum.
const RESPAWN_DELAY: Duration = Duration::from_secs(5);

/// Spawn the health server as a supervised background task.
///
/// Mirrors `link_heal::spawn`/`quorum::spawn`: an outer loop wraps each
/// attempt in `tokio::task::spawn` so a panic surfaces as a warn log instead
/// of aborting redis-wrapper, and a bind or serve failure retries after
/// `RESPAWN_DELAY` instead of leaving /health and /role gone for good. HAProxy
/// treats a missing health server exactly like a down node — the target is
/// pulled from BOTH backends — so an unsupervised task dying here is a
/// silent, permanent outage for whatever this container happens to be at the
/// time, worse than the redis-server/sentinel deaths `supervise` already
/// watches for.
///
/// Failures are deduped via `HEALTHY_RUN_THRESHOLD` so a crash loop emits one
/// `ComponentError` per incident instead of one every `RESPAWN_DELAY`.
pub fn spawn(
    health_port: u16,
    redis_port: u16,
    sentinel_port: u16,
    redis_password: String,
    private_domain: String,
    redis_master_name: String,
    local_sentinel_password: String,
    telemetry: Telemetry,
) {
    tokio::spawn(async move {
        // Whether the last failure already produced a ComponentError — reset
        // once a subsequent attempt runs healthy for HEALTHY_RUN_THRESHOLD,
        // so a later, unrelated failure gets reported too.
        let mut alerted_for_current_incident = false;

        loop {
            let hp = health_port;
            let rp = redis_port;
            let sp = sentinel_port;
            let pw = redis_password.clone();
            let domain = private_domain.clone();
            let mn = redis_master_name.clone();
            let spw = local_sentinel_password.clone();

            let started_at = Instant::now();
            let handle = tokio::task::spawn(async move {
                run_health_server(hp, rp, sp, pw, domain, mn, spw).await
            });
            let outcome = handle.await;
            let ran_for = started_at.elapsed();

            let failure = match outcome {
                Ok(Ok(())) => {
                    // axum::serve only returns on a graceful-shutdown signal
                    // we never send, so this is unexpected but not fatal.
                    warn!("health-server: run loop returned cleanly — respawning in 5s");
                    Some("run loop returned cleanly".to_string())
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "health-server: bind/serve failed — respawning in 5s");
                    Some(format!("bind/serve failed: {e:#}"))
                }
                Err(e) if e.is_panic() => {
                    warn!(panic = ?e, "health-server: task panicked — respawning in 5s");
                    Some("task panicked".to_string())
                }
                Err(e) => {
                    warn!(error = %e, "health-server: join error — respawning in 5s");
                    Some(format!("join error: {e}"))
                }
            };

            if let Some(error) = failure {
                if ran_for >= HEALTHY_RUN_THRESHOLD {
                    alerted_for_current_incident = false;
                }
                if !alerted_for_current_incident {
                    telemetry.send(TelemetryEvent::ComponentError {
                        component: "redis-wrapper".to_string(),
                        error,
                        context: "health-server".to_string(),
                    });
                    alerted_for_current_incident = true;
                }
            }

            sleep(RESPAWN_DELAY).await;
        }
    });
}

#[cfg(test)]
mod answer_confirms_self_tests {
    use super::*;

    #[test]
    fn identical_hosts_confirm() {
        assert!(answer_confirms_self(
            "redis-1.railway.internal",
            "redis-1.railway.internal"
        ));
    }

    #[test]
    fn case_difference_still_confirms() {
        assert!(answer_confirms_self(
            "Redis-1.Railway.Internal",
            "redis-1.railway.internal"
        ));
    }

    #[test]
    fn trailing_root_dot_still_confirms() {
        assert!(answer_confirms_self(
            "redis-1.railway.internal.",
            "redis-1.railway.internal"
        ));
    }

    #[test]
    fn different_host_does_not_confirm() {
        assert!(!answer_confirms_self(
            "redis-2.railway.internal",
            "redis-1.railway.internal"
        ));
    }
}
