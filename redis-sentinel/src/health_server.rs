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
//! The dual check on /role is the proxy-layer split-brain fence. An isolated
//! master's local Sentinel loses quorum and can no longer confirm mastership,
//! so /role returns 503 and HAProxy stops routing writes to it — even though
//! the isolated Redis still thinks it is the master.
//!
//! This works in concert with min-replicas-to-write in redis.conf: the node
//! self-fences at the Redis layer after min-replicas-max-lag seconds, while
//! the Sentinel check fences it at the HAProxy layer immediately.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use redis::{aio::MultiplexedConnection, Client};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{timeout, Duration};
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    redis_url: String,
    sentinel_url: String,
    /// Our own private hostname, used to verify Sentinel's master-addr answer.
    private_domain: String,
    redis_conn: Arc<Mutex<Option<MultiplexedConnection>>>,
    sentinel_conn: Arc<Mutex<Option<MultiplexedConnection>>>,
}

impl AppState {
    fn new(redis_url: String, sentinel_url: String, private_domain: String) -> Self {
        Self {
            redis_url,
            sentinel_url,
            private_domain,
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
/// Condition (2) is the fence: if this node is network-partitioned from the
/// rest of the cluster its local Sentinel loses quorum and can no longer
/// authoritatively answer — we treat an unreachable Sentinel as 503 (fail-
/// closed), ensuring HAProxy stops routing writes here even though the
/// isolated Redis still believes it is master.
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
            let confirmed = master_host == &state.private_domain;
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
    sentinel_confirms_master(state, "mymaster").await
}

pub async fn run_health_server(
    health_port: u16,
    redis_port: u16,
    sentinel_port: u16,
    redis_password: String,
    private_domain: String,
) {
    // Sentinel has no auth by default; connect without password.
    let redis_url = format!("redis://:{}@127.0.0.1:{}", redis_password, redis_port);
    let sentinel_url = format!("redis://127.0.0.1:{}", sentinel_port);
    let state = AppState::new(redis_url, sentinel_url, private_domain);

    let app = Router::new()
        .route("/health", get(health))
        .route("/role", get(role))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], health_port));
    info!(port = health_port, "health server listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("health server bind failed");

    axum::serve(listener, app)
        .await
        .expect("health server failed");
}
