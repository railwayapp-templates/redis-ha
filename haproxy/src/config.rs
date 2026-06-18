use anyhow::{Context, Result};
use common::ConfigExt;

pub struct Config {
    /// Comma-separated "hostname:port" list of Redis backends.
    /// Example: "redis-1.railway.internal:6379,redis-2.railway.internal:6379"
    pub redis_nodes: String,
    /// Port where redis-wrapper health server listens on each backend node.
    pub health_port: u16,
    pub redis_port: u16,
    pub max_conn: String,
    pub timeout_connect: String,
    pub timeout_client: String,
    pub timeout_server: String,
    pub timeout_check: String,
    pub check_interval: String,
    pub check_fastinter: String,
    pub check_downinter: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let redis_nodes = String::env_required("REDIS_NODES").context(
            "REDIS_NODES is required.\n\
             Format: hostname:port,...\n\
             Example: Redis-1.railway.internal:6379,Redis-2.railway.internal:6379",
        )?;

        Ok(Self {
            redis_nodes,
            health_port: u16::env_parse("HEALTH_CHECK_PORT", 8080),
            redis_port: u16::env_parse("REDIS_PORT", 6379),
            max_conn: String::env_or("HAPROXY_MAX_CONN", "10000"),
            timeout_connect: String::env_or("HAPROXY_TIMEOUT_CONNECT", "10s"),
            timeout_client: String::env_or("HAPROXY_TIMEOUT_CLIENT", "30m"),
            timeout_server: String::env_or("HAPROXY_TIMEOUT_SERVER", "30m"),
            timeout_check: String::env_or("HAPROXY_TIMEOUT_CHECK", "3s"),
            check_interval: String::env_or("HAPROXY_CHECK_INTERVAL", "3s"),
            check_fastinter: String::env_or("HAPROXY_CHECK_FASTINTER", "500ms"),
            check_downinter: String::env_or("HAPROXY_CHECK_DOWNINTER", "500ms"),
        })
    }
}
