use anyhow::{bail, Result};
use common::{ConfigExt, RailwayEnv};
use std::env;

pub struct Config {
    pub redis_password: String,
    pub redis_port: u16,
    /// Host:port of the master to replicate from. Empty string on the primary node.
    pub replica_of: String,
    pub sentinel_enabled: bool,
    pub sentinel_port: u16,
    pub sentinel_quorum: u32,
    /// Comma-separated "host:port" list of all Sentinel peers.
    pub sentinel_hosts: String,
    pub redis_master_name: String,
    pub sentinel_down_after_ms: u64,
    pub sentinel_failover_timeout_ms: u64,
    pub health_port: u16,
    pub data_dir: String,
    /// The hostname of this service's private domain (used to derive master host for sentinels).
    pub private_domain: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let redis_password = String::env_required("REDIS_PASSWORD")?;
        let sentinel_enabled = bool::env_bool("SENTINEL_ENABLED", false);
        let sentinel_hosts = String::env_or("SENTINEL_HOSTS", "");

        if sentinel_enabled && sentinel_hosts.is_empty() {
            bail!("SENTINEL_HOSTS is required when SENTINEL_ENABLED=true");
        }

        Ok(Self {
            redis_password,
            redis_port: u16::env_parse("REDIS_PORT", 6379),
            replica_of: String::env_or("REPLICA_OF", ""),
            sentinel_enabled,
            sentinel_port: u16::env_parse("SENTINEL_PORT", 26379),
            sentinel_quorum: u32::env_parse("SENTINEL_QUORUM", 2),
            sentinel_hosts,
            redis_master_name: String::env_or("REDIS_MASTER_NAME", "mymaster"),
            sentinel_down_after_ms: u64::env_parse("SENTINEL_DOWN_AFTER_MS", 5000),
            sentinel_failover_timeout_ms: u64::env_parse("SENTINEL_FAILOVER_TIMEOUT_MS", 30000),
            health_port: u16::env_parse("HEALTH_PORT", 8080),
            data_dir: Self::resolve_data_dir(),
            private_domain: RailwayEnv::private_domain(),
        })
    }

    /// Where redis keeps its data.
    ///
    /// Follows the volume, rather than requiring the volume to follow us: an
    /// explicit `DATA_DIR` wins, otherwise we use wherever Railway actually
    /// mounted the volume, and `/data` is only the off-platform fallback.
    ///
    /// This matters for HA conversion. Adopting a live standalone service as
    /// the cluster's primary keeps that service's existing volume and mount
    /// path, which is `/data` only by coincidence of the Railway `redis`
    /// template. A root of bitnami lineage (`railwayapp/redis`, Railway's
    /// mirror) is mounted at `/bitnami/redis/data`, and any customer is free
    /// to pick their own path. Hardcoding `/data` there meant redis started
    /// against an empty directory on the container filesystem while the real
    /// RDB/AOF sat unread on the volume.
    ///
    /// Mirrors postgres-patroni's `volume_root()`, which has always derived
    /// its paths from `RAILWAY_VOLUME_MOUNT_PATH` — the reason the equivalent
    /// postgres conversion was never exposed to this.
    fn resolve_data_dir() -> String {
        if let Ok(dir) = env::var("DATA_DIR") {
            if !dir.is_empty() {
                return dir;
            }
        }
        if let Ok(mount) = env::var("RAILWAY_VOLUME_MOUNT_PATH") {
            if !mount.is_empty() {
                return mount;
            }
        }
        "/data".to_string()
    }

    /// True if this node starts as the primary (REPLICA_OF is empty).
    pub fn is_primary(&self) -> bool {
        self.replica_of.is_empty()
    }

    /// The initial master host for Sentinel configuration.
    ///
    /// For the primary node: its own private domain.
    /// For replicas: the host parsed from REPLICA_OF.
    pub fn initial_master_host(&self) -> String {
        if self.is_primary() {
            self.private_domain.clone()
        } else {
            // REPLICA_OF is "host:port" — take the host part
            self.replica_of
                .split(':')
                .next()
                .unwrap_or(&self.private_domain)
                .to_string()
        }
    }

    pub fn initial_master_port(&self) -> u16 {
        if self.is_primary() {
            self.redis_port
        } else {
            self.replica_of
                .split(':')
                .nth(1)
                .and_then(|p| p.parse().ok())
                .unwrap_or(self.redis_port)
        }
    }
}
