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

#[cfg(test)]
impl Config {
    /// A fully-populated Config for tests to mutate. Built directly instead of
    /// through from_env so tests that don't care about the environment don't
    /// have to serialize on it.
    pub fn for_tests() -> Self {
        Self {
            redis_password: "pw".to_string(),
            redis_port: 6379,
            replica_of: String::new(),
            sentinel_enabled: true,
            sentinel_port: 26379,
            sentinel_quorum: 2,
            sentinel_hosts: "redis-1:26379".to_string(),
            redis_master_name: "mymaster".to_string(),
            sentinel_down_after_ms: 5000,
            sentinel_failover_timeout_ms: 30000,
            health_port: 8080,
            data_dir: "/data".to_string(),
            private_domain: "redis-1.railway.internal".to_string(),
        }
    }
}

/// Whether the resolved data dir actually lives on the mounted volume —
/// equal to the mount, or nested under it. A plain string-prefix test is not
/// enough: `/database` starts with `/data` but is a different directory.
pub fn data_dir_is_on_volume(data_dir: &str, mount: &str) -> bool {
    !mount.is_empty() && (data_dir == mount || data_dir.starts_with(&format!("{}/", mount)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env vars are process-global and cargo runs tests in parallel threads —
    /// every test that reads or writes the environment holds this.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for key in [
            "DATA_DIR",
            "RAILWAY_VOLUME_MOUNT_PATH",
            "REDIS_PASSWORD",
            "SENTINEL_ENABLED",
            "SENTINEL_HOSTS",
            "REPLICA_OF",
            "REDIS_PORT",
        ] {
            env::remove_var(key);
        }
    }

    // --- resolve_data_dir: every branch ---

    #[test]
    fn explicit_data_dir_wins_over_the_mount() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("DATA_DIR", "/custom");
        env::set_var("RAILWAY_VOLUME_MOUNT_PATH", "/vol");
        assert_eq!(Config::resolve_data_dir(), "/custom");
    }

    #[test]
    fn empty_data_dir_is_ignored() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("DATA_DIR", "");
        env::set_var("RAILWAY_VOLUME_MOUNT_PATH", "/vol");
        assert_eq!(Config::resolve_data_dir(), "/vol");
    }

    #[test]
    fn follows_the_volume_mount() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("RAILWAY_VOLUME_MOUNT_PATH", "/bitnami/redis/data");
        assert_eq!(Config::resolve_data_dir(), "/bitnami/redis/data");
    }

    #[test]
    fn empty_mount_falls_back_to_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("RAILWAY_VOLUME_MOUNT_PATH", "");
        assert_eq!(Config::resolve_data_dir(), "/data");
    }

    #[test]
    fn no_env_falls_back_to_default() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        assert_eq!(Config::resolve_data_dir(), "/data");
    }

    // --- data_dir_is_on_volume: every branch ---

    #[test]
    fn dir_equal_to_mount_is_on_volume() {
        assert!(data_dir_is_on_volume("/data", "/data"));
    }

    #[test]
    fn subdirectory_of_mount_is_on_volume() {
        assert!(data_dir_is_on_volume("/data/redis", "/data"));
    }

    #[test]
    fn sibling_sharing_a_prefix_is_not_on_volume() {
        // The bug a plain starts_with would introduce.
        assert!(!data_dir_is_on_volume("/database", "/data"));
    }

    #[test]
    fn unrelated_dir_is_not_on_volume() {
        assert!(!data_dir_is_on_volume("/tmp/elsewhere", "/data"));
    }

    #[test]
    fn empty_mount_is_never_on_volume() {
        assert!(!data_dir_is_on_volume("/data", ""));
    }

    // --- from_env: every branch of its validation ---

    #[test]
    fn from_env_requires_a_password() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        let err = Config::from_env()
            .err()
            .expect("should fail without a password");
        assert!(err.to_string().contains("REDIS_PASSWORD"));
    }

    #[test]
    fn from_env_rejects_sentinel_without_hosts() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("REDIS_PASSWORD", "pw");
        env::set_var("SENTINEL_ENABLED", "true");
        let err = Config::from_env()
            .err()
            .expect("should fail without sentinel hosts");
        assert!(err.to_string().contains("SENTINEL_HOSTS"));
    }

    #[test]
    fn from_env_accepts_sentinel_with_hosts() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("REDIS_PASSWORD", "pw");
        env::set_var("SENTINEL_ENABLED", "true");
        env::set_var("SENTINEL_HOSTS", "redis-1:26379");
        let config = Config::from_env().unwrap();
        assert!(config.sentinel_enabled);
        assert_eq!(config.sentinel_hosts, "redis-1:26379");
    }

    #[test]
    fn from_env_accepts_sentinel_disabled_without_hosts() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("REDIS_PASSWORD", "pw");
        let config = Config::from_env().unwrap();
        assert!(!config.sentinel_enabled);
    }

    // --- is_primary / initial master host and port: every branch ---

    #[test]
    fn empty_replica_of_means_primary() {
        assert!(Config::for_tests().is_primary());
    }

    #[test]
    fn set_replica_of_means_replica() {
        let mut config = Config::for_tests();
        config.replica_of = "master:6379".to_string();
        assert!(!config.is_primary());
    }

    #[test]
    fn primary_master_host_is_own_domain() {
        let config = Config::for_tests();
        assert_eq!(config.initial_master_host(), "redis-1.railway.internal");
        assert_eq!(config.initial_master_port(), 6379);
    }

    #[test]
    fn replica_master_host_and_port_come_from_replica_of() {
        let mut config = Config::for_tests();
        config.replica_of = "master-host:7000".to_string();
        assert_eq!(config.initial_master_host(), "master-host");
        assert_eq!(config.initial_master_port(), 7000);
    }

    #[test]
    fn replica_of_without_port_falls_back_to_own_port() {
        let mut config = Config::for_tests();
        config.replica_of = "master-host".to_string();
        assert_eq!(config.initial_master_host(), "master-host");
        assert_eq!(config.initial_master_port(), 6379);
    }

    #[test]
    fn replica_of_with_junk_port_falls_back_to_own_port() {
        let mut config = Config::for_tests();
        config.replica_of = "master-host:abc".to_string();
        assert_eq!(config.initial_master_port(), 6379);
    }
}
