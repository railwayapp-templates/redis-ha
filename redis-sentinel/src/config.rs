use anyhow::{bail, Result};
use common::{ConfigExt, RailwayEnv};
use std::env;

pub struct Config {
    pub redis_password: String,
    pub redis_port: u16,
    /// Host:port of the master to replicate from. Empty string on the primary node.
    pub replica_of: String,
    /// Seconds of replica ACK silence the split-brain fence tolerates before
    /// that replica stops counting toward min-replicas-to-write. Must not
    /// exceed sentinel_down_after_ms — see redis_conf's module docs.
    pub min_replicas_max_lag_secs: u64,
    /// Passthrough value for `repl-backlog-size` (e.g. "64mb").
    pub repl_backlog_size: String,
    /// The three-value tail of `client-output-buffer-limit replica <hard>
    /// <soft> <soft-seconds>` (e.g. "512mb 128mb 120").
    pub client_output_buffer_limit_replica: String,
    pub sentinel_enabled: bool,
    pub sentinel_port: u16,
    pub sentinel_quorum: u32,
    /// Comma-separated "host:port" list of all Sentinel peers.
    pub sentinel_hosts: String,
    pub redis_master_name: String,
    pub sentinel_down_after_ms: u64,
    pub sentinel_failover_timeout_ms: u64,
    /// Milliseconds Sentinel tolerates a rebooted master answering -LOADING
    /// before treating it as down. 0 (upstream's shipped default) disables
    /// this path entirely — see sentinel_conf's comment on the directive.
    pub sentinel_master_reboot_down_after_ms: u64,
    pub health_port: u16,
    pub data_dir: String,
    /// The hostname of this service's private domain (used to derive master host for sentinels).
    pub private_domain: String,
    /// `maxmemory` in bytes: an explicit `MAXMEMORY_MB` override, or 75% of
    /// the container's own cgroup memory limit when one can be detected.
    /// `None` when neither is available — Redis then gets no ceiling at all,
    /// the behavior every boot had before this field existed.
    ///
    /// Unset entirely, Redis grows without bound: with no eviction limit to
    /// hit, the container's own cgroup eventually OOM-kills the process —
    /// and BGSAVE/AOF-rewrite's fork() briefly doubles resident memory via
    /// copy-on-write, so a `maxmemory` sized flush against the cgroup limit
    /// (rather than comfortably under it) lets a routine save trigger the
    /// same kill. 75%, not 100%, leaves room for exactly that spike plus
    /// client/replication buffers, which don't count against `maxmemory`
    /// either.
    pub maxmemory_bytes: Option<u64>,
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
            min_replicas_max_lag_secs: u64::env_parse("MIN_REPLICAS_MAX_LAG", 5),
            repl_backlog_size: String::env_or("REPL_BACKLOG_SIZE", "64mb"),
            client_output_buffer_limit_replica: String::env_or(
                "CLIENT_OUTPUT_BUFFER_LIMIT_REPLICA",
                "512mb 128mb 120",
            ),
            sentinel_enabled,
            sentinel_port: u16::env_parse("SENTINEL_PORT", 26379),
            sentinel_quorum: u32::env_parse("SENTINEL_QUORUM", 2),
            sentinel_hosts,
            redis_master_name: String::env_or("REDIS_MASTER_NAME", "mymaster"),
            sentinel_down_after_ms: u64::env_parse("SENTINEL_DOWN_AFTER_MS", 5000),
            sentinel_failover_timeout_ms: u64::env_parse("SENTINEL_FAILOVER_TIMEOUT_MS", 30000),
            sentinel_master_reboot_down_after_ms: u64::env_parse(
                "SENTINEL_MASTER_REBOOT_DOWN_AFTER_MS",
                10000,
            ),
            health_port: u16::env_parse("HEALTH_PORT", 8080),
            data_dir: Self::resolve_data_dir(),
            private_domain: RailwayEnv::private_domain(),
            maxmemory_bytes: Self::resolve_maxmemory_bytes(),
        })
    }

    /// An explicit `MAXMEMORY_MB` wins outright; otherwise 75% of whatever
    /// cgroup memory limit can be detected. See the field doc on
    /// `maxmemory_bytes` for why a limit is worth having at all.
    fn resolve_maxmemory_bytes() -> Option<u64> {
        if let Ok(raw) = env::var("MAXMEMORY_MB") {
            if let Ok(mb) = raw.trim().parse::<u64>() {
                return Some(mb.saturating_mul(1024 * 1024));
            }
        }
        Self::detect_cgroup_memory_limit_bytes("/sys/fs/cgroup").map(|limit| limit * 3 / 4)
    }

    /// Reads this container's own cgroup memory limit, in bytes. Tries
    /// cgroup v2 (`memory.max`, the modern default) first, falling back to
    /// v1 (`memory/memory.limit_in_bytes`). The two report "no limit"
    /// differently — v2 as the literal string `max`, v1 as an enormous
    /// sentinel near `i64::MAX` rounded to a page boundary — so both are
    /// normalized through the same ceiling: a "limit" at or above 1 TiB is
    /// treated as unlimited, since no Railway plan grants anywhere near that
    /// and a real value that large would make a 75% cap meaningless anyway.
    /// A limit of exactly 0 is likewise treated as undetected rather than as
    /// "set maxmemory to 0" (which to Redis means *no* limit, the opposite of
    /// what a 0-byte cgroup reading would ever actually mean).
    fn detect_cgroup_memory_limit_bytes(cgroup_root: &str) -> Option<u64> {
        const UNLIMITED_CEILING_BYTES: u64 = 1024 * 1024 * 1024 * 1024; // 1 TiB

        let v2_limit = std::fs::read_to_string(format!("{cgroup_root}/memory.max"))
            .ok()
            .filter(|s| s.trim() != "max")
            .and_then(|s| s.trim().parse::<u64>().ok());
        let raw = match v2_limit {
            Some(limit) => Some(limit),
            None => std::fs::read_to_string(format!("{cgroup_root}/memory/memory.limit_in_bytes"))
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok()),
        }?;

        if raw == 0 || raw >= UNLIMITED_CEILING_BYTES {
            None
        } else {
            Some(raw)
        }
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
    /// template, and any customer is free to pick their own path. Hardcoding
    /// `/data` there meant redis started against an empty directory on the
    /// container filesystem while the real RDB/AOF sat unread on the volume.
    ///
    /// The mount is not always the data dir either. Bitnami's redis (Railway
    /// mirrors it as `railwayapp/redis`, and the `bitnami-redis` template
    /// mounts the volume at `/bitnami`) keeps its dataset one level down, in
    /// `<mount>/redis/data`. Taking the mount at face value there points redis
    /// at a directory holding only the `redis/` subtree — no RDB, no AOF — so
    /// it starts empty and the adopted dataset is silently abandoned, exactly
    /// the failure the paragraph above describes.
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
                return Self::resolve_nested_dataset(&mount);
            }
        }
        "/data".to_string()
    }

    /// Redirects to a nested dataset directory under `mount` when one holds the
    /// data and the mount root does not.
    ///
    /// Keyed on evidence rather than on the image, the same way the RDB->AOF
    /// adoption is: a bitnami-lineage volume is recognised by carrying redis
    /// files under `redis/data`, so a customer who set `REDIS_DATA_DIR` to that
    /// layout on an official image is picked up too, and a fresh volume — where
    /// bitnami's `redis/data` exists but is empty — is left at the mount root
    /// so a new cluster keeps the plain layout.
    fn resolve_nested_dataset(mount: &str) -> String {
        // Only when the mount root itself has nothing to lose. If both hold
        // data, the mount root is what a previous HA boot wrote and must win —
        // redirecting would strand whatever has been written since.
        if Self::holds_redis_dataset(mount) {
            return mount.to_string();
        }
        let nested = format!("{}/redis/data", mount.trim_end_matches('/'));
        if Self::holds_redis_dataset(&nested) {
            return nested;
        }
        mount.to_string()
    }

    /// True if `dir` contains a dataset redis could actually load — an RDB
    /// snapshot, a committed multi-part AOF, or a pre-7 single-file AOF.
    ///
    /// Loadable is the operative word, which is why the AOF check goes through
    /// `aof_manifest_exists` rather than testing for `appendonlydir`: a crashed
    /// adoption leaves that directory holding orphan files Redis cannot read.
    /// Counting it would keep a mount root whose data is unreadable and abandon
    /// a perfectly good nested dataset.
    ///
    /// Also the empty-primary boot guard's "nothing to serve" test
    /// (`crate::boot_role::empty_primary_boot_guard`): a master with no
    /// loadable dataset is what a wiped or replaced volume boots from.
    pub(crate) fn holds_redis_dataset(dir: &str) -> bool {
        let path = std::path::Path::new(dir);
        path.join("dump.rdb").exists()
            || crate::redis_conf::aof_manifest_exists(dir)
            || path.join("appendonly.aof").exists()
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
            min_replicas_max_lag_secs: 5,
            repl_backlog_size: "64mb".to_string(),
            client_output_buffer_limit_replica: "512mb 128mb 120".to_string(),
            sentinel_enabled: true,
            sentinel_port: 26379,
            sentinel_quorum: 2,
            sentinel_hosts: "redis-1:26379".to_string(),
            redis_master_name: "mymaster".to_string(),
            sentinel_down_after_ms: 5000,
            sentinel_failover_timeout_ms: 30000,
            sentinel_master_reboot_down_after_ms: 10000,
            health_port: 8080,
            data_dir: "/data".to_string(),
            private_domain: "redis-1.railway.internal".to_string(),
            maxmemory_bytes: None,
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
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
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
            "MAXMEMORY_MB",
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

    // --- resolve_nested_dataset: the bitnami layout ---
    //
    // These touch the filesystem rather than the environment, so they take no
    // ENV_LOCK: each builds its own uniquely-named directory under the temp
    // dir and reads only that.

    static NEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A unique empty directory, standing in for a mounted volume.
    fn temp_mount(label: &str) -> PathBuf {
        let n = NEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("redis-ha-{}-{}-{}", std::process::id(), label, n));
        fs::create_dir_all(&dir).expect("create temp mount");
        dir
    }

    fn touch(path: PathBuf) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        fs::write(path, b"").expect("write file");
    }

    #[test]
    fn plain_layout_stays_at_the_mount_root() {
        let mount = temp_mount("plain");
        touch(mount.join("dump.rdb"));

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.to_str().unwrap()
        );
    }

    #[test]
    fn bitnami_layout_redirects_to_the_nested_dataset() {
        // What the `bitnami-redis` template produces: volume mounted at
        // /bitnami, dataset written to /bitnami/redis/data. Reading the mount
        // root here is the data-loss bug this function exists to prevent.
        let mount = temp_mount("bitnami");
        touch(mount.join("redis/data/appendonlydir/appendonly.aof.manifest"));

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.join("redis/data").to_str().unwrap()
        );
    }

    #[test]
    fn a_pre_7_single_file_aof_counts_as_a_dataset() {
        let mount = temp_mount("legacy-aof");
        touch(mount.join("redis/data/appendonly.aof"));

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.join("redis/data").to_str().unwrap()
        );
    }

    #[test]
    fn a_fresh_volume_keeps_the_plain_layout() {
        // Bitnami creates redis/data on first boot, so its mere existence must
        // not redirect a cluster that has no dataset to adopt.
        let mount = temp_mount("fresh");
        fs::create_dir_all(mount.join("redis/data")).expect("create empty nested");

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.to_str().unwrap()
        );
    }

    #[test]
    fn the_mount_root_wins_when_both_hold_data() {
        // A second boot after a conversion: the HA node has been writing at the
        // mount root, so redirecting to the stale nested copy would strand
        // every write since the conversion.
        let mount = temp_mount("both");
        touch(mount.join("appendonlydir/appendonly.aof.manifest"));
        touch(mount.join("redis/data/dump.rdb"));

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.to_str().unwrap()
        );
    }

    #[test]
    fn an_orphan_aof_dir_at_the_root_does_not_hold_the_nested_dataset_hostage() {
        // An adoption that crashed before its rewrite committed leaves an
        // appendonlydir with no manifest — orphan files Redis cannot load. The
        // nested dataset is the only readable copy, so it has to win.
        let mount = temp_mount("orphan-root");
        touch(mount.join("appendonlydir/appendonly.aof.1.base.rdb"));
        touch(mount.join("redis/data/dump.rdb"));

        assert_eq!(
            Config::resolve_nested_dataset(mount.to_str().unwrap()),
            mount.join("redis/data").to_str().unwrap()
        );
    }

    #[test]
    fn a_trailing_slash_on_the_mount_does_not_double_up() {
        let mount = temp_mount("trailing");
        touch(mount.join("redis/data/dump.rdb"));
        let with_slash = format!("{}/", mount.to_str().unwrap());

        assert_eq!(
            Config::resolve_nested_dataset(&with_slash),
            mount.join("redis/data").to_str().unwrap()
        );
    }

    // --- resolve_maxmemory_bytes: MAXMEMORY_MB override ---

    #[test]
    fn maxmemory_mb_override_wins_and_converts_to_bytes() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("MAXMEMORY_MB", "512");
        assert_eq!(
            Config::resolve_maxmemory_bytes(),
            Some(512 * 1024 * 1024)
        );
    }

    #[test]
    fn maxmemory_mb_ignores_a_junk_value_and_falls_through_to_detection() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_env();
        env::set_var("MAXMEMORY_MB", "not-a-number");
        // Falls through to cgroup detection, which finds nothing on a test
        // host with no /sys/fs/cgroup/memory.max at this literal path — so
        // this only asserts it doesn't panic or return the junk value as 0.
        assert_ne!(Config::resolve_maxmemory_bytes(), Some(0));
    }

    // --- detect_cgroup_memory_limit_bytes: every branch, via a fake cgroup root ---

    fn fake_cgroup_root(label: &str) -> PathBuf {
        let n = NEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "redis-ha-cgroup-{}-{}-{}",
            std::process::id(),
            label,
            n
        ));
        fs::create_dir_all(&dir).expect("create fake cgroup root");
        dir
    }

    #[test]
    fn reads_a_real_v2_limit() {
        let root = fake_cgroup_root("v2-real");
        fs::write(root.join("memory.max"), "2147483648\n").unwrap(); // 2 GiB
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            Some(2147483648)
        );
    }

    #[test]
    fn v2_max_falls_through_to_v1() {
        let root = fake_cgroup_root("v2-max-v1-real");
        fs::write(root.join("memory.max"), "max\n").unwrap();
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::write(root.join("memory/memory.limit_in_bytes"), "1073741824\n").unwrap(); // 1 GiB
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            Some(1073741824)
        );
    }

    #[test]
    fn missing_v2_file_falls_through_to_v1() {
        let root = fake_cgroup_root("no-v2");
        fs::create_dir_all(root.join("memory")).unwrap();
        fs::write(root.join("memory/memory.limit_in_bytes"), "536870912\n").unwrap(); // 512 MiB
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            Some(536870912)
        );
    }

    #[test]
    fn v1_unlimited_sentinel_is_treated_as_undetected() {
        let root = fake_cgroup_root("v1-unlimited");
        fs::write(root.join("memory.max"), "max\n").unwrap();
        fs::create_dir_all(root.join("memory")).unwrap();
        // The real cgroup v1 "no limit" sentinel: i64::MAX rounded down to a
        // 4096-byte page boundary.
        fs::write(
            root.join("memory/memory.limit_in_bytes"),
            "9223372036854771712",
        )
        .unwrap();
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            None
        );
    }

    #[test]
    fn a_limit_at_the_unlimited_ceiling_is_treated_as_undetected() {
        let root = fake_cgroup_root("v2-ceiling");
        fs::write(
            root.join("memory.max"),
            (1024u64 * 1024 * 1024 * 1024).to_string(), // exactly 1 TiB
        )
        .unwrap();
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            None
        );
    }

    #[test]
    fn a_zero_limit_is_treated_as_undetected_not_as_zero_maxmemory() {
        let root = fake_cgroup_root("v2-zero");
        fs::write(root.join("memory.max"), "0").unwrap();
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            None
        );
    }

    #[test]
    fn neither_file_present_is_undetected() {
        let root = fake_cgroup_root("neither");
        assert_eq!(
            Config::detect_cgroup_memory_limit_bytes(root.to_str().unwrap()),
            None
        );
    }
}
