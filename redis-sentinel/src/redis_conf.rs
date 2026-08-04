use crate::config::Config;

/// Whether this boot has to adopt an RDB-only dataset.
///
/// Redis loads the AOF, not the RDB, whenever `appendonly yes` is set at
/// startup. On a volume that holds a `dump.rdb` but no `appendonlydir` — a
/// standalone Redis being adopted as a cluster primary, since Railway's
/// standalone template runs `--save 60 1` with no AOF — that means booting
/// from an empty AOF and silently abandoning the customer's data, with
/// `dump.rdb` left untouched on disk beside it.
///
/// So start with AOF off, let Redis load the RDB, and switch AOF on at
/// runtime (`CONFIG SET appendonly yes`), which is the documented migration
/// and rewrites the AOF from the in-memory dataset.
pub fn needs_rdb_to_aof_migration(data_dir: &str) -> bool {
    std::path::Path::new(&format!("{}/dump.rdb", data_dir)).exists()
        && !std::path::Path::new(&format!("{}/appendonlydir", data_dir)).exists()
}

pub fn generate_redis_conf(config: &Config) -> String {
    let adopting_rdb = needs_rdb_to_aof_migration(&config.data_dir);

    let mut lines: Vec<String> = vec![
        format!("port {}", config.redis_port),
        format!("requirepass {}", config.redis_password),
        "protected-mode yes".to_string(),
        // Persist data to the volume. Deliberately off for this one boot when
        // adopting an RDB-only dataset — enabled at runtime once the RDB is
        // loaded (see needs_rdb_to_aof_migration).
        if adopting_rdb {
            "appendonly no".to_string()
        } else {
            "appendonly yes".to_string()
        },
        "appendfsync everysec".to_string(),
        format!("dir {}", config.data_dir),
        // Log to stdout so Railway captures it
        "logfile \"\"".to_string(),
        "loglevel notice".to_string(),
        // Allow replication from any host on the private network. Railway's
        // private network is IPv6 (fd12::... hostnames) — binding only 0.0.0.0
        // leaves Redis unreachable from any peer connecting over it.
        "bind 0.0.0.0 ::".to_string(),
        // Announce this node's stable private hostname (not its IP, which changes on
        // redeploy) to the master/replicas during replication handshake. The "ip" name
        // is legacy — the field accepts any string, including a hostname.
        format!("replica-announce-ip {}", config.private_domain),
        format!("replica-announce-port {}", config.redis_port),
        "cluster-preferred-endpoint-type hostname".to_string(),
        // Split-brain fence: master stops accepting writes when it loses contact
        // with all replicas for longer than min-replicas-max-lag seconds.
        // Bounds the split-brain window on network partition to this lag rather
        // than letting the isolated master accept writes indefinitely.
        // 1 replica required — self-fences only when fully isolated.
        "min-replicas-to-write 1".to_string(),
        // Must be <= SENTINEL_DOWN_AFTER_MS (5s default) so the master goes
        // read-only around the same time Sentinel declares it ODOWN elsewhere.
        "min-replicas-max-lag 10".to_string(),
    ];

    if !config.is_primary() {
        // Parse REPLICA_OF as "host:port"
        let parts: Vec<&str> = config.replica_of.splitn(2, ':').collect();
        if parts.len() == 2 {
            lines.push(format!("replicaof {} {}", parts[0], parts[1]));
        }
        // Replicas need the master password to authenticate
        lines.push(format!("masterauth {}", config.redis_password));
    }

    lines.join("\n") + "\n"
}
