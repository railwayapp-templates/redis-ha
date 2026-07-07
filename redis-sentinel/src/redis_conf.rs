use crate::config::Config;

pub fn generate_redis_conf(config: &Config) -> String {
    let mut lines: Vec<String> = vec![
        format!("port {}", config.redis_port),
        format!("requirepass {}", config.redis_password),
        "protected-mode yes".to_string(),
        // Persist data to the volume
        "appendonly yes".to_string(),
        "appendfsync everysec".to_string(),
        format!("dir {}", config.data_dir),
        // Log to stdout so Railway captures it
        "logfile \"\"".to_string(),
        "loglevel notice".to_string(),
        // Allow replication from any host on the private network
        "bind 0.0.0.0".to_string(),
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
