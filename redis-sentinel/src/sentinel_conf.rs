use crate::config::Config;

/// Generate sentinel.conf content from environment configuration.
///
/// This file is only written on first boot. After Sentinel runs a failover it
/// rewrites the file with the new master address, so we preserve whatever is
/// already on disk across restarts.
pub fn generate_sentinel_conf(config: &Config) -> String {
    let master_host = config.initial_master_host();
    let master_port = config.initial_master_port();

    let mut lines: Vec<String> = vec![
        format!("port {}", config.sentinel_port),
        "daemonize no".to_string(),
        "logfile \"\"".to_string(),
        "loglevel notice".to_string(),
        // Resolve peers by DNS hostname so Railway's internal DNS works
        "sentinel resolve-hostnames yes".to_string(),
        "sentinel announce-hostnames yes".to_string(),
        // Monitor the master set
        format!(
            "sentinel monitor {} {} {} {}",
            config.redis_master_name, master_host, master_port, config.sentinel_quorum
        ),
        format!(
            "sentinel auth-pass {} {}",
            config.redis_master_name, config.redis_password
        ),
        format!(
            "sentinel down-after-milliseconds {} {}",
            config.redis_master_name, config.sentinel_down_after_ms
        ),
        format!(
            "sentinel failover-timeout {} {}",
            config.redis_master_name, config.sentinel_failover_timeout_ms
        ),
        // Allow one replica to sync at a time during failover
        format!("sentinel parallel-syncs {} 1", config.redis_master_name),
        // Reboot detection: treat a restarted master that looks like it came back
        // too quickly as potentially still-broken
        format!(
            "sentinel master-reboot-down-after-period {} 0",
            config.redis_master_name
        ),
    ];

    // Inject known peers so gossip bootstraps faster
    for peer in config.sentinel_hosts.split(',') {
        let peer = peer.trim();
        if peer.is_empty() {
            continue;
        }
        let parts: Vec<&str> = peer.splitn(2, ':').collect();
        if parts.len() == 2 && !parts[0].is_empty() {
            if let Ok(port) = parts[1].parse::<u16>() {
                lines.push(format!("sentinel known-sentinel {} {} {}", config.redis_master_name, parts[0], port));
            }
        }
    }

    lines.join("\n") + "\n"
}
