use crate::boot_role::BootMaster;
use crate::config::Config;

/// Move a sentinel.conf whose recorded topology no longer exists out of the
/// way, so Sentinel doesn't resume monitoring a world that is gone.
///
/// A volume reused across a template revert, scale-down, or re-conversion
/// still carries the old cluster's sentinel state — a monitor line naming a
/// master that isn't any currently-declared member. Resuming that state
/// demotes the node into a replica of a ghost: nothing to sync from, nothing
/// to fail over to, no writable master anywhere. Renamed aside, never
/// deleted — the file is the only record of the old world's failover
/// history. Returns the quarantine path when something moved.
pub fn quarantine_ghost_sentinel_conf(
    data_dir: &str,
) -> std::io::Result<Option<std::path::PathBuf>> {
    let conf = std::path::Path::new(data_dir).join("sentinel.conf");
    if !conf.exists() {
        return Ok(None);
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ghost = std::path::Path::new(data_dir).join(format!("sentinel.conf.ghost-{}", ts));
    std::fs::rename(&conf, &ghost)?;
    Ok(Some(ghost))
}

/// Generate sentinel.conf content from environment configuration.
///
/// This file is only written on first boot. After Sentinel runs a failover it
/// rewrites the file with the new master address, so we preserve whatever is
/// already on disk across restarts.
///
/// The monitor line follows the resolved boot master, not the raw env
/// topology: a node whose first boot learned the current master from peer
/// Sentinels must start monitoring that master, or its own Sentinel would
/// begin life believing the stamped-at-deploy topology the rest of the
/// cluster has already failed over from.
pub fn generate_sentinel_conf(config: &Config, boot_master: &BootMaster) -> String {
    let (master_host, master_port) = match boot_master {
        BootMaster::SelfIsMaster => (config.private_domain.clone(), config.redis_port),
        BootMaster::ReplicaOf(host, port) => (host.clone(), *port),
        BootMaster::NoLocalState => (config.initial_master_host(), config.initial_master_port()),
    };

    let mut lines: Vec<String> = vec![
        format!("port {}", config.sentinel_port),
        "daemonize no".to_string(),
        "logfile \"\"".to_string(),
        "loglevel notice".to_string(),
        // Resolve peers by DNS hostname so Railway's internal DNS works
        "sentinel resolve-hostnames yes".to_string(),
        "sentinel announce-hostnames yes".to_string(),
        // Announce this Sentinel under the node's stable private hostname,
        // exactly like redis.conf's replica-announce-ip does for the data
        // side. Without it announce-hostnames only formats what Sentinel
        // knows — it still gossips its raw container IP, which changes on
        // every redeploy and, worse, is what peers would hand to the
        // deletion probe (an IP is not a resolvable name, so a peer known
        // only by IP could never be proven deleted). Peers key sentinels by
        // runid, so existing clusters absorb the address switch in place.
        format!("sentinel announce-ip {}", config.private_domain),
        format!("sentinel announce-port {}", config.sentinel_port),
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
