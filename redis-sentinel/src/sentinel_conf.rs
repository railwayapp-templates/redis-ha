use crate::boot_role::BootMaster;
use crate::config::Config;

/// Whether a sentinel.conf on disk actually enforces client auth right now,
/// via a non-empty `requirepass`.
///
/// This is the ground truth every LOCAL Sentinel client in this crate must
/// defer to instead of assuming the env-derived posture applies:
/// `requirepass` is written only when [`generate_sentinel_conf`] creates a
/// *fresh* file, and Sentinel owns the file after that (see the module doc
/// on [`quarantine_ghost_sentinel_conf`]) — `SENTINEL CONFIG SET` cannot add
/// it to a preserved conf at runtime (see `quorum::ensure_announce_identity`
/// and the Redis Sentinel docs' enumerated global parameters, which list
/// `sentinel-user`/`sentinel-pass` but not `requirepass`). So a preserved
/// conf from before Sentinel auth existed keeps requiring no auth for the
/// life of this boot, no matter that auth is now the default for new
/// clusters.
///
/// That gap matters here specifically because sending `AUTH` to a Sentinel
/// that has no password configured is a hard connection failure in Redis
/// ("Client sent AUTH, but no password is set"), not a harmless no-op.
/// Blindly authenticating every local URL would turn this image's rollout
/// onto an already-running unauthenticated cluster into an outage of every
/// local watcher (quorum-sync, link-heal, the health server,
/// demote-on-shutdown) on top of not even closing the auth gap on that
/// node. Reading the file back is what lets the wrapper send exactly the
/// AUTH the co-located Sentinel will actually accept.
///
/// Whitespace-tolerant and case-insensitive on the keyword, matching
/// [`crate::boot_role::parse_sentinel_monitor`]'s style. A quoted empty
/// value (`requirepass ""`) — valid Redis conf syntax for "no password" —
/// counts as no auth, not as a password of two quote characters.
pub fn conf_requires_auth(contents: &str) -> bool {
    contents.lines().any(|line| {
        let mut fields = line.split_whitespace();
        if !matches!(fields.next(), Some(kw) if kw.eq_ignore_ascii_case("requirepass")) {
            return false;
        }
        fields
            .next()
            .is_some_and(|value| !value.trim_matches('"').is_empty())
    })
}

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
///
/// `sentinel_password` is the outcome of the first-boot auth decision
/// (`sentinel_auth::first_boot_sentinel_password`): the cluster's shared
/// `REDIS_PASSWORD` when this boot enables Sentinel auth, `""` (no auth
/// lines) when it must stay open — because the peers it is joining are
/// open, or the `SENTINEL_AUTH` kill switch is off.
pub fn generate_sentinel_conf(
    config: &Config,
    boot_master: &BootMaster,
    sentinel_password: &str,
) -> String {
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
    ];

    // Sentinel client auth (empty = none — the caller decides, see the doc
    // comment). `requirepass` is the directive that protects THIS
    // Sentinel's own port from an unauthenticated `SENTINEL
    // SET/RESET/FAILOVER/REMOVE` (Redis Sentinel docs, "Sentinel
    // password-only authentication"). `sentinel sentinel-pass` is how this
    // Sentinel authenticates when it dials OUT to a peer Sentinel that also
    // requires auth; the same docs section notes that with a single shared
    // password and no ACL, Sentinel already falls back to using its own
    // `requirepass` for outbound auth when no `sentinel-pass` is
    // configured, so this is redundant with that fallback — it is set
    // explicitly anyway so outbound auth doesn't depend on an implicit
    // default. `sentinel sentinel-user` is deliberately omitted: that
    // directive names a NON-default ACL superuser for outbound auth, and
    // password-only auth (this configuration, no ACL) always authenticates
    // outbound as `default`, which needs no user directive at all.
    if !sentinel_password.is_empty() {
        lines.push(format!("requirepass {}", sentinel_password));
        lines.push(format!("sentinel sentinel-pass {}", sentinel_password));
    }

    lines.extend([
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
        // Bounds how long Sentinel tolerates a rebooted master answering
        // -LOADING before treating it as down. Upstream ships this at 0,
        // which is not "no bound" but "no reboot-triggered bound at all" —
        // the pre-7.0 behavior of waiting on that master's own load time,
        // however long it takes. Non-zero gives Sentinel an independent,
        // bounded trigger into failing over to a replica that already has
        // data instead of leaving the cluster hostage to that wait.
        format!(
            "sentinel master-reboot-down-after-period {} {}",
            config.redis_master_name, config.sentinel_master_reboot_down_after_ms
        ),
    ]);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boot_role::BootMaster;

    #[test]
    fn master_reboot_down_after_period_defaults_to_ten_seconds() {
        let config = Config::for_tests();
        let conf = generate_sentinel_conf(&config, &BootMaster::NoLocalState, "");
        assert!(conf.contains("sentinel master-reboot-down-after-period mymaster 10000"));
    }

    // Must never regress to upstream's shipped 0, which disables the
    // reboot-triggered down path entirely — see the comment on the directive.
    #[test]
    fn master_reboot_down_after_period_is_configurable_via_env() {
        let mut config = Config::for_tests();
        config.sentinel_master_reboot_down_after_ms = 20000;
        let conf = generate_sentinel_conf(&config, &BootMaster::NoLocalState, "");
        assert!(conf.contains("sentinel master-reboot-down-after-period mymaster 20000"));
    }

    // --- generate_sentinel_conf: auth lines ---

    #[test]
    fn an_open_boot_writes_no_auth_lines() {
        // An open first boot (peers answered unauthenticated, or the
        // SENTINEL_AUTH kill switch is off) must reproduce the pre-auth
        // conf exactly: no requirepass, no sentinel-pass, no sentinel-user.
        let config = Config::for_tests();
        let conf = generate_sentinel_conf(&config, &BootMaster::SelfIsMaster, "");
        assert!(!conf.lines().any(|l| l.starts_with("requirepass")));
        assert!(!conf.contains("sentinel-pass"));
        assert!(!conf.contains("sentinel-user"));
    }

    #[test]
    fn an_authed_boot_adds_requirepass_and_sentinel_pass_but_never_sentinel_user() {
        let config = Config::for_tests();
        let conf = generate_sentinel_conf(&config, &BootMaster::SelfIsMaster, "s3cr3t");
        assert!(conf.lines().any(|l| l == "requirepass s3cr3t"));
        assert!(conf
            .lines()
            .any(|l| l == "sentinel sentinel-pass s3cr3t"));
        // Password-only auth with the default user needs no user directive
        // (verified against the Redis Sentinel docs — see the doc comment
        // on generate_sentinel_conf).
        assert!(!conf.contains("sentinel-user"));
    }

    #[test]
    fn the_reused_redis_password_lands_in_exactly_the_three_auth_directives() {
        // The real call site passes the cluster's REDIS_PASSWORD as the
        // sentinel password, so the one secret appears exactly three times:
        // the pre-existing `sentinel auth-pass` (data-side auth) plus the
        // two Sentinel-side lines this feature adds.
        let mut config = Config::for_tests();
        config.redis_password = "hunter2".to_string();
        let conf =
            generate_sentinel_conf(&config, &BootMaster::SelfIsMaster, &config.redis_password);
        assert_eq!(conf.matches("hunter2").count(), 3);
        assert!(conf.lines().any(|l| l == "requirepass hunter2"));
        assert!(conf.lines().any(|l| l == "sentinel sentinel-pass hunter2"));
        assert!(conf
            .lines()
            .any(|l| l == "sentinel auth-pass mymaster hunter2"));
    }

    // --- conf_requires_auth ---

    #[test]
    fn no_requirepass_line_is_no_auth() {
        assert!(!conf_requires_auth("port 26379\ndaemonize no\n"));
        assert!(!conf_requires_auth(""));
    }

    #[test]
    fn a_requirepass_line_with_a_value_requires_auth() {
        assert!(conf_requires_auth("port 26379\nrequirepass hunter2\n"));
    }

    #[test]
    fn requirepass_is_case_insensitive_and_whitespace_tolerant() {
        assert!(conf_requires_auth("RequirePass   hunter2\n"));
        assert!(conf_requires_auth("  requirepass\thunter2  \n"));
    }

    #[test]
    fn a_quoted_empty_password_is_no_auth() {
        // Valid Redis conf syntax for "no password" — must not be confused
        // with a two-character password of two quote marks.
        assert!(!conf_requires_auth(r#"requirepass """#));
    }

    #[test]
    fn a_quoted_nonempty_password_still_requires_auth() {
        assert!(conf_requires_auth(r#"requirepass "hunter2""#));
    }

    #[test]
    fn a_truncated_requirepass_line_is_no_auth() {
        assert!(!conf_requires_auth("requirepass\n"));
    }

    #[test]
    fn other_directives_do_not_match() {
        assert!(!conf_requires_auth(
            "sentinel auth-pass mymaster hunter2\nmasterauth hunter2\n"
        ));
    }
}
