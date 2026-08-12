use crate::boot_role::BootMaster;
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
    let has_rdb = std::path::Path::new(&format!("{}/dump.rdb", data_dir)).exists();
    // The manifest, not the directory: `CONFIG SET appendonly yes` commits the
    // rewritten AOF by atomically renaming the manifest into place, so a crash
    // before that commit leaves an appendonlydir with orphan files Redis
    // cannot load. Keying on the directory would skip the migration on the
    // next boot and strand the (still intact) dump.rdb all over again.
    has_rdb && !aof_manifest_exists(data_dir)
}

/// Whether a committed (loadable) multi-part AOF exists — the manifest is the
/// atomic commit marker; base/incr files without it are unreadable orphans.
pub fn aof_manifest_exists(data_dir: &str) -> bool {
    std::path::Path::new(data_dir)
        .join("appendonlydir")
        .join("appendonly.aof.manifest")
        .exists()
}

/// Move a manifest-less `appendonlydir` out of the way before an RDB
/// adoption boot.
///
/// A previous adoption that crashed between `CONFIG SET appendonly yes` and
/// the rewrite committing its manifest leaves an appendonlydir Redis cannot
/// load, whose orphan files would collide with the AOF the new boot creates.
/// Renamed aside, never deleted — those files are the only trace of writes
/// accepted in that window. Returns the quarantine path when something moved.
pub fn quarantine_manifestless_aof_dir(
    data_dir: &str,
) -> std::io::Result<Option<std::path::PathBuf>> {
    let aof_dir = std::path::Path::new(data_dir).join("appendonlydir");
    if !aof_dir.exists() || aof_dir.join("appendonly.aof.manifest").exists() {
        return Ok(None);
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let orphaned = std::path::Path::new(data_dir).join(format!("appendonlydir.orphaned-{}", ts));
    std::fs::rename(&aof_dir, &orphaned)?;
    Ok(Some(orphaned))
}

/// The master this boot replicates from, or `None` when it starts as one.
///
/// `boot_master` is Sentinel's own persisted answer, which wins over
/// `REPLICA_OF` whenever it has one — see `crate::boot_role`. `NoLocalState`
/// is the first-boot/fallback path and reproduces the env-only behavior
/// exactly.
fn replicate_from(config: &Config, boot_master: &BootMaster) -> Option<(String, u16)> {
    match boot_master {
        BootMaster::NoLocalState => {
            if config.is_primary() {
                return None;
            }
            // Parse REPLICA_OF as "host:port"
            let parts: Vec<&str> = config.replica_of.splitn(2, ':').collect();
            match (parts.first(), parts.get(1).and_then(|p| p.parse::<u16>().ok())) {
                (Some(host), Some(port)) if !host.is_empty() => Some((host.to_string(), port)),
                _ => None,
            }
        }
        // Sentinel says this node is the master: no replicaof, whatever
        // REPLICA_OF says. A promoted node that restarts must not demote
        // itself back onto the node it was promoted over.
        BootMaster::SelfIsMaster => None,
        // Sentinel says someone else is: replicate from them, whatever
        // REPLICA_OF says — including on a node deployed as the initial
        // master, which is exactly the post-failover redeploy case.
        BootMaster::ReplicaOf(host, port) => Some((host.clone(), *port)),
    }
}

/// The split-brain fence for a cluster whose sentinel majority is `quorum`:
/// majority − 1, floored at 1 so the fence never switches itself off.
///
/// Why majority − 1 is exactly the safe value: in a partition, only the side
/// holding a sentinel majority can elect a new master. The old master's side
/// then holds at most (membership − majority) other nodes, i.e. at most
/// majority − 2 replicas (odd membership) — strictly fewer than this fence
/// requires, so it stops accepting writes. Any lower value re-opens the
/// two-writer window; any higher value fences the master on replica crashes
/// a failover could never be elected from anyway.
pub fn min_replicas_to_write(sentinel_quorum: u32) -> u32 {
    sentinel_quorum.saturating_sub(1).max(1)
}

/// The `requirepass` a previous boot's generated redis.conf carries,
/// unquoted. None on a fresh volume (no conf yet), when the line is absent,
/// or when the value is empty. Only the wrapper's own generated conf — or
/// Redis's `CONFIG REWRITE` of it, which writes the same escaping — is ever
/// parsed here, so the quoting dialect is exactly `quote_conf_value`'s.
pub fn persisted_requirepass(data_dir: &str) -> Option<String> {
    let conf = std::fs::read_to_string(format!("{}/redis.conf", data_dir)).ok()?;
    parse_requirepass(&conf).filter(|p| !p.is_empty())
}

fn parse_requirepass(conf: &str) -> Option<String> {
    for line in conf.lines() {
        let mut fields = line.trim().splitn(2, char::is_whitespace);
        if !matches!(fields.next(), Some(kw) if kw.eq_ignore_ascii_case("requirepass")) {
            continue;
        }
        let Some(value) = fields.next() else { continue };
        return Some(unquote_conf_value(value.trim()));
    }
    None
}

/// Inverse of [`quote_conf_value`]: strips the surrounding quotes and the
/// `\\`/`\"` escapes. An unquoted value (a hand-edited conf) passes through
/// as-is.
fn unquote_conf_value(value: &str) -> String {
    let Some(inner) = value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return value.to_string();
    };
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(escaped) = chars.next() {
                out.push(escaped);
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Renders `value` as a double-quoted config-file token, safe for any byte
/// value a password could actually contain — space, `#`, a bare `'` or `"`,
/// all of which corrupt or truncate an unquoted `requirepass`/`masterauth`
/// line (worst case: everything after a stray `#` silently becomes a
/// comment, and the node boots with no password at all). Backslash and
/// double-quote — the two characters that let a crafted value escape the
/// quotes and inject a second directive onto the same line — are
/// backslash-escaped; every other byte passes through unchanged inside the
/// quotes. Redis's own config parser (`sdssplitargs`, shared by redis.conf
/// and sentinel.conf, and what `CONFIG REWRITE` itself both reads and
/// writes) decodes exactly this escaping. Always quoting — not just when the
/// value looks like it needs it — means one code path instead of a
/// "does this need quoting" branch to get wrong.
///
/// A password can carry any of this: conversion adopts whatever
/// `REDIS_PASSWORD` the standalone service already had, which the customer
/// set, not this image.
pub fn quote_conf_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

pub fn generate_redis_conf(config: &Config, boot_master: &BootMaster) -> String {
    let adopting_rdb = needs_rdb_to_aof_migration(&config.data_dir);

    let mut lines: Vec<String> = vec![
        format!("port {}", config.redis_port),
        format!("requirepass {}", quote_conf_value(&config.redis_password)),
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
        // Sized for full resyncs, not partial ones: the 1MB repl-backlog-size
        // default forces a FULL resync on any disconnect longer than it takes
        // to fill 1MB of writes, and the default client-output-buffer-limit
        // for replicas (256mb hard / 64mb soft-60s) lets that same resync
        // blow its own output buffer mid-transfer on anything but a small
        // dataset — either one turns a short blip into a resync loop instead
        // of a completed one. Stamped on every boot, standalone included: a
        // reverted-HA root keeps this image and may gain replicas again
        // later, and both directives are inert without any.
        format!("repl-backlog-size {}", config.repl_backlog_size),
        format!(
            "client-output-buffer-limit replica {}",
            config.client_output_buffer_limit_replica
        ),
    ];

    // Absent when no cgroup memory limit could be detected and no
    // MAXMEMORY_MB override was set — Redis then has no ceiling, same as
    // every boot before this existed. `noeviction`, not an eviction policy:
    // this dataset backs correctness-sensitive uses (queues, idempotency
    // keys, rate limits), so silently discarding a live key under memory
    // pressure is worse than failing the write that would have crossed the
    // ceiling. See Config::maxmemory_bytes for why 75%, not 100%.
    if let Some(bytes) = config.maxmemory_bytes {
        lines.push(format!("maxmemory {bytes}"));
        lines.push("maxmemory-policy noeviction".to_string());
    }

    if config.sentinel_enabled {
        // Split-brain fence: master stops accepting writes when the replicas
        // still acking it drop below the count a majority-side partition
        // would leave it. Sized from the stamped quorum (majority − 1): 1 on
        // a 3-node cluster, 2 on 5, 3 on 7. A fixed 1 only fences a FULLY
        // isolated master — on a 5-node cluster a partition that traps one
        // replica with the old master leaves both sides writable until the
        // network heals, and everything the old side accepted is discarded.
        // The quorum-sync watcher keeps this converged with the live
        // membership at runtime; this is the boot-time stamp.
        //
        // HA boots only: a standalone boot (SENTINEL_ENABLED unset — e.g. a
        // root whose HA template was reverted, which keeps this image) has no
        // replicas by definition, so the fence would permanently reject every
        // write with NOREPLICAS.
        lines.push(format!(
            "min-replicas-to-write {}",
            min_replicas_to_write(config.sentinel_quorum)
        ));
        // The lag bound is the dual-writer window: during a partition the
        // isolated master keeps accepting writes until it stops seeing ACKs
        // from enough replicas for this many seconds, while the healthy
        // majority is running its own SENTINEL_DOWN_AFTER_MS clock toward
        // promoting a replacement. This must not exceed that down-after
        // window (5s default) — any larger and every partition guarantees a
        // dual-writer window longer than Sentinel's own failover trigger,
        // and everything the isolated side accepts in it is discarded on
        // heal.
        lines.push(format!(
            "min-replicas-max-lag {}",
            config.min_replicas_max_lag_secs
        ));
    }

    // Every node carries the master password, not just the ones deployed as
    // replicas: Sentinel demotes an old master into a replica in place, and a
    // node that has no masterauth when that happens can never complete the
    // handshake ("MASTER aborted replication with an error: NOAUTH
    // Authentication required"). It keeps reporting role:slave while serving a
    // dataset frozen at the moment it lost the master role — and stays
    // eligible for a later promotion, which is how that stale dataset becomes
    // the cluster's. Inert on a master, which never reads it.
    lines.push(format!("masterauth {}", quote_conf_value(&config.redis_password)));

    if let Some((host, port)) = replicate_from(config, boot_master) {
        lines.push(format!("replicaof {} {}", host, port));
    }

    lines.join("\n") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn config_at(data_dir: &str) -> Config {
        let mut config = Config::for_tests();
        config.data_dir = data_dir.to_string();
        config
    }

    fn write_rdb(dir: &std::path::Path) {
        fs::write(dir.join("dump.rdb"), b"REDIS0011fake").unwrap();
    }

    fn write_manifestless_aof_dir(dir: &std::path::Path) {
        let aof = dir.join("appendonlydir");
        fs::create_dir_all(&aof).unwrap();
        fs::write(aof.join("appendonly.aof.1.incr.aof"), b"orphan").unwrap();
    }

    fn write_aof_manifest(dir: &std::path::Path) {
        let aof = dir.join("appendonlydir");
        fs::create_dir_all(&aof).unwrap();
        fs::write(
            aof.join("appendonly.aof.manifest"),
            b"file appendonly.aof.1.base.rdb seq 1 type b\n",
        )
        .unwrap();
    }

    // --- needs_rdb_to_aof_migration: every branch ---

    #[test]
    fn fresh_volume_needs_no_migration() {
        let dir = tempdir().unwrap();
        assert!(!needs_rdb_to_aof_migration(dir.path().to_str().unwrap()));
    }

    #[test]
    fn rdb_only_needs_migration() {
        // The adoption case: a standalone Railway redis persists via RDB only.
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        assert!(needs_rdb_to_aof_migration(dir.path().to_str().unwrap()));
    }

    #[test]
    fn rdb_with_manifestless_aof_dir_still_needs_migration() {
        // The crash window: a previous attempt died between CONFIG SET and the
        // manifest commit. Keying on the directory instead of the manifest
        // would skip the migration here and strand the dump.rdb again.
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        write_manifestless_aof_dir(dir.path());
        assert!(needs_rdb_to_aof_migration(dir.path().to_str().unwrap()));
    }

    #[test]
    fn rdb_with_committed_aof_needs_no_migration() {
        // Post-migration boots: the manifest exists, the AOF is the source.
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        write_aof_manifest(dir.path());
        assert!(!needs_rdb_to_aof_migration(dir.path().to_str().unwrap()));
    }

    #[test]
    fn aof_without_rdb_needs_no_migration() {
        // A bitnami-lineage volume that always ran AOF, or any AOF-only node.
        let dir = tempdir().unwrap();
        write_aof_manifest(dir.path());
        assert!(!needs_rdb_to_aof_migration(dir.path().to_str().unwrap()));
    }

    // --- quarantine_manifestless_aof_dir: every branch ---

    #[test]
    fn quarantine_does_nothing_without_an_aof_dir() {
        let dir = tempdir().unwrap();
        let moved = quarantine_manifestless_aof_dir(dir.path().to_str().unwrap()).unwrap();
        assert!(moved.is_none());
    }

    #[test]
    fn quarantine_leaves_a_committed_aof_alone() {
        let dir = tempdir().unwrap();
        write_aof_manifest(dir.path());
        let moved = quarantine_manifestless_aof_dir(dir.path().to_str().unwrap()).unwrap();
        assert!(moved.is_none());
        assert!(dir
            .path()
            .join("appendonlydir/appendonly.aof.manifest")
            .exists());
    }

    #[test]
    fn quarantine_moves_a_manifestless_dir_aside_preserving_contents() {
        let dir = tempdir().unwrap();
        write_manifestless_aof_dir(dir.path());
        let moved = quarantine_manifestless_aof_dir(dir.path().to_str().unwrap())
            .unwrap()
            .expect("should have quarantined");
        // Renamed, not deleted: the orphan incr file is the only trace of
        // writes accepted in the crashed window.
        assert!(!dir.path().join("appendonlydir").exists());
        assert!(moved.join("appendonly.aof.1.incr.aof").exists());
        assert_eq!(
            fs::read(moved.join("appendonly.aof.1.incr.aof")).unwrap(),
            b"orphan"
        );
    }

    // --- generate_redis_conf: every branch ---

    #[test]
    fn fresh_volume_boots_with_aof_on() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(conf.contains("appendonly yes"));
        assert!(!conf.contains("appendonly no"));
    }

    #[test]
    fn rdb_adoption_boots_with_aof_off() {
        // AOF at boot would make Redis load an empty AOF and ignore the RDB —
        // this one boot loads the RDB; AOF is enabled at runtime after.
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(conf.contains("appendonly no"));
    }

    #[test]
    fn crash_remnants_still_boot_with_aof_off() {
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        write_manifestless_aof_dir(dir.path());
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(conf.contains("appendonly no"));
    }

    #[test]
    fn conf_points_redis_at_the_data_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let conf = generate_redis_conf(&config_at(path), &BootMaster::NoLocalState);
        assert!(conf.contains(&format!("dir {}", path)));
    }

    #[test]
    fn primary_has_no_replicaof() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(!conf.contains("replicaof"));
    }

    // Sentinel demotes an old master in place, and the credential has to
    // already be in its config when that happens — there is no second chance:
    // the node reports role:slave, never syncs, and serves the dataset it had
    // when it lost the master role.
    #[test]
    fn every_node_carries_masterauth_including_the_deployed_primary() {
        let dir = tempdir().unwrap();
        let config = config_at(dir.path().to_str().unwrap());
        assert!(config.is_primary());
        assert!(generate_redis_conf(&config, &BootMaster::NoLocalState).contains(r#"masterauth "pw""#));
        assert!(generate_redis_conf(&config, &BootMaster::SelfIsMaster).contains(r#"masterauth "pw""#));
    }

    #[test]
    fn replica_gets_replicaof_and_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:7000".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("replicaof master-host 7000"));
        assert!(conf.contains(r#"masterauth "pw""#));
    }

    #[test]
    fn malformed_replica_of_skips_replicaof_but_keeps_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!conf.contains("replicaof"));
        assert!(conf.contains(r#"masterauth "pw""#));
    }

    #[test]
    fn ha_boot_keeps_the_split_brain_fence() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(conf.contains("min-replicas-to-write 1"));
        assert!(conf.contains("min-replicas-max-lag 5"));
    }

    // The lag bound has to stay inside SENTINEL_DOWN_AFTER_MS or the fence
    // stops meaning anything; it comes from the env, not a hardcoded literal.
    #[test]
    fn min_replicas_max_lag_is_configurable_via_env() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.min_replicas_max_lag_secs = 3;
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("min-replicas-max-lag 3"));
    }

    // The fence follows the stamped quorum (majority − 1): a 5-node boot
    // (SENTINEL_QUORUM=3) must require 2 acking replicas, or a partition
    // that traps one replica with the old master leaves two writers.
    #[test]
    fn fence_scales_with_the_stamped_quorum() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.sentinel_quorum = 3;
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("min-replicas-to-write 2"));
    }

    #[test]
    fn min_replicas_is_majority_minus_one_floored_at_one() {
        assert_eq!(min_replicas_to_write(2), 1); // 3-node cluster
        assert_eq!(min_replicas_to_write(3), 2); // 5-node cluster
        assert_eq!(min_replicas_to_write(4), 3); // 7-node cluster
        // Degenerate stamps never disable the fence outright.
        assert_eq!(min_replicas_to_write(1), 1);
        assert_eq!(min_replicas_to_write(0), 1);
    }

    // Regression: a standalone boot (SENTINEL_ENABLED unset — the state a
    // root is left in after its HA template is reverted, still on this
    // image) has no replicas, so the fence would reject every write with
    // NOREPLICAS forever.
    #[test]
    fn standalone_boot_has_no_split_brain_fence() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.sentinel_enabled = false;
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!conf.contains("min-replicas-to-write"));
        assert!(!conf.contains("min-replicas-max-lag"));
    }

    // --- the resolved boot role overriding REPLICA_OF ---

    // The data-loss case: this node was deployed as a replica of master-host,
    // Sentinel promoted it, and it restarted. Regenerating `replicaof
    // master-host` would demote it back onto the node it was promoted over and
    // full-sync every write since the promotion away.
    #[test]
    fn a_promoted_node_restarting_does_not_demote_itself() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:6379".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::SelfIsMaster);
        assert!(!conf.contains("replicaof"));
    }

    // The dual-master case: deployed as the initial master (REPLICA_OF empty),
    // restarted after a failover elected someone else.
    #[test]
    fn a_deployed_master_follows_sentinel_state_into_replica_role() {
        let dir = tempdir().unwrap();
        let config = config_at(dir.path().to_str().unwrap());
        assert!(config.is_primary());
        let conf = generate_redis_conf(
            &config,
            &BootMaster::ReplicaOf("redis-2.railway.internal".to_string(), 6379),
        );
        assert!(conf.contains("replicaof redis-2.railway.internal 6379"));
        assert!(conf.contains(r#"masterauth "pw""#));
    }

    #[test]
    fn a_replica_follows_sentinel_state_to_the_new_master() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "redis-1:6379".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::ReplicaOf("redis-3".to_string(), 6380));
        assert!(conf.contains("replicaof redis-3 6380"));
        assert!(!conf.contains("replicaof redis-1"));
    }

    // NoLocalState is the fallback path (first boot, unreadable state, kill
    // switch) and must leave the env-derived topology exactly as it was.
    #[test]
    fn no_local_state_keeps_the_env_topology() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:6379".to_string();
        let as_replica = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(as_replica.contains("replicaof master-host 6379"));
        assert!(as_replica.contains(r#"masterauth "pw""#));

        config.replica_of = String::new();
        let as_primary = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!as_primary.contains("replicaof"));
    }

    // An unparseable port used to be emitted verbatim (`replicaof host abc`),
    // which redis refuses to parse — the node never starts at all. Dropping
    // the directive at least boots it as a master Sentinel can then reconfigure.
    #[test]
    fn replica_of_with_an_unparseable_port_skips_replicaof() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:abc".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!conf.contains("replicaof"));
        assert!(conf.contains(r#"masterauth "pw""#));
    }

    // --- replication sizing: full-resync and output-buffer protection ---

    #[test]
    fn every_boot_stamps_replication_sizing_defaults() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(
            &config_at(dir.path().to_str().unwrap()),
            &BootMaster::NoLocalState,
        );
        assert!(conf.contains("repl-backlog-size 64mb"));
        assert!(conf.contains("client-output-buffer-limit replica 512mb 128mb 120"));
    }

    // Inert without replicas, but a standalone boot (reverted-HA root) can
    // gain replicas again later on the same image — must not be gated on
    // sentinel_enabled.
    #[test]
    fn standalone_boot_still_stamps_replication_sizing() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.sentinel_enabled = false;
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("repl-backlog-size 64mb"));
        assert!(conf.contains("client-output-buffer-limit replica 512mb 128mb 120"));
    }

    #[test]
    fn replication_sizing_is_configurable_via_env() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.repl_backlog_size = "256mb".to_string();
        config.client_output_buffer_limit_replica = "1gb 256mb 180".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("repl-backlog-size 256mb"));
        assert!(conf.contains("client-output-buffer-limit replica 1gb 256mb 180"));
    }

    // --- maxmemory: absent by default, stamped (with noeviction) when set ---

    #[test]
    fn no_maxmemory_directive_when_none_was_detected() {
        let dir = tempdir().unwrap();
        let config = config_at(dir.path().to_str().unwrap());
        assert_eq!(config.maxmemory_bytes, None); // for_tests() default
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!conf.contains("maxmemory"));
    }

    #[test]
    fn maxmemory_and_noeviction_are_stamped_together_when_set() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.maxmemory_bytes = Some(1_610_612_736); // 1.5 GiB
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("maxmemory 1610612736"));
        assert!(conf.contains("maxmemory-policy noeviction"));
    }

    #[test]
    fn maxmemory_is_stamped_on_a_standalone_boot_too() {
        // Same reasoning as replication sizing: a standalone (reverted-HA)
        // root keeps this image and the same OOM exposure.
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.sentinel_enabled = false;
        config.maxmemory_bytes = Some(536_870_912);
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("maxmemory 536870912"));
        assert!(conf.contains("maxmemory-policy noeviction"));
    }

    // --- quote_conf_value: every branch ---

    #[test]
    fn a_plain_value_is_just_quoted() {
        assert_eq!(quote_conf_value("hunter2"), r#""hunter2""#);
    }

    #[test]
    fn a_backslash_is_escaped() {
        assert_eq!(quote_conf_value(r"pass\word"), r#""pass\\word""#);
    }

    #[test]
    fn a_double_quote_is_escaped() {
        assert_eq!(quote_conf_value(r#"pass"word"#), r#""pass\"word""#);
    }

    #[test]
    fn a_space_and_hash_survive_inside_the_quotes_untouched() {
        // Neither needs escaping once the whole value is quoted — a bare
        // space would otherwise split the token, and a bare "#" would
        // otherwise start a comment.
        assert_eq!(quote_conf_value("pass word#tail"), r#""pass word#tail""#);
    }

    #[test]
    fn an_empty_value_becomes_an_empty_quoted_string() {
        assert_eq!(quote_conf_value(""), r#""""#);
    }

    #[test]
    fn adjacent_backslash_and_quote_each_get_their_own_escape() {
        // The case a naive "escape quotes, then escape backslashes" two-pass
        // implementation gets wrong: escaping backslashes AFTER quotes would
        // re-escape the backslash quote-escaping just added.
        assert_eq!(quote_conf_value(r#"a\"b"#), r#""a\\\"b""#);
    }

    // --- requirepass / masterauth: quoted end to end, via generate_redis_conf ---

    #[test]
    fn requirepass_and_masterauth_are_quoted() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.redis_password = "hunter2".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.lines().any(|l| l == r#"requirepass "hunter2""#));
        assert!(conf.lines().any(|l| l == r#"masterauth "hunter2""#));
    }

    #[test]
    fn a_password_with_a_hash_and_a_space_does_not_truncate_or_split_the_directive() {
        // Unquoted, this password would comment out everything after "#" on
        // the requirepass line (silently booting with NO password) and split
        // the masterauth line into two malformed tokens. Conversion adopts
        // whatever REDIS_PASSWORD the standalone service already had, so
        // this is a real customer-controlled input.
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.redis_password = "p# w\"ord".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.lines().any(|l| l == r#"requirepass "p# w\"ord""#));
        assert!(conf.lines().any(|l| l == r#"masterauth "p# w\"ord""#));
        // Every directive after the password lines is still on its own,
        // intact line — nothing got swallowed into a comment or merged.
        assert!(conf.lines().any(|l| l == "protected-mode yes"));
        assert!(conf.lines().any(|l| l.starts_with("dir ")));
    }

    #[test]
    fn persisted_requirepass_round_trips_a_generated_conf() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        let mut config = config_at(data_dir);
        config.redis_password = "p# w\"or\\d".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        std::fs::write(format!("{}/redis.conf", data_dir), conf).unwrap();
        assert_eq!(
            persisted_requirepass(data_dir),
            Some("p# w\"or\\d".to_string())
        );
    }

    #[test]
    fn persisted_requirepass_is_none_on_a_fresh_volume() {
        let dir = tempdir().unwrap();
        assert_eq!(persisted_requirepass(dir.path().to_str().unwrap()), None);
    }

    #[test]
    fn persisted_requirepass_is_none_when_the_line_is_absent_or_empty() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        std::fs::write(format!("{}/redis.conf", data_dir), "port 6379\n").unwrap();
        assert_eq!(persisted_requirepass(data_dir), None);
        std::fs::write(
            format!("{}/redis.conf", data_dir),
            "port 6379\nrequirepass \"\"\n",
        )
        .unwrap();
        assert_eq!(persisted_requirepass(data_dir), None);
        // A bare keyword with no value must not abort the scan or panic.
        std::fs::write(format!("{}/redis.conf", data_dir), "requirepass\n").unwrap();
        assert_eq!(persisted_requirepass(data_dir), None);
    }

    #[test]
    fn persisted_requirepass_accepts_an_unquoted_hand_edited_value() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        std::fs::write(
            format!("{}/redis.conf", data_dir),
            "REQUIREPASS hunter2\n",
        )
        .unwrap();
        assert_eq!(persisted_requirepass(data_dir), Some("hunter2".to_string()));
    }
}
