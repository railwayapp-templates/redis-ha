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
    ];

    if config.sentinel_enabled {
        // Split-brain fence: master stops accepting writes when it loses contact
        // with all replicas for longer than min-replicas-max-lag seconds.
        // Bounds the split-brain window on network partition to this lag rather
        // than letting the isolated master accept writes indefinitely.
        // 1 replica required — self-fences only when fully isolated.
        //
        // HA boots only: a standalone boot (SENTINEL_ENABLED unset — e.g. a
        // root whose HA template was reverted, which keeps this image) has no
        // replicas by definition, so the fence would permanently reject every
        // write with NOREPLICAS.
        lines.push("min-replicas-to-write 1".to_string());
        // Must be <= SENTINEL_DOWN_AFTER_MS (5s default) so the master goes
        // read-only around the same time Sentinel declares it ODOWN elsewhere.
        lines.push("min-replicas-max-lag 10".to_string());
    }

    // Every node carries the master password, not just the ones deployed as
    // replicas: Sentinel demotes an old master into a replica in place, and a
    // node that has no masterauth when that happens can never complete the
    // handshake ("MASTER aborted replication with an error: NOAUTH
    // Authentication required"). It keeps reporting role:slave while serving a
    // dataset frozen at the moment it lost the master role — and stays
    // eligible for a later promotion, which is how that stale dataset becomes
    // the cluster's. Inert on a master, which never reads it.
    lines.push(format!("masterauth {}", config.redis_password));

    if !config.is_primary() {
        // Parse REPLICA_OF as "host:port"
        let parts: Vec<&str> = config.replica_of.splitn(2, ':').collect();
        if parts.len() == 2 {
            lines.push(format!("replicaof {} {}", parts[0], parts[1]));
        }
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
        let conf = generate_redis_conf(&config_at(dir.path().to_str().unwrap()));
        assert!(conf.contains("appendonly yes"));
        assert!(!conf.contains("appendonly no"));
    }

    #[test]
    fn rdb_adoption_boots_with_aof_off() {
        // AOF at boot would make Redis load an empty AOF and ignore the RDB —
        // this one boot loads the RDB; AOF is enabled at runtime after.
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        let conf = generate_redis_conf(&config_at(dir.path().to_str().unwrap()));
        assert!(conf.contains("appendonly no"));
    }

    #[test]
    fn crash_remnants_still_boot_with_aof_off() {
        let dir = tempdir().unwrap();
        write_rdb(dir.path());
        write_manifestless_aof_dir(dir.path());
        let conf = generate_redis_conf(&config_at(dir.path().to_str().unwrap()));
        assert!(conf.contains("appendonly no"));
    }

    #[test]
    fn conf_points_redis_at_the_data_dir() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let conf = generate_redis_conf(&config_at(path));
        assert!(conf.contains(&format!("dir {}", path)));
    }

    #[test]
    fn primary_has_no_replicaof() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(&config_at(dir.path().to_str().unwrap()));
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
        assert!(generate_redis_conf(&config).contains("masterauth pw"));
    }

    #[test]
    fn replica_gets_replicaof_and_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:7000".to_string();
        let conf = generate_redis_conf(&config);
        assert!(conf.contains("replicaof master-host 7000"));
        assert!(conf.contains("masterauth pw"));
    }

    #[test]
    fn malformed_replica_of_skips_replicaof_but_keeps_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host".to_string();
        let conf = generate_redis_conf(&config);
        assert!(!conf.contains("replicaof"));
        assert!(conf.contains("masterauth pw"));
    }

    #[test]
    fn ha_boot_keeps_the_split_brain_fence() {
        let dir = tempdir().unwrap();
        let conf = generate_redis_conf(&config_at(dir.path().to_str().unwrap()));
        assert!(conf.contains("min-replicas-to-write 1"));
        assert!(conf.contains("min-replicas-max-lag 10"));
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
        let conf = generate_redis_conf(&config);
        assert!(!conf.contains("min-replicas-to-write"));
        assert!(!conf.contains("min-replicas-max-lag"));
    }
}
