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

pub fn generate_redis_conf(config: &Config, boot_master: &BootMaster) -> String {
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
    lines.push(format!("masterauth {}", config.redis_password));

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
        assert!(generate_redis_conf(&config, &BootMaster::NoLocalState).contains("masterauth pw"));
        assert!(generate_redis_conf(&config, &BootMaster::SelfIsMaster).contains("masterauth pw"));
    }

    #[test]
    fn replica_gets_replicaof_and_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host:7000".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(conf.contains("replicaof master-host 7000"));
        assert!(conf.contains("masterauth pw"));
    }

    #[test]
    fn malformed_replica_of_skips_replicaof_but_keeps_masterauth() {
        let dir = tempdir().unwrap();
        let mut config = config_at(dir.path().to_str().unwrap());
        config.replica_of = "master-host".to_string();
        let conf = generate_redis_conf(&config, &BootMaster::NoLocalState);
        assert!(!conf.contains("replicaof"));
        assert!(conf.contains("masterauth pw"));
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
        assert!(conf.contains("masterauth pw"));
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
        assert!(as_replica.contains("masterauth pw"));

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
        assert!(conf.contains("masterauth pw"));
    }
}
