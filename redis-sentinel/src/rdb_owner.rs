//! RDB ownership: which `dump.rdb` this image's own redis-server left on the
//! volume.
//!
//! ## The gap a timestamp cannot close
//! `redis_conf::needs_rdb_to_aof_migration` has to tell two volumes apart
//! that look alike: a `dump.rdb` the customer kept persisting under an image
//! with AOF off, beside an `appendonlydir` some earlier image abandoned
//! (adopt the RDB), and a `dump.rdb` this image's own Redis saved at
//! shutdown beside the AOF it was running (trust the AOF). Its fallback is
//! freshness — the RDB has to be newer than every AOF file by a margin — and
//! that reads the second shape wrong whenever the node was quiet for longer
//! than the margin before it stopped. Redis fsyncs the AOF and only then
//! writes the shutdown RDB, but fsync changes no mtime and a node with no
//! buffered writes appends nothing, so every `appendonlydir` file keeps the
//! mtime of the last write while `dump.rdb` gets the shutdown's. The gap
//! equals the idle time. Measured on redis 7.4.7 with this image's conf: six
//! quiet seconds before SIGTERM left an eight-second gap and untouched AOF
//! mtimes. Ten quiet minutes before a redeploy would have re-adopted the RDB
//! — a full AOF rewrite on boot and a full `appendonlydir.superseded-*` copy
//! left on the volume, on every quiet restart, for nothing.
//!
//! ## What settles it
//! Whether this image's redis-server is what wrote the RDB. While it runs,
//! and when it exits, every `dump.rdb` on the volume is its own (a BGSAVE,
//! a replica's full-sync transfer, the shutdown save) and is a snapshot of a
//! dataset the AOF already holds in full. So the wrapper records the identity
//! of that file — mtime to the nanosecond and length, in [`MARKER`] — at
//! boot once the adoption decision is made, on a slow poll while Redis runs,
//! and after redis-server exits under the supervisor however it exited. A
//! boot that finds `dump.rdb` matching the marker knows the AOF beside it is
//! the source, whatever the mtimes say. A boot that finds no marker, or one
//! naming a different file, has an RDB written by something else — another
//! image after a revert, a restore — and falls back to the freshness rule.
//!
//! The marker can only fail towards the fallback: unreadable, stale or
//! missing, the decision is exactly what it would be without it.

use crate::atomic_write::write_atomic;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};
use tracing::warn;

/// Identity of the `dump.rdb` this image's redis-server last left on the
/// volume, in the data dir beside it. See the module doc.
pub const MARKER: &str = ".rdb_owner";

/// Slow: a BGSAVE is minutes apart at the tightest default save point, and
/// the exit path records the final state synchronously. The poll only
/// covers a supervisor that dies without running that path.
const POLL: Duration = Duration::from_secs(5);

/// What identifies one write of `dump.rdb`: Redis writes a temp file and
/// renames it into place, so every save is a new inode with a new mtime.
/// Length is included so a same-second rewrite on a coarse-timestamp
/// filesystem still reads as a different file when its size moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdbIdentity {
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
    pub len: u64,
}

impl RdbIdentity {
    /// The `dump.rdb` currently in `data_dir`, or `None` when there is none
    /// or it cannot be read.
    pub fn of(data_dir: &str) -> Option<Self> {
        let meta = fs::metadata(Path::new(data_dir).join("dump.rdb")).ok()?;
        let mtime = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(Self {
            mtime_secs: mtime.as_secs(),
            mtime_nanos: mtime.subsec_nanos(),
            len: meta.len(),
        })
    }

    fn render(&self) -> String {
        format!(
            "dump.rdb mtime={}.{:09} len={}\n",
            self.mtime_secs, self.mtime_nanos, self.len
        )
    }

    fn parse(text: &str) -> Option<Self> {
        let mut mtime = None;
        let mut len = None;
        for field in text.split_whitespace() {
            if let Some(value) = field.strip_prefix("mtime=") {
                let (secs, nanos) = value.split_once('.')?;
                mtime = Some((secs.parse::<u64>().ok()?, nanos.parse::<u32>().ok()?));
            } else if let Some(value) = field.strip_prefix("len=") {
                len = Some(value.parse::<u64>().ok()?);
            }
        }
        let ((mtime_secs, mtime_nanos), len) = (mtime?, len?);
        Some(Self {
            mtime_secs,
            mtime_nanos,
            len,
        })
    }
}

pub fn marker_path(data_dir: &str) -> PathBuf {
    Path::new(data_dir).join(MARKER)
}

/// The identity an earlier run recorded, or `None` when there is no marker
/// or it does not parse.
pub fn recorded(data_dir: &str) -> Option<RdbIdentity> {
    RdbIdentity::parse(&fs::read_to_string(marker_path(data_dir)).ok()?)
}

/// Whether the `dump.rdb` on the volume is the one this image's own
/// redis-server left there. False on any doubt: no RDB, no marker, a marker
/// naming a different file, anything unreadable.
pub fn owns_rdb(data_dir: &str) -> bool {
    match (recorded(data_dir), RdbIdentity::of(data_dir)) {
        (Some(recorded), Some(current)) => recorded == current,
        _ => false,
    }
}

/// The poll and an exit path can call [`record`] at the same moment;
/// `write_atomic` stages through one temp name, so two writers would trip
/// over each other's temp file. One at a time.
static RECORD: Mutex<()> = Mutex::new(());

/// Record the `dump.rdb` currently on the volume as this image's own.
/// Writes only when the identity changed; removes the marker when there is
/// no RDB to own, so a stale claim never outlives its file. `Ok(true)` when
/// something was written or removed.
pub fn record(data_dir: &str) -> io::Result<bool> {
    let _one_at_a_time = RECORD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let marker = marker_path(data_dir);
    match RdbIdentity::of(data_dir) {
        None => match fs::remove_file(&marker) {
            Ok(()) => Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        },
        Some(current) => {
            if recorded(data_dir) == Some(current) {
                return Ok(false);
            }
            write_atomic(&marker, &current.render(), None)?;
            Ok(true)
        }
    }
}

/// Keep the marker current while redis-server runs, for the case where the
/// supervisor itself is killed and never reaches its exit-path record. A
/// failing volume is reported once, not every poll.
pub fn spawn(data_dir: String) {
    tokio::spawn(async move {
        let mut failing = false;
        loop {
            tokio::time::sleep(POLL).await;
            match record(&data_dir) {
                Ok(_) => failing = false,
                Err(err) => {
                    if !failing {
                        warn!(
                            error = %err,
                            "rdb owner: could not record the RDB redis-server is writing — \
                             a boot after an unclean stop falls back to comparing timestamps"
                        );
                    }
                    failing = true;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_rdb(dir: &Path, contents: &[u8]) {
        fs::write(dir.join("dump.rdb"), contents).unwrap();
    }

    #[test]
    fn identity_round_trips_through_the_marker_text() {
        let id = RdbIdentity {
            mtime_secs: 1_789_567_983,
            mtime_nanos: 7,
            len: 42,
        };
        assert_eq!(RdbIdentity::parse(&id.render()), Some(id));
        assert_eq!(id.render(), "dump.rdb mtime=1789567983.000000007 len=42\n");
    }

    #[test]
    fn a_marker_that_does_not_parse_owns_nothing() {
        assert_eq!(RdbIdentity::parse(""), None);
        assert_eq!(RdbIdentity::parse("mtime=1.2"), None);
        assert_eq!(RdbIdentity::parse("len=3"), None);
        assert_eq!(RdbIdentity::parse("mtime=abc.0 len=3"), None);
        assert_eq!(RdbIdentity::parse("mtime=1 len=3"), None);
    }

    #[test]
    fn nothing_is_owned_without_a_marker_or_without_an_rdb() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        assert!(!owns_rdb(data_dir));
        write_rdb(dir.path(), b"REDIS0011");
        assert!(!owns_rdb(data_dir));
        assert!(record(data_dir).unwrap());
        fs::remove_file(dir.path().join("dump.rdb")).unwrap();
        assert!(!owns_rdb(data_dir));
    }

    #[test]
    fn a_recorded_rdb_is_owned_until_it_is_rewritten() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        write_rdb(dir.path(), b"REDIS0011");
        assert!(record(data_dir).unwrap());
        assert!(owns_rdb(data_dir));
        // Same file, nothing to write.
        assert!(!record(data_dir).unwrap());
        // Something else saved an RDB: new bytes, new identity.
        write_rdb(dir.path(), b"REDIS0011 and then some");
        assert!(!owns_rdb(data_dir));
        // Recording again reclaims it.
        assert!(record(data_dir).unwrap());
        assert!(owns_rdb(data_dir));
    }

    #[test]
    fn a_same_length_rewrite_is_a_different_file_by_mtime() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        write_rdb(dir.path(), b"REDIS0011");
        record(data_dir).unwrap();
        // Another image saved the same bytes later: only the mtime moves.
        let later = std::time::SystemTime::now() + Duration::from_secs(60);
        fs::File::options()
            .write(true)
            .open(dir.path().join("dump.rdb"))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(later))
            .unwrap();
        assert!(!owns_rdb(data_dir));
    }

    #[test]
    fn recording_with_no_rdb_removes_a_stale_marker() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        write_rdb(dir.path(), b"REDIS0011");
        record(data_dir).unwrap();
        fs::remove_file(dir.path().join("dump.rdb")).unwrap();
        assert!(record(data_dir).unwrap());
        assert!(!marker_path(data_dir).exists());
        assert!(!record(data_dir).unwrap());
    }

    #[test]
    fn a_garbage_marker_owns_nothing_and_is_replaced_on_record() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().to_str().unwrap();
        write_rdb(dir.path(), b"REDIS0011");
        fs::write(marker_path(data_dir), "not a marker\n").unwrap();
        assert!(!owns_rdb(data_dir));
        assert!(record(data_dir).unwrap());
        assert!(owns_rdb(data_dir));
    }
}
