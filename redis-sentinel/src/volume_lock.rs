//! Volume-lifetime runtime lock: at most one Redis container touches this
//! volume at a time.
//!
//! A redeploy (or an instance restart during host maintenance) can leave the
//! old and new containers briefly overlapping on the shared volume. Redis has
//! no on-disk interlock of its own — two redis-servers appending to the same
//! AOF / rewriting the same RDB is silent corruption, not a refused boot. The
//! wrapper therefore holds an exclusive `flock` on a file at the data
//! directory root for the whole life of the supervisor process: a booting
//! container waits for the previous holder's supervisor to exit before it
//! touches the data directory, and fail-stops loudly on timeout so the
//! restart policy retries the boot instead of risking two engines on one
//! dataset.
//!
//! The kernel releases a `flock` when the last open description on the file
//! is closed, however the holder ended (graceful stop, SIGKILL, OOM) — so
//! "lock free" really means the previous supervisor is gone. The supervisor
//! outlives its children by construction (`process_manager::supervise` reaps
//! both engines before exiting), which is what makes its lifetime a faithful
//! proxy for "this container is done with the volume".

use anyhow::{bail, Result};
use nix::fcntl::{Flock, FlockArg};
use std::fs::OpenOptions;
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const RUNTIME_LOCK_FILE: &str = ".railway-redis-runtime.lock";
const DEFAULT_WAIT_SECS: u64 = 300;

/// How the lock attempt ended, so the wrapper can keep the documented
/// fail-open boot while still surfacing the lost invariant in telemetry —
/// without this, a boot without the volume lock differs from a normal boot
/// by one warn line nobody alerts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeLockOutcome {
    /// The exclusive flock is held for this process's lifetime.
    Held,
    /// The lock file could not be opened; the boot continued without the
    /// lock. The overlap protection is gone for this container.
    FailedOpen,
}

/// Acquire the exclusive volume runtime lock, waiting up to
/// `RUNTIME_LOCK_WAIT_SECONDS` (default 300) for a previous holder.
///
/// The lock handle is deliberately leaked on success: the file description
/// stays open until this process exits, which is exactly the intended lock
/// lifetime. An unopenable lock file fails OPEN (warn + continue) — the lock
/// is defense in depth, and refusing to boot over a bad lock file would be a
/// worse failure than the overlap it guards against; the outcome is returned
/// so the caller can report it. A timeout waiting on a live holder fails
/// CLOSED.
pub fn acquire_volume_runtime_lock(data_dir: &str) -> Result<VolumeLockOutcome> {
    let wait_secs = std::env::var("RUNTIME_LOCK_WAIT_SECONDS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_WAIT_SECS);
    acquire_with_wait(data_dir, wait_secs)
}

fn acquire_with_wait(data_dir: &str, wait_secs: u64) -> Result<VolumeLockOutcome> {
    let path = Path::new(data_dir).join(RUNTIME_LOCK_FILE);
    let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(f) => f,
        Err(err) => {
            warn!(
                path = %path.display(),
                error = %err,
                "could not open the runtime lock file; continuing without the volume lock"
            );
            return Ok(VolumeLockOutcome::FailedOpen);
        }
    };

    let deadline = Instant::now() + Duration::from_secs(wait_secs);
    let mut waited = false;
    loop {
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => {
                if waited {
                    info!("previous container released the volume; continuing boot");
                }
                std::mem::forget(lock);
                return Ok(VolumeLockOutcome::Held);
            }
            Err((returned, _errno)) => {
                if !waited {
                    warn!(
                        wait_secs,
                        "another container still holds this volume (overlapping deploy); \
                         waiting for it to shut down"
                    );
                    waited = true;
                }
                if Instant::now() >= deadline {
                    bail!(
                        "previous container did not release the volume within {wait_secs}s; \
                         refusing to start redis against a volume another instance may still be using"
                    );
                }
                file = returned;
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::fcntl::{Flock, FlockArg};
    use std::fs::OpenOptions;

    #[test]
    fn acquires_when_uncontended_and_blocks_reacquisition() {
        let dir = std::env::temp_dir().join(format!("rt-lock-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();

        acquire_with_wait(dir_str, 1).expect("uncontended acquire should succeed");

        // The leaked handle above still holds the lock: an independent open
        // description on the same file must not be able to take it.
        let probe = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(RUNTIME_LOCK_FILE))
            .unwrap();
        assert!(
            Flock::lock(probe, FlockArg::LockExclusiveNonblock).is_err(),
            "lock should still be held by the leaked handle"
        );
    }

    #[test]
    fn times_out_when_another_holder_keeps_the_lock() {
        let dir = std::env::temp_dir().join(format!("rt-lock-timeout-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir_str = dir.to_str().unwrap();

        let holder = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(RUNTIME_LOCK_FILE))
            .unwrap();
        let held = Flock::lock(holder, FlockArg::LockExclusiveNonblock)
            .expect("holder should acquire first");

        let start = Instant::now();
        let result = acquire_with_wait(dir_str, 2);
        assert!(result.is_err(), "contended acquire should time out");
        assert!(start.elapsed() >= Duration::from_secs(2));

        drop(held);
        acquire_with_wait(dir_str, 1).expect("acquire after release should succeed");
    }

    #[test]
    fn missing_directory_fails_open() {
        let dir = std::env::temp_dir().join(format!("rt-lock-missing-{}", std::process::id()));
        // Deliberately never created: open fails, and the guard must degrade
        // to a warning instead of failing the boot.
        acquire_with_wait(dir.to_str().unwrap(), 1)
            .expect("unopenable lock file must fail open, not fail the boot");
    }
}
