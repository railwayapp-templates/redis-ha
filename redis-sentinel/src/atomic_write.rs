//! Crash-safe config writes: tmp-in-same-dir + fsync + rename.
//!
//! A plain `fs::write` can tear: a crash (or the OOM killer, or a node
//! reboot) between the open and the final flush leaves a truncated file at
//! the real path, and nothing distinguishes it from a complete one on the
//! next boot. For redis.conf that poisons `persisted_requirepass` — the next
//! boot pins the cluster password to whatever prefix survived. For
//! sentinel.conf it is worse: the file is written on FIRST BOOT ONLY and
//! preserved forever after, so a torn write is permanent — the boot-role
//! resolver finds no usable monitor line, the empty-master boot guard's
//! conf-path arm silently never fires, and auth resolution reads a file
//! that no longer says what this node decided.
//!
//! The classic fix, in the order that makes each step meaningful:
//!  1. write the full contents to a temp file IN THE SAME DIRECTORY (rename
//!     is only atomic within one filesystem);
//!  2. apply the final permissions to the temp file — a credential-bearing
//!     conf must never be readable more broadly than intended at its final
//!     path, not even for the instant before a chmod;
//!  3. `sync_all` the temp file so its bytes are durable before any name
//!     points at them;
//!  4. `rename` over the target — atomic replace: every future open sees
//!     either the complete old file or the complete new one, never a mix;
//!  5. fsync the parent directory so the rename itself survives a crash.

use std::fs::{self, File, OpenOptions};
use std::io::{Error, ErrorKind, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// Write `contents` to `path` atomically and durably. `mode` is the
/// permission bits the file must have at its final path (applied to the
/// temp file BEFORE the rename); `None` keeps the process default, exactly
/// like the `fs::write` this replaces.
pub fn write_atomic(path: &Path, contents: &str, mode: Option<u32>) -> std::io::Result<()> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let Some(file_name) = path.file_name() else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "atomic write target has no file name",
        ));
    };
    let tmp = dir.join(format!(".{}.tmp", file_name.to_string_lossy()));

    // A crash between a previous boot's write and rename leaves the temp
    // file behind, possibly with stale permissions `create_new` would keep;
    // recreate it rather than reuse it.
    match fs::remove_file(&tmp) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    let result: std::io::Result<()> = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        if let Some(mode) = mode {
            // At creation, so there is no window where the temp file is
            // more readable than the final file may be. The umask can only
            // clear bits, so the explicit set below makes the mode exact.
            options.mode(mode);
        }
        let mut file = options.open(&tmp)?;
        if let Some(mode) = mode {
            file.set_permissions(fs::Permissions::from_mode(mode))?;
        }
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)?;
        File::open(dir)?.sync_all()?;
        Ok(())
    })();

    if result.is_err() {
        // Best effort: never leave a half-written temp file lying around on
        // a failure path.
        let _ = fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn writes_the_contents_to_the_target_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("redis.conf");
        write_atomic(&path, "port 6379\n", None).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "port 6379\n");
    }

    #[test]
    fn replaces_an_existing_file_wholesale() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("redis.conf");
        fs::write(&path, "old contents, much longer than the new ones\n").unwrap();
        write_atomic(&path, "port 6379\n", None).unwrap();
        // A non-atomic in-place write of a shorter payload would leave the
        // old tail behind; the rename replaces the whole file.
        assert_eq!(fs::read_to_string(&path).unwrap(), "port 6379\n");
    }

    #[test]
    fn applies_the_requested_mode() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sentinel.conf");
        write_atomic(&path, "requirepass hunter2\n", Some(0o600)).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn leaves_no_temp_file_behind() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sentinel.conf");
        write_atomic(&path, "port 26379\n", Some(0o600)).unwrap();
        let names: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["sentinel.conf".to_string()]);
    }

    #[test]
    fn recovers_from_a_stale_temp_file_of_a_crashed_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sentinel.conf");
        // What a crash mid-write leaves: a partial temp file, possibly with
        // permissions the next write would not choose.
        let stale = dir.path().join(".sentinel.conf.tmp");
        fs::write(&stale, "port 263").unwrap();
        fs::set_permissions(&stale, fs::Permissions::from_mode(0o644)).unwrap();
        write_atomic(&path, "port 26379\n", Some(0o600)).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "port 26379\n");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(!stale.exists());
    }

    #[test]
    fn a_missing_parent_directory_is_an_error_not_a_panic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("no-such-subdir").join("redis.conf");
        assert!(write_atomic(&path, "port 6379\n", None).is_err());
    }
}
