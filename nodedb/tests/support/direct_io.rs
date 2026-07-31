// SPDX-License-Identifier: BUSL-1.1

//! Runtime `O_DIRECT` probe shared by every integration test that spawns the
//! real server binary.
//!
//! The WAL ships with direct I/O on and refuses to downgrade itself, so a test
//! server may only be told to opt out on a filesystem that genuinely cannot
//! open files with `O_DIRECT`. Deciding that from the filesystem *type* is what
//! took the whole suite off the production write path once already — the same
//! blind spot an `O_DIRECT`-only replay bug hid behind. Filesystem support also
//! moves (tmpfs gained `O_DIRECT` in Linux 6.1), so the only trustworthy answer
//! is an actual `open(2)` against the directory the server will use.

#![allow(dead_code)] // Not every test binary needs every helper here.

use std::path::Path;
use std::process::Command;

/// Whether `dir`'s filesystem accepts `O_DIRECT` on `open(2)`.
///
/// Probes exactly the way the WAL's own segment open does, and creates `dir`
/// first because the probe runs before the server has ever touched the path.
pub fn direct_io_supported(dir: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt as _;

        if std::fs::create_dir_all(dir).is_err() {
            return false;
        }
        let probe = dir.join(".nodedb-direct-io-probe");
        let supported = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(&probe)
            .is_ok();
        let _ = std::fs::remove_file(&probe);
        supported
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = dir;
        false
    }
}

/// The `NODEDB_WAL_DIRECT_IO` value a server rooted at `data_dir` must be
/// started with, or `None` to leave the shipped default in place.
///
/// Opting out is reported on stderr rather than done quietly: a run that skips
/// the production direct-I/O path has weaker coverage than the suite claims,
/// and an operator reading a green test log has no other way to learn that.
pub fn wal_direct_io_override(data_dir: &Path) -> Option<&'static str> {
    if direct_io_supported(data_dir) {
        return None;
    }
    eprintln!(
        "NOTICE: {} is on a filesystem that rejects O_DIRECT — this run sets \
         NODEDB_WAL_DIRECT_IO=false and does NOT cover the production direct-I/O WAL path",
        data_dir.display()
    );
    Some("false")
}

/// Apply [`wal_direct_io_override`] to a server command about to be spawned
/// against `data_dir`. Sets nothing when the filesystem supports direct I/O, so
/// the child boots on the production default.
pub fn apply_wal_direct_io(cmd: &mut Command, data_dir: &Path) {
    if let Some(value) = wal_direct_io_override(data_dir) {
        cmd.env("NODEDB_WAL_DIRECT_IO", value);
    }
}
