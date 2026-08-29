// SPDX-License-Identifier: Apache-2.0

//! Durable atomic file / directory operations for checkpoint-class writes.
//!
//! The tmp-file + rename pattern is atomic only if both the file data and
//! the containing directory entry reach stable storage in the correct order.
//! On ext4 / XFS the rename metadata op can reach disk before the data pages
//! backing the tmp file — a power loss between the write and the next
//! checkpoint then leaves a correctly-named file containing zeros.
//!
//! [`atomic_write_fsync`] is the single helper all checkpoint-class writers
//! go through so the ordering (`write → sync_data → rename → fsync_dir`) is
//! enforced in one place. [`atomic_swap_dirs_fsync`] does the same for
//! directory-level swaps (rename old-dir → backup, rename new-dir → old-dir).
//!
//! [`read_checkpoint_dontneed`] pairs with the write helper on the read side:
//! checkpoint bytes are consumed once (deserialized into the in-memory index)
//! and then superseded. Leaving them in the page cache wastes memory needed
//! by hot workloads.

use std::fs;
use std::io::Write;
use std::path::{Component, Path};

use crate::error::{Result, WalError};

/// Fsync a directory to ensure file creation/deletion metadata is durable.
///
/// On ext4/XFS, creating or deleting a file writes the file data to disk
/// but the directory entry may only be in the page cache. A power loss
/// before the directory entry is persisted causes the file to "disappear"
/// on reboot. Calling fsync on the directory fd ensures the metadata
/// (filename, inode pointer) is on stable storage.
pub fn fsync_directory(dir: &Path) -> Result<()> {
    // Crash injection: the directory entry never reaches stable storage.
    // Every caller must treat this as a durability failure, not a warning.
    nodedb_types::fail_point_err!("wal::fsync_directory", |detail: String| WalError::Io(
        std::io::Error::other(format!("failpoint wal::fsync_directory: {detail}"))
    ));

    let dir_file = fs::File::open(dir).map_err(WalError::Io)?;
    dir_file.sync_all().map_err(WalError::Io)?;
    Ok(())
}

fn invalid_input(detail: String) -> WalError {
    WalError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        detail,
    ))
}

/// Reject a path that cannot be renamed within a single fsynced directory.
///
/// A `..` component makes the final name resolve outside the directory this
/// module fsyncs, so the durability ordering no longer covers the entry that
/// was created, and a name assembled from catalog or wire input can address a
/// directory the caller never intended to write. Both are rejected here rather
/// than left to the callers, which is why every caller passes paths it built
/// by joining a fixed base with a single name.
fn checked_parent<'a>(op: &str, label: &str, path: &'a Path) -> Result<&'a Path> {
    if path.components().any(|c| c == Component::ParentDir) {
        return Err(invalid_input(format!(
            "{op}: {label} path contains a '..' component: {}",
            path.display()
        )));
    }
    if !matches!(path.components().next_back(), Some(Component::Normal(_))) {
        return Err(invalid_input(format!(
            "{op}: {label} path does not end in a plain name: {}",
            path.display()
        )));
    }
    path.parent().ok_or_else(|| {
        invalid_input(format!(
            "{op}: {label} path has no parent directory: {}",
            path.display()
        ))
    })
}

/// Check that `other` resolves in the same directory as `parent`.
///
/// `rename` is atomic only within one filesystem directory, and this module
/// fsyncs exactly one parent, so a cross-directory pair would leave the new
/// entry undurable even though every call returned `Ok`.
fn same_parent(op: &str, label: &str, parent: &Path, other: &Path) -> Result<()> {
    let other_parent = checked_parent(op, label, other)?;
    if other_parent != parent {
        return Err(invalid_input(format!(
            "{op}: {label} ({}) is not in the fsynced directory {}",
            other.display(),
            parent.display()
        )));
    }
    Ok(())
}

/// Atomically write `bytes` to `dst` via a `tmp` file with full durability.
///
/// Order of operations (must not change):
/// 1. Create / truncate `tmp` and write `bytes`.
/// 2. `sync_data()` on `tmp` — forces file data pages to stable storage.
/// 3. `rename(tmp, dst)` — atomic on POSIX filesystems.
/// 4. `fsync_directory(parent)` — forces the directory entry durable so the
///    new name survives power loss.
///
/// `tmp` and `dst` MUST be in the same directory; otherwise rename is not
/// atomic and the parent fsync won't cover both entries. This is checked, not
/// assumed: a mismatched pair, or either path containing a `..` component,
/// returns `InvalidInput` before anything is written.
pub fn atomic_write_fsync(tmp: &Path, dst: &Path, bytes: &[u8]) -> Result<()> {
    let parent = checked_parent("atomic_write_fsync", "dst", dst)?;
    same_parent("atomic_write_fsync", "tmp", parent, tmp)?;

    {
        let mut f = fs::File::create(tmp).map_err(WalError::Io)?;
        f.write_all(bytes).map_err(WalError::Io)?;
        f.sync_data().map_err(WalError::Io)?;
    }

    fs::rename(tmp, dst).map_err(WalError::Io)?;
    fsync_directory(parent)?;
    Ok(())
}

/// Atomically swap a directory: `rename(live, backup); rename(staged, live)`,
/// fsyncing the parent directory once both renames have completed.
///
/// `live`, `backup`, and `staged` MUST share the same parent directory; this
/// is checked, and a path outside it or containing a `..` component returns
/// `InvalidInput` before either rename runs. The caller is responsible for
/// removing the backup directory once the new state is proven good — this
/// helper does not delete anything.
pub fn atomic_swap_dirs_fsync(live: &Path, backup: &Path, staged: &Path) -> Result<()> {
    let parent = checked_parent("atomic_swap_dirs_fsync", "live", live)?;
    same_parent("atomic_swap_dirs_fsync", "backup", parent, backup)?;
    same_parent("atomic_swap_dirs_fsync", "staged", parent, staged)?;

    fs::rename(live, backup).map_err(WalError::Io)?;
    fs::rename(staged, live).map_err(WalError::Io)?;
    fsync_directory(parent)?;
    Ok(())
}

/// Read a checkpoint file and advise the kernel to drop its pages from the
/// page cache.
///
/// Checkpoint files are consumed exactly once per process lifetime (loaded
/// into the in-memory index and then superseded). `posix_fadvise(DONTNEED)`
/// after read frees the page-cache memory for hot workloads.
///
/// On non-Unix targets the advise call is skipped and this degrades to a
/// plain read.
pub fn read_checkpoint_dontneed(path: &Path) -> Result<Vec<u8>> {
    let file = fs::File::open(path).map_err(WalError::Io)?;
    let len = file.metadata().map_err(WalError::Io)?.len();
    let bytes = fs::read(path).map_err(WalError::Io)?;

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd as _;
        // Safe: `file` owns the fd for the duration of the call; len fits in
        // off_t on all supported platforms (checkpoint files are << i64::MAX).
        let ret = unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                0,
                len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if ret != 0 {
            tracing::debug!(
                path = %path.display(),
                ret,
                "posix_fadvise(DONTNEED) returned nonzero — checkpoint bytes may stay in page cache"
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (file, len);
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_fsync_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");
        let tmp = dir.path().join("payload.ckpt.tmp");

        atomic_write_fsync(&tmp, &dst, b"hello world").unwrap();
        assert!(!tmp.exists(), "tmp must be renamed away");
        assert_eq!(fs::read(&dst).unwrap(), b"hello world");
    }

    #[test]
    fn atomic_write_fsync_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("payload.ckpt");
        let tmp = dir.path().join("payload.ckpt.tmp");

        atomic_write_fsync(&tmp, &dst, b"v1").unwrap();
        atomic_write_fsync(&tmp, &dst, b"v2").unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"v2");
    }

    #[test]
    fn atomic_swap_dirs_fsync_swaps() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("live");
        let backup = dir.path().join("backup");
        let staged = dir.path().join("staged");

        fs::create_dir(&live).unwrap();
        fs::write(live.join("marker"), b"old").unwrap();
        fs::create_dir(&staged).unwrap();
        fs::write(staged.join("marker"), b"new").unwrap();

        atomic_swap_dirs_fsync(&live, &backup, &staged).unwrap();

        assert_eq!(fs::read(live.join("marker")).unwrap(), b"new");
        assert_eq!(fs::read(backup.join("marker")).unwrap(), b"old");
        assert!(!staged.exists());
    }

    #[test]
    fn atomic_write_fsync_rejects_cross_directory_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other");
        fs::create_dir(&other).unwrap();
        let dst = dir.path().join("payload.ckpt");
        let tmp = other.join("payload.ckpt.tmp");

        let err = atomic_write_fsync(&tmp, &dst, b"x").unwrap_err();
        assert!(
            matches!(&err, WalError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput),
            "cross-directory tmp must be InvalidInput, got {err:?}"
        );
        assert!(!dst.exists(), "nothing may be written before the check");
        assert!(!tmp.exists());
    }

    #[test]
    fn atomic_write_fsync_rejects_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("sub").join("..").join("escaped.ckpt");
        let tmp = dir.path().join("sub").join("..").join("escaped.ckpt.tmp");

        let err = atomic_write_fsync(&tmp, &dst, b"x").unwrap_err();
        assert!(
            matches!(&err, WalError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput),
            "a `..` component must be InvalidInput, got {err:?}"
        );
        assert!(!dir.path().join("escaped.ckpt").exists());
    }

    #[test]
    fn atomic_swap_dirs_fsync_rejects_cross_directory_member() {
        let dir = tempfile::tempdir().unwrap();
        let other = dir.path().join("other");
        fs::create_dir(&other).unwrap();
        let live = dir.path().join("live");
        let staged = dir.path().join("staged");
        fs::create_dir(&live).unwrap();
        fs::write(live.join("marker"), b"old").unwrap();
        fs::create_dir(&staged).unwrap();

        // `backup` outside the fsynced directory: the rename would not be
        // covered by the single parent fsync this helper performs.
        let backup = other.join("backup");
        let err = atomic_swap_dirs_fsync(&live, &backup, &staged).unwrap_err();
        assert!(
            matches!(&err, WalError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput),
            "cross-directory backup must be InvalidInput, got {err:?}"
        );
        assert_eq!(
            fs::read(live.join("marker")).unwrap(),
            b"old",
            "live must be untouched when the check fails"
        );
    }

    #[test]
    fn read_checkpoint_dontneed_returns_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ckpt");
        fs::write(&path, b"checkpoint bytes").unwrap();

        let bytes = read_checkpoint_dontneed(&path).unwrap();
        assert_eq!(bytes, b"checkpoint bytes");
    }
}
