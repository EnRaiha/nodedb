// SPDX-License-Identifier: Apache-2.0

//! Segment files a roll created but never installed.
//!
//! A roll creates the next segment file before it seals the current one. When
//! a step in between fails, the old writer stays active and goes on writing
//! the LSNs the new file's name claims. Two guards keep that file from ever
//! being resumed:
//!
//! - [`discard_unused_segment`] removes it before the roll returns its error.
//! - [`check_resume_above_previous`] refuses to open a WAL whose last segment
//!   starts at or below an LSN an earlier segment already holds. That covers
//!   a crash mid-roll and a removal that itself failed.

use std::path::Path;

use crate::error::{Result, WalError};
use crate::recovery::recover;

use super::atomic_io::fsync_directory;
use super::meta::SegmentMeta;

/// Remove the segment file a failed roll created, and fsync the directory so
/// the removal survives a crash. Returns the error the caller returns: `roll_err`
/// alone, or [`WalError::RollCleanupFailed`] naming both failures.
pub(crate) fn discard_unused_segment(wal_dir: &Path, path: &Path, roll_err: WalError) -> WalError {
    let removed = match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        // The roll may have failed before the file existed.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    };
    let cleanup = removed
        .map_err(WalError::Io)
        .and_then(|()| fsync_directory(wal_dir));
    match cleanup {
        Ok(()) => roll_err,
        Err(cleanup_err) => WalError::RollCleanupFailed {
            path: path.display().to_string(),
            roll: Box::new(roll_err),
            cleanup: Box::new(cleanup_err),
        },
    }
}

/// Refuse to resume `segments` (in LSN order) when the last one would reissue
/// LSNs an earlier segment already holds.
///
/// `resumed_next_lsn` is the next LSN the writer resumed on the last segment
/// would assign. The nearest earlier segment with records must end below it.
///
/// Refusing is safer than removing the file here. The file's name claims LSNs
/// that are already written elsewhere, but only a scan says it holds no
/// records, and a damaged header scans as empty. Deleting on that evidence
/// can destroy records. Refusing destroys nothing, and the error names the
/// file an operator removes.
pub fn check_resume_above_previous(segments: &[SegmentMeta], resumed_next_lsn: u64) -> Result<()> {
    let Some((last, earlier)) = segments.split_last() else {
        return Ok(());
    };
    for previous in earlier.iter().rev() {
        let info = recover(&previous.path)?;
        if info.record_count == 0 {
            continue;
        }
        if info.last_lsn >= resumed_next_lsn {
            return Err(WalError::SegmentOverlapsPrevious {
                path: last.path.display().to_string(),
                first_lsn: last.first_lsn,
                previous_path: previous.path.display().to_string(),
                previous_last_lsn: info.last_lsn,
            });
        }
        return Ok(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::segment::{discover_segments, segment_path};
    use crate::writer::{WalWriter, WalWriterConfig};

    fn config() -> WalWriterConfig {
        WalWriterConfig {
            use_direct_io: false,
            ..Default::default()
        }
    }

    /// Write a segment starting at `first_lsn` holding `records` records.
    fn write_segment(dir: &Path, first_lsn: u64, records: u64) {
        let path = segment_path(dir, first_lsn);
        let mut writer = WalWriter::open_with_start_lsn(&path, config(), first_lsn).unwrap();
        for i in 0..records {
            writer
                .append(RecordType::Put as u32, 1, 0, 0, &i.to_le_bytes())
                .unwrap();
        }
        writer.sync().unwrap();
    }

    #[test]
    fn an_empty_segment_inside_the_previous_segments_lsns_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        // The old segment kept writing LSNs 1..=5 after a roll created 3.
        write_segment(dir.path(), 1, 5);
        write_segment(dir.path(), 3, 0);
        let segments = discover_segments(dir.path()).unwrap();
        let err = check_resume_above_previous(&segments, 3).unwrap_err();
        assert!(
            matches!(
                err,
                WalError::SegmentOverlapsPrevious {
                    first_lsn: 3,
                    previous_last_lsn: 5,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn an_empty_segment_after_the_previous_one_resumes() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 1, 5);
        write_segment(dir.path(), 6, 0);
        let segments = discover_segments(dir.path()).unwrap();
        check_resume_above_previous(&segments, 6).unwrap();
    }

    #[test]
    fn discard_removes_the_file_and_keeps_the_roll_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 9);
        std::fs::write(&path, b"").unwrap();
        let err = discard_unused_segment(dir.path(), &path, WalError::Sealed);
        assert!(matches!(err, WalError::Sealed), "{err}");
        assert!(!path.exists());
    }

    #[test]
    fn discard_of_a_missing_file_keeps_the_roll_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 9);
        let err = discard_unused_segment(dir.path(), &path, WalError::Sealed);
        assert!(matches!(err, WalError::Sealed), "{err}");
    }

    #[test]
    fn a_failed_removal_names_both_failures() {
        let dir = tempfile::tempdir().unwrap();
        // A non-empty directory at the segment path cannot be removed as a file.
        let path = segment_path(dir.path(), 9);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("x"), b"x").unwrap();
        let err = discard_unused_segment(dir.path(), &path, WalError::Sealed);
        assert!(matches!(err, WalError::RollCleanupFailed { .. }), "{err}");
        let msg = err.to_string();
        assert!(msg.contains("sealed") || msg.contains("Sealed"), "{msg}");
        assert!(msg.contains(&path.display().to_string()), "{msg}");
    }
}
