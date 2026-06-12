// SPDX-License-Identifier: Apache-2.0

//! Segment filename conventions and on-disk metadata.

use std::path::{Path, PathBuf};

/// Default segment target size: 64 MiB.
///
/// This is a soft limit — the writer finishes the current record before rolling.
/// Actual segments may be slightly larger than this value.
pub const DEFAULT_SEGMENT_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// Segment file extension.
pub(crate) const SEGMENT_EXTENSION: &str = "seg";

/// Segment file prefix.
pub(crate) const SEGMENT_PREFIX: &str = "wal-";

/// Metadata about a WAL segment file on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Path to the segment file on disk.
    pub path: PathBuf,

    /// First LSN stored in this segment (from the filename).
    pub first_lsn: u64,

    /// File size in bytes.
    pub file_size: u64,
}

impl SegmentMeta {
    /// Path to the companion double-write buffer file.
    pub fn dwb_path(&self) -> PathBuf {
        self.path.with_extension("dwb")
    }
}

impl Ord for SegmentMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.first_lsn.cmp(&other.first_lsn)
    }
}

impl PartialOrd for SegmentMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Build a segment filename from a starting LSN.
pub fn segment_filename(first_lsn: u64) -> String {
    format!("{SEGMENT_PREFIX}{first_lsn:020}.{SEGMENT_EXTENSION}")
}

/// Build a full segment path within a WAL directory.
pub fn segment_path(wal_dir: &Path, first_lsn: u64) -> PathBuf {
    wal_dir.join(segment_filename(first_lsn))
}

/// Parse the first_lsn from a segment filename.
///
/// Returns `None` if the filename doesn't match the expected pattern.
pub(crate) fn parse_segment_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix(SEGMENT_PREFIX)?;
    let lsn_str = stem.strip_suffix(&format!(".{SEGMENT_EXTENSION}"))?;
    lsn_str.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_filename_format() {
        assert_eq!(segment_filename(1), "wal-00000000000000000001.seg");
        assert_eq!(segment_filename(999), "wal-00000000000000000999.seg");
        assert_eq!(segment_filename(u64::MAX), "wal-18446744073709551615.seg");
    }

    #[test]
    fn parse_segment_filename_valid() {
        assert_eq!(
            parse_segment_filename("wal-00000000000000000001.seg"),
            Some(1)
        );
        assert_eq!(
            parse_segment_filename("wal-00000000000000000999.seg"),
            Some(999)
        );
    }

    #[test]
    fn parse_segment_filename_invalid() {
        assert_eq!(parse_segment_filename("wal.log"), None);
        assert_eq!(parse_segment_filename("wal-abc.seg"), None);
        assert_eq!(parse_segment_filename("other-00001.seg"), None);
        assert_eq!(parse_segment_filename("wal-00001.dwb"), None);
    }
}
