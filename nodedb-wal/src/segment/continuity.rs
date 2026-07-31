// SPDX-License-Identifier: Apache-2.0

//! Cross-segment LSN continuity for multi-segment replay.
//!
//! Within a segment, a stop point with committed records behind it is caught by
//! [`crate::torn_tail::verify_committed_prefix`]. The same class of silent data
//! loss exists one level up: replay concatenates the records of every segment
//! in the directory, so a segment file that has gone missing from the middle of
//! the log produces a record stream with a hole in it and no error anywhere.
//!
//! Segments are written in strictly increasing LSN order, so the boundary is
//! checkable: the next segment's declared first LSN must not be above the LSN
//! that follows the last record of the previous one.
//!
//! Two things are deliberately *not* holes:
//!
//! - **A missing prefix.** Checkpoint truncation deletes whole segments below
//!   the checkpoint by design, so the first surviving segment may start
//!   anywhere. Only boundaries between surviving segments are checked.
//! - **A segment with no records.** A rollover that was never written to says
//!   nothing about continuity, so the boundary carries over from the last
//!   segment that actually held records.

use std::path::PathBuf;

use crate::error::{Result, WalError};

use super::meta::SegmentMeta;

/// Running LSN boundary between the segments replayed so far.
#[derive(Debug, Default)]
pub struct SegmentContinuity {
    /// Path and highest LSN of the most recent segment that was read to
    /// completion and contained at least one record.
    previous: Option<(PathBuf, u64)>,
}

impl SegmentContinuity {
    /// Start a fresh check with no established boundary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verify that `segment` continues the log where the previous completed
    /// segment left off. Call this before reading `segment`.
    pub fn check(&self, segment: &SegmentMeta) -> Result<()> {
        let Some((previous_path, previous_last_lsn)) = &self.previous else {
            return Ok(());
        };

        let expected_lsn = previous_last_lsn.saturating_add(1);
        if segment.first_lsn > expected_lsn {
            let err = WalError::SegmentLsnGap {
                path: segment.path.display().to_string(),
                previous_path: previous_path.display().to_string(),
                previous_last_lsn: *previous_last_lsn,
                expected_lsn,
                found_lsn: segment.first_lsn,
            };
            // The boundary check is the only place that can see the hole: a
            // single segment says nothing about the one that should precede it.
            crate::diag::segment_lsn_gap(
                &err,
                &segment.path,
                previous_path,
                *previous_last_lsn,
                expected_lsn,
                segment.first_lsn,
            );
            return Err(err);
        }

        Ok(())
    }

    /// Record that `segment` was read all the way through, ending at
    /// `last_lsn`. Call this only after the segment has been fully consumed —
    /// a replay that stopped early (a record limit) has learned nothing about
    /// where the segment ends.
    ///
    /// `last_lsn` must be the highest LSN the segment *contains*, not the
    /// highest one the caller kept: LSN filtering changes what is returned, not
    /// what is on disk. A `last_lsn` of 0 means the segment held no records.
    pub fn completed(&mut self, segment: &SegmentMeta, last_lsn: u64) {
        if last_lsn != 0 {
            self.previous = Some((segment.path.clone(), last_lsn));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn meta(first_lsn: u64) -> SegmentMeta {
        SegmentMeta {
            path: Path::new("/wal").join(super::super::segment_filename(first_lsn)),
            first_lsn,
            file_size: 0,
        }
    }

    #[test]
    fn first_segment_may_start_anywhere() {
        let continuity = SegmentContinuity::new();
        assert!(continuity.check(&meta(5000)).is_ok());
    }

    #[test]
    fn contiguous_segments_pass() {
        let mut continuity = SegmentContinuity::new();
        let first = meta(1);
        continuity.check(&first).unwrap();
        continuity.completed(&first, 42);
        assert!(continuity.check(&meta(43)).is_ok());
    }

    #[test]
    fn gap_between_segments_is_rejected() {
        let mut continuity = SegmentContinuity::new();
        let first = meta(1);
        continuity.completed(&first, 42);
        match continuity.check(&meta(90)) {
            Err(WalError::SegmentLsnGap {
                expected_lsn,
                found_lsn,
                ..
            }) => {
                assert_eq!(expected_lsn, 43);
                assert_eq!(found_lsn, 90);
            }
            other => panic!("expected a segment LSN gap, got {other:?}"),
        }
    }

    #[test]
    fn empty_segment_keeps_the_previous_boundary() {
        let mut continuity = SegmentContinuity::new();
        let first = meta(1);
        continuity.completed(&first, 42);
        let empty = meta(43);
        continuity.check(&empty).unwrap();
        continuity.completed(&empty, 0);
        assert!(continuity.check(&meta(43)).is_ok());
        assert!(continuity.check(&meta(90)).is_err());
    }
}
