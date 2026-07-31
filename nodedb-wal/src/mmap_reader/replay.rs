// SPDX-License-Identifier: Apache-2.0

//! Multi-segment replay over mmap'd WAL segments.
//!
//! Concatenating segments is only safe if two things hold: each segment stops
//! where the log really ends (not at a hole with committed records behind it),
//! and no segment has gone missing from between the ones that survive. The
//! first is [`crate::torn_tail::verify_committed_prefix`], the second is
//! [`SegmentContinuity`]; both are applied here so the Event Plane's catchup
//! path cannot observe a silently shortened log.

use std::path::Path;

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::record::WalRecord;
use crate::segment::{SegmentContinuity, SegmentDecryptor, SegmentMeta};

use super::reader::MmapWalReader;

/// Minimum number of segments to justify parallel replay overhead.
const PARALLEL_SEGMENT_THRESHOLD: usize = 4;

/// One fully consumed segment: the records the caller asked for, plus the
/// highest LSN the segment actually contains.
struct SegmentScan {
    records: Vec<WalRecord>,
    /// Highest LSN present in the segment, ignoring the `from_lsn` filter —
    /// continuity is a property of the log, not of the caller's window into it.
    /// Zero means the segment held no records.
    last_lsn: u64,
}

/// Replay WAL segments from a directory using mmap, starting from `from_lsn`.
///
/// Discovers all sealed segments, mmap's each, and returns records with
/// LSN >= `from_lsn`. This is the Event Plane's tier-2 catchup path.
///
/// When 4+ segments need scanning, uses `std::thread::scope` to read
/// segments in parallel (one thread per segment). Each thread mmap's its
/// segment and filters records independently; results are merged in
/// segment order (already LSN-sorted since segments are monotonic).
///
/// Records are returned as plaintext. `keys` must be the key ring the directory
/// was written under, or `None` for a WAL that was never encrypted; an
/// encrypted record met with `keys == None` fails with
/// [`WalError::EncryptedRecordWithoutKey`] rather than reaching the caller as
/// ciphertext.
pub fn replay_segments_mmap(
    wal_dir: &Path,
    from_lsn: u64,
    keys: Option<&KeyRing>,
) -> Result<Vec<WalRecord>> {
    let segments = crate::segment::discover_segments(wal_dir)?;
    let live = filter_segments_by_lsn(&segments, from_lsn);

    if live.len() < PARALLEL_SEGMENT_THRESHOLD {
        return replay_segments_sequential(live, from_lsn, keys);
    }

    replay_segments_parallel(live, from_lsn, keys)
}

/// Return the slice of `segments` whose LSN range may contain records with
/// lsn >= `from_lsn`. A segment at index `i` is skippable iff the next
/// segment's `first_lsn` is `<= from_lsn` — meaning segment `i`'s entire
/// range is strictly below the cutoff. The last segment is never skipped
/// on this criterion because its upper bound is unknown.
fn filter_segments_by_lsn(segments: &[SegmentMeta], from_lsn: u64) -> &[SegmentMeta] {
    // Find the first segment whose next-segment first_lsn > from_lsn, OR
    // the last segment (always live). Since segments are LSN-sorted, the
    // live tail starts at the largest i such that segments[i].first_lsn
    // <= from_lsn.
    let mut start = 0;
    for i in 0..segments.len() {
        // Segment i covers [first_lsn_i, first_lsn_{i+1}).
        let upper = segments.get(i + 1).map(|s| s.first_lsn).unwrap_or(u64::MAX);
        if upper > from_lsn {
            start = i;
            break;
        }
        start = i + 1;
    }
    if start >= segments.len() {
        // All segments strictly below from_lsn; nothing to replay.
        return &[];
    }
    &segments[start..]
}

/// Read one segment end-to-end, keeping records at or above `from_lsn`.
fn scan_segment(
    segment: &SegmentMeta,
    from_lsn: u64,
    keys: Option<&KeyRing>,
) -> Result<SegmentScan> {
    let mut reader = MmapWalReader::open(&segment.path)?;
    let decryptor = SegmentDecryptor::new(reader.segment_preamble(), keys);
    let mut records = Vec::new();
    let mut last_lsn = 0u64;
    while let Some(record) = reader.next_record()? {
        last_lsn = record.header.lsn;
        if record.header.lsn >= from_lsn {
            records.push(decryptor.decrypt_record(record)?);
        }
    }
    // A segment that stops early may be an interrupted final write, or it may
    // be a hole with committed records behind it. Only the first is a legal
    // end of the log.
    crate::torn_tail::verify_committed_prefix(&segment.path, reader.stop_reason(), last_lsn)?;
    reader.release_pages();
    Ok(SegmentScan { records, last_lsn })
}

/// Sequential segment replay (used for small segment counts).
///
/// `segments` is already the live tail: replay may legitimately start in the
/// middle of the log, and everything below it is out of scope for continuity
/// exactly as a checkpoint-truncated prefix would be.
fn replay_segments_sequential(
    segments: &[SegmentMeta],
    from_lsn: u64,
    keys: Option<&KeyRing>,
) -> Result<Vec<WalRecord>> {
    let mut records = Vec::new();
    let mut continuity = SegmentContinuity::new();
    for seg in segments {
        continuity.check(seg)?;
        let scan = scan_segment(seg, from_lsn, keys)?;
        records.extend(scan.records);
        continuity.completed(seg, scan.last_lsn);
    }
    Ok(records)
}

/// Parallel segment replay using scoped threads.
///
/// Each segment is read in its own thread via mmap. Since segments are
/// monotonically ordered by LSN, concatenating per-segment results in
/// segment order produces a globally LSN-ordered result. Continuity is a
/// property of the sequence, so it is judged after the joins, in segment
/// order.
fn replay_segments_parallel(
    segments: &[SegmentMeta],
    from_lsn: u64,
    keys: Option<&KeyRing>,
) -> Result<Vec<WalRecord>> {
    // Collect per-segment results. Index corresponds to segment order.
    let mut per_segment: Vec<Result<SegmentScan>> = Vec::with_capacity(segments.len());

    std::thread::scope(|scope| {
        let handles: Vec<_> = segments
            .iter()
            .map(|seg| scope.spawn(move || scan_segment(seg, from_lsn, keys)))
            .collect();

        for handle in handles {
            per_segment.push(handle.join().unwrap_or_else(|_| {
                Err(WalError::Io(std::io::Error::other(
                    "segment replay thread panicked",
                )))
            }));
        }
    });

    // Merge in segment order (preserves LSN ordering).
    let total_estimate: usize = per_segment
        .iter()
        .map(|r| r.as_ref().map(|scan| scan.records.len()).unwrap_or(0))
        .sum();
    let mut records = Vec::with_capacity(total_estimate);
    let mut continuity = SegmentContinuity::new();
    for (seg, seg_result) in segments.iter().zip(per_segment) {
        continuity.check(seg)?;
        let scan = seg_result?;
        records.extend(scan.records);
        continuity.completed(seg, scan.last_lsn);
    }

    Ok(records)
}

/// Paginated mmap replay: reads at most `max_records` from `from_lsn`.
///
/// Returns `(records, has_more)` where `has_more` is `true` if the limit
/// was reached before all segments were exhausted. This bounds memory
/// usage per catch-up cycle to O(max_records) instead of O(all WAL data).
///
/// Always uses sequential reading (no parallel threads) since the bounded
/// record count makes parallel overhead unnecessary.
///
/// Records are returned as plaintext; see [`replay_segments_mmap`] for `keys`.
pub fn replay_segments_mmap_limit(
    wal_dir: &Path,
    from_lsn: u64,
    max_records: usize,
    keys: Option<&KeyRing>,
) -> Result<(Vec<WalRecord>, bool)> {
    let segments = crate::segment::discover_segments(wal_dir)?;
    let live = filter_segments_by_lsn(&segments, from_lsn);
    let mut records = Vec::with_capacity(max_records.min(4096));
    let mut continuity = SegmentContinuity::new();

    for seg in live {
        continuity.check(seg)?;
        let mut reader = MmapWalReader::open(&seg.path)?;
        let decryptor = SegmentDecryptor::new(reader.segment_preamble(), keys);
        let mut last_lsn = 0u64;
        while let Some(record) = reader.next_record()? {
            last_lsn = record.header.lsn;
            if record.header.lsn >= from_lsn {
                records.push(decryptor.decrypt_record(record)?);
                if records.len() >= max_records {
                    // Partial scan — don't release pages for a segment
                    // we'll likely re-open on the next catchup cycle. Stopping
                    // short also leaves the rest of this segment unread, so its
                    // end LSN is unknown and no boundary can be judged.
                    return Ok((records, true));
                }
            }
        }
        crate::torn_tail::verify_committed_prefix(&seg.path, reader.stop_reason(), last_lsn)?;
        reader.release_pages();
        continuity.completed(seg, last_lsn);
    }

    Ok((records, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    #[test]
    fn replay_mmap_from_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let config = crate::segmented::SegmentedWalConfig::for_testing(wal_dir.clone());
        let mut wal = crate::segmented::SegmentedWal::open(config).unwrap();

        let lsn1 = wal.append(RecordType::Put as u32, 1, 0, 0, b"a").unwrap();
        let lsn2 = wal.append(RecordType::Put as u32, 1, 0, 0, b"b").unwrap();
        let lsn3 = wal.append(RecordType::Put as u32, 1, 0, 0, b"c").unwrap();
        wal.sync().unwrap();

        // Replay from lsn2 — should get records b and c.
        let records = replay_segments_mmap(&wal_dir, lsn2, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].header.lsn, lsn2);
        assert_eq!(records[1].header.lsn, lsn3);

        // Replay from lsn1 — all 3.
        let all = replay_segments_mmap(&wal_dir, lsn1, None).unwrap();
        assert_eq!(all.len(), 3);
    }
}
