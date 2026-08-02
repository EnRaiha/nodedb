// SPDX-License-Identifier: Apache-2.0

//! The recording implementation of the WAL's report sites.
//!
//! Every function here is called only on a failure path that is already about
//! to return an error, so the work it does costs nothing while the log is
//! healthy. None of them can fail: `Capture::emit` returns `None` when the host
//! never initialized the recorder and is documented never to panic, so the
//! result is deliberately discarded — a failure to record must never be worse
//! than the failure being recorded.

use std::path::Path;

use faultbox::{Capture, EventKind, error_chain_of};

use super::context;
use crate::error::WalError;

/// Name of the preserved segment inside a report directory.
///
/// A fixed plain path component on purpose: `faultbox` validates the name as a
/// single component (a separator or `..` would place the copy outside the
/// report directory), and keeping it stable lets a repeat detection of the same
/// bug recognise the snapshot it already holds instead of copying the segment
/// again.
const DAMAGED_SEGMENT_ARTIFACT: &str = "damaged-segment.wal";

/// Report a hole in a segment, preserving the damaged segment as it stands.
///
/// This runs during recovery, while the bad bytes are still on disk and before
/// anything truncates or rewrites them, so the copy is the only chance to keep
/// the evidence. A segment larger than the host's configured
/// `preserve_max_bytes` is not copied; the report then records that the
/// snapshot was skipped and where the source is, so a missing artifact is
/// stated rather than merely absent.
pub fn mid_file_corruption(
    err: &WalError,
    path: &Path,
    offset: u64,
    resync_offset: u64,
    resync_lsn: u64,
    last_lsn: u64,
) {
    let file_len = std::fs::metadata(path).ok().map(|meta| meta.len());
    let ctx = context::MidFileCorruption {
        path,
        offset,
        resync_offset,
        resync_lsn,
        last_lsn,
        file_len,
    };
    let note = format!(
        "verbatim copy of the WAL segment as recovery found it: damage begins at offset \
         {offset}, the first intact record past it is at offset {resync_offset} (LSN \
         {resync_lsn}), and the last LSN read before the damage was {last_lsn}"
    );
    let _ = Capture::new(
        EventKind::Corruption,
        "mid-file WAL corruption hides committed records",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .preserve("wal-segment", path, DAMAGED_SEGMENT_ARTIFACT, Some(note))
    .emit();
}

/// Report a whole segment missing from the middle of the log.
///
/// Nothing is preserved: the evidence is the file that is *not* there, and the
/// surviving segments on either side are intact and still in the WAL directory.
pub fn segment_lsn_gap(
    err: &WalError,
    path: &Path,
    previous_path: &Path,
    previous_last_lsn: u64,
    expected_lsn: u64,
    found_lsn: u64,
) {
    let ctx = context::SegmentLsnGap {
        path,
        previous_path,
        previous_last_lsn,
        expected_lsn,
        found_lsn,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "WAL segments are not contiguous: a segment is missing from the middle of the log",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a replay whose requested suffix starts below the retained floor.
///
/// Nothing is preserved: the evidence is the segments that are *no longer*
/// there, and the earliest surviving one is named in the report so an operator
/// can see where the log now begins.
pub fn replay_below_retained_floor(
    err: &WalError,
    path: &Path,
    from_lsn: u64,
    retained_floor_lsn: u64,
) {
    let ctx = context::ReplayBelowRetainedFloor {
        path,
        from_lsn,
        retained_floor_lsn,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "WAL replay asked for a suffix that truncation already deleted",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a writer entering its terminal state after a failed fsync.
///
/// Called from the one transition into that state, not from the checks that
/// repeat the error afterwards, so a poisoned writer rejecting a thousand
/// subsequent calls still accounts for one failure.
pub fn durability_lost(err: &WalError, detail: &str) {
    let ctx = context::DurabilityLost { detail };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "WAL fsync failed: acknowledged records may no longer exist on disk",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a record that is still ciphertext where plaintext is required.
pub fn encrypted_record_without_key(err: &WalError, lsn: u64, site: &'static str) {
    let ctx = context::EncryptedRecordWithoutKey { lsn, site };
    let _ = Capture::new(
        EventKind::Error,
        "encrypted WAL record reached a plaintext-only path with no decryption key",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}

/// Report a full device under a WAL append.
pub fn out_of_space(err: &WalError, site: &'static str, file_offset: u64, pending_bytes: u64) {
    let ctx = context::OutOfSpace {
        site,
        file_offset,
        pending_bytes,
    };
    let _ = Capture::new(
        EventKind::Error,
        "WAL append failed: no space left on device",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .emit();
}
