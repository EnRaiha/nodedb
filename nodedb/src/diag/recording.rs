// SPDX-License-Identifier: BUSL-1.1

//! Recording implementation of capture sites outside the WAL.
//!
//! Each function here is called only from the one site that detects its
//! failure, never re-emitted as the error propagates further up. None of them
//! can fail: `Capture::emit` returns `None` when the recorder was never
//! initialized and is documented never to panic, so the result is
//! deliberately discarded — a failure to record must never be worse than the
//! failure being recorded.

use std::sync::atomic::{AtomicU64, Ordering};

use faultbox::{Capture, EventKind, error_chain_of};
use nodedb_cluster::MetadataEntry;

use super::context;

/// Count of finished Data-Plane writes whose response the bounded response ring
/// refused, leaving the caller with nothing but a deadline.
///
/// Stays at zero by construction. The recorder's report is the forensic detail;
/// this counter is what makes the same failure visible to the metrics exporter
/// in a build with no recorder configured.
static DATA_PLANE_RESPONSES_LOST: AtomicU64 = AtomicU64::new(0);

/// Read the count of Data-Plane responses lost to a full response ring.
/// Exposed for the metrics exporter and tests.
pub fn data_plane_responses_lost() -> u64 {
    DATA_PLANE_RESPONSES_LOST.load(Ordering::Relaxed)
}

/// The decoded entry's variant name, read off its `Debug` text rather than an
/// exhaustive match against every `MetadataEntry` variant. A forensic label
/// tolerates an approximation that a routing decision would not, and reading
/// it this way means a new variant keeps reporting a real name here without
/// a matching arm to maintain.
pub fn entry_kind(entry: &MetadataEntry) -> String {
    let debug = format!("{entry:?}");
    match debug.find(|c: char| !(c.is_alphanumeric() || c == '_')) {
        Some(end) => debug[..end].to_owned(),
        None => debug,
    }
}

/// The stable class of an error's `Display` text: the text before the first
/// colon, which names what failed rather than the per-occurrence detail
/// after it.
fn error_class(err: &crate::Error) -> String {
    let text = err.to_string();
    text.split(':').next().unwrap_or(&text).trim().to_owned()
}

/// Report a durable host-side effect failure that stopped the metadata
/// applier without advancing its watermark.
///
/// Called from the one site that detects this: the `apply` loop's `break` on
/// `apply_host_side_effects` returning `Err`. Not re-emitted by anything
/// above it, so an entry that Raft keeps re-delivering because it keeps
/// failing files one report with a growing occurrence count, not one per
/// retry.
pub fn metadata_apply_wedged(
    err: &crate::Error,
    entry: &MetadataEntry,
    raft_index: u64,
    last_applied_watermark: u64,
    permanent: bool,
) {
    let kind = entry_kind(entry);
    let class = error_class(err);
    let ctx = context::MetadataApplyWedged {
        raft_index,
        last_applied_watermark,
        entry_kind: &kind,
        error_class: &class,
        permanent,
    };
    let _ = Capture::new(
        EventKind::Error,
        "metadata applier: durable host-side effect failed; watermark not advanced",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an ILP connection terminated by an undecodable line while it still
/// held accepted lines.
///
/// Called from the invalid-UTF-8 arm of the ILP connection loop, the only
/// place that cause is detected. The sibling read-failure arm is a different
/// root cause (a broken socket or an over-length line, not malformed content)
/// and files its own report.
pub fn ilp_invalid_utf8_drop(
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    record_ilp_drop("invalid_utf8", peer, database_id, buffered_lines, outcome);
}

/// Report an ILP connection terminated by a failed or over-length line read
/// while it still held accepted lines.
///
/// Called from the read-error arm of the ILP connection loop, the only place
/// that cause is detected.
pub fn ilp_line_read_drop(
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    record_ilp_drop(
        "line_read_failed",
        peer,
        database_id,
        buffered_lines,
        outcome,
    );
}

/// Shared emit for the two ILP termination causes. Private so the only entry
/// points remain the one-per-cause functions above — a shared *public* entry
/// point would invite a third caller reporting a cause it did not detect.
fn record_ilp_drop(
    cause: &'static str,
    peer: &str,
    database_id: u64,
    buffered_lines: u64,
    outcome: context::IlpFlushOutcome,
) {
    let ctx = context::IlpAcceptedLinesDropped {
        cause,
        peer,
        database_id,
        buffered_lines,
        outcome,
    };
    let _ = Capture::new(
        EventKind::Error,
        "ILP connection terminated holding lines the client can never learn the fate of",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a committed, CRC-valid WAL record that startup replay could not
/// apply.
///
/// Called only from `replay_abort`, the one place recovery decides a record is
/// unapplyable, so a WAL tail that fails identically on every core files one
/// report with a growing occurrence count rather than one per core.
pub fn replay_record_unapplied(
    engine: &str,
    stage: &str,
    core_id: usize,
    record_lsn: u64,
    detail: &str,
) {
    let ctx = context::ReplayRecordUnapplied {
        engine,
        stage,
        core_id,
        record_lsn,
        detail,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "WAL replay: a committed record could not be applied",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an acknowledged write whose redo record the Control-Plane funnel was
/// supposed to mint but did not.
///
/// Called only from the durable-at-ack barrier in `submit_write`, the one place
/// that knows both what the plan required and what was actually appended. Not
/// re-emitted anywhere above it, so a workload hammering the same unclassified
/// op files one report with a growing occurrence count rather than one per row.
pub fn write_acked_without_durability(engine: &'static str) {
    let ctx = context::WriteAckedWithoutDurability { engine };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "write acknowledged with no durable redo record",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document write rejected because its inverted-index update failed.
///
/// Called from the one site that detects it: the point-put apply path's
/// `index_document_in_txn` error arm. The error propagates from there, so the
/// caller's write transaction — which carries both the row and the index
/// entry — is dropped un-committed and neither half is durable. The report
/// exists because that rejection is invisible in the write's error message:
/// the client learns the write failed, not that the collection's full-text
/// index is what failed it.
pub fn fts_index_update_failed(err: &crate::Error, collection: &str, surrogate: u32) {
    let class = error_class(err);
    let ctx = context::FtsIndexUpdateFailed {
        collection,
        surrogate,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Error,
        "document write rejected: full-text index update failed",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a document batch insert refused because its rows carry no surrogates.
///
/// Called from the one site that detects it: the batch-insert handler's
/// parallel-length guard. The rejection propagates from there, so nothing is
/// written and the caller is told the insert failed. The report exists because
/// the rejection names only the symptom — the defect is in whatever produced a
/// plan whose surrogate list is not parallel to its document list, and that
/// producer is not visible from the Data Plane.
pub fn batch_insert_without_surrogates(
    collection: &str,
    document_count: usize,
    surrogate_count: usize,
) {
    let ctx = context::BatchInsertWithoutSurrogates {
        collection,
        document_count,
        surrogate_count,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "document batch insert refused: rows carry no cross-engine identity",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a completed Data-Plane write whose response the bounded response ring
/// refused, so the caller can only ever learn a deadline.
///
/// Called from the one site that detects it: the response-push helper every
/// core-loop completion path funnels through. Not re-emitted anywhere above it
/// — nothing above it knows the response existed — so a ring that stays
/// saturated files one report per fate with a growing occurrence count rather
/// than one per dropped response.
pub fn data_plane_response_lost(core_id: usize, write: context::LostResponseWrite) {
    DATA_PLANE_RESPONSES_LOST.fetch_add(1, Ordering::Relaxed);
    let ctx = context::DataPlaneResponseLost { core_id, write };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Data-Plane response dropped: the caller can never learn this write's outcome",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report a Calvin cross-shard transaction whose completion wait timed out.
///
/// Called from the completion-timeout arm of
/// `submit_and_await_calvin_with_timeout`, the only place this failure is
/// detected — the sibling "channel closed" arm is a different root cause
/// (registry shutdown, not a missing ack) and is not reported here.
pub fn calvin_completion_timeout(
    err: &crate::Error,
    epoch: u64,
    position: u32,
    participants: usize,
    timeout_secs: u64,
) {
    let ctx = context::CalvinCompletionTimeout {
        epoch,
        position,
        participants,
        timeout_secs,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Calvin transaction completion wait timed out",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
