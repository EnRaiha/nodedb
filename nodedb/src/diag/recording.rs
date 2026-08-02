// SPDX-License-Identifier: BUSL-1.1

//! Recording implementation of capture sites outside the WAL.
//!
//! Each function here is called only from the one site that detects its
//! failure, never re-emitted as the error propagates further up. None of them
//! can fail: `Capture::emit` returns `None` when the recorder was never
//! initialized and is documented never to panic, so the result is
//! deliberately discarded — a failure to record must never be worse than the
//! failure being recorded.

use faultbox::{Capture, EventKind, error_chain_of};
use nodedb_cluster::MetadataEntry;

use super::context;

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
