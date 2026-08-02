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
