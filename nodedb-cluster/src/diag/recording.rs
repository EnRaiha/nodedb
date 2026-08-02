// SPDX-License-Identifier: BUSL-1.1

//! The recording implementation of the Calvin sequencer's report sites.
//!
//! Both functions are called from paths that already log and count the drop
//! today; this only adds a structured report alongside that, at the same
//! site, without changing what either caller returns or does next. None of
//! them can fail: `Capture::emit` returns `None` when the host never
//! initialized the recorder and is documented never to panic, so the result
//! is deliberately discarded — a failure to record must never be worse than
//! the failure being recorded.

use faultbox::{Capture, EventKind};

use super::context::{self, DroppedTxn};

/// Report a break in the sequencer's epoch sequence.
///
/// Called from the one site that detects this: `apply`'s epoch-ordering check.
/// `direction` is the classification that site made (`"ahead"` — entries
/// missing on this replica; `"behind"` — an already-consumed epoch proposed
/// again), which decides both the report's wording and its grouping. There is
/// no live error object at this point — the break is a state-machine invariant
/// check, not a propagated failure — so no `error_chain` is attached.
pub fn sequencer_epoch_gap(
    epoch_expected: u64,
    epoch_found: u64,
    direction: &'static str,
    txns_in_batch: usize,
    raft_index: u64,
) {
    // Absolute distance: `found` is below `expected` in the "behind" case, and
    // a saturating subtraction one way round would report every regression as
    // a zero-width break.
    let gap = epoch_found.abs_diff(epoch_expected);
    let ctx = context::SequencerEpochGap {
        epoch_expected,
        epoch_found,
        gap,
        direction,
        txns_in_batch,
        raft_index,
    };
    let _ = Capture::new(
        EventKind::InvariantViolation,
        "Calvin sequencer: epoch sequence break detected",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report the transactions dropped from a single `apply` call's fan-out loop
/// because a destination vshard channel was full or closed.
///
/// Called once after the fan-out loop completes, not per dropped
/// transaction — per-txn emission would report-storm under sustained
/// backpressure, since the loop can drop many positions in one call. Only
/// called when at least one transaction was dropped.
pub fn sequencer_backpressure_drop(epoch: u64, dropped_count: u64, drops: &[(u32, &'static str)]) {
    let drops: Vec<DroppedTxn> = drops
        .iter()
        .map(|&(vshard, cause)| DroppedTxn { vshard, cause })
        .collect();
    let ctx = context::SequencerBackpressureDrop {
        epoch,
        dropped_count,
        drops: &drops,
    };
    let _ = Capture::new(
        EventKind::Error,
        "Calvin sequencer: transactions dropped from epoch fan-out under backpressure",
    )
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
