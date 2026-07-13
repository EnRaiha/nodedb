// SPDX-License-Identifier: BUSL-1.1

//! In-flight transaction types for the Calvin scheduler driver.

use std::collections::BTreeSet;
use std::time::Instant;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::super::lock_manager::LockKey;

/// An in-flight transaction that has been dispatched and is awaiting a
/// Data Plane response.
///
/// The executor response channel is held by a bridge task (see
/// `Scheduler::spawn_response_bridge`) that forwards completions to the
/// scheduler's fan-in `completion_rx`. This avoids polling and ensures the
/// main `select!` loop wakes the moment a response arrives.
pub(super) struct PendingTxn {
    /// Original sequenced transaction (for WAL record on completion).
    pub txn: SequencedTxn,
    /// Wall-clock time at dispatch (for lock-wait latency metrics).
    ///
    /// `Instant::now()` is used here for observability only; never
    /// influences WAL bytes.
    pub dispatch_time: Instant,
    /// Whether this vShard's slice carries a primary user data write (a non-edge
    /// Document/KV/Vector/Timeseries/Columnar/Array write). Only the primary-write
    /// participant deposits its applied `Response` (affected-count and any
    /// RETURNING rows) into `SharedState::calvin_apply_results`. The implicit-edge
    /// cleanup participants that dual-home alongside it carry no primary write and
    /// so never clobber the entry the coordinator drains.
    pub has_primary_write: bool,
    /// Commit-resolution state for a static-set Calvin txn.
    ///
    /// `Some(CommitState::Staged)` for a static txn dispatched via the
    /// validate-and-stage path: its first executor response carries the local
    /// commit vote and drives a flush-or-drop before the commit tail runs.
    /// `None` for dependent/active txns, which apply directly.
    pub commit_state: Option<CommitState>,
}

/// Commit-resolution state of a staged static Calvin transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver) enum CommitState {
    /// Awaiting the validate-and-stage response, whose `read_set_valid` carries
    /// the local commit vote that drives the flush-or-drop decision.
    Staged,
    /// The txn committed and a `MetaOp::CalvinResolve` has been dispatched to
    /// resolve its staged post-images into a replayable `RedoRecord`; awaiting
    /// that response before the redo is WAL-appended and the flush dispatched.
    AwaitingRedoResolve,
    /// A flush (`committed = true`) or drop (`committed = false`) has been
    /// dispatched; awaiting its response before the commit tail runs.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was appended
    /// for this commit's non-empty write set — `commit_apply_tail` then only
    /// records write versions at it, since the redo record already IS the
    /// applied marker. `None` for a drop, or a committed txn whose resolved
    /// redo carried no ops (pure read / CRDT) — `commit_apply_tail` falls back
    /// to appending a `CalvinApplied` marker in that case.
    AwaitingResolve {
        committed: bool,
        redo_lsn: Option<crate::types::Lsn>,
    },
}

/// A transaction that is blocked on lock acquisition.
pub(super) struct BlockedTxn {
    pub txn: SequencedTxn,
    pub keys: BTreeSet<LockKey>,
    /// Wall-clock time at first block (for latency metrics).
    ///
    /// `Instant::now()` used for observability only.
    pub blocked_at: Instant,
}
