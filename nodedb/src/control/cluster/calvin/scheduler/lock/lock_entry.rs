// SPDX-License-Identifier: BUSL-1.1

//! Per-key lock state for the Calvin lock table.
//!
//! A [`LockEntry`] holds either a single exclusive holder or a set of shared
//! holders, plus a FIFO waiter queue that carries each waiter's requested
//! [`LockMode`].

use std::collections::VecDeque;

use smallvec::SmallVec;

use super::lock_key::TxnId;

// ── LockMode ──────────────────────────────────────────────────────────────────

/// The mode under which a lock is held or requested.
///
/// `Shared` locks are mutually compatible (many readers); `Exclusive` locks
/// conflict with everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Compatible with other shared holders on the same key.
    Shared,
    /// Held by exactly one transaction; excludes all others.
    Exclusive,
}

// ── AcquireOutcome ────────────────────────────────────────────────────────────

/// Result of a lock-acquire call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireOutcome {
    /// All requested locks were granted; the transaction is ready to dispatch.
    Ready,
    /// At least one key conflicted with a current holder.  The transaction has
    /// been enqueued as a waiter on every unavailable key.
    Blocked,
}

// ── LockEntry ─────────────────────────────────────────────────────────────────

/// Per-key lock state.
///
/// Invariants maintained by [`LockManager`](super::manager::LockManager):
/// - `Exclusive` entries have exactly one holder.
/// - `Shared` entries have one or more holders, all compatible.
/// - `waiters` is FIFO; each waiter carries the mode it requested so that
///   promotion on release is mode-aware.
pub(super) struct LockEntry {
    /// The mode the current holders hold this lock under.
    pub(super) mode: LockMode,
    /// The transactions currently holding this lock. Exclusive: exactly one;
    /// Shared: one or more.
    pub(super) holders: SmallVec<[TxnId; 2]>,
    /// Transactions waiting for this lock, in FIFO order, each tagged with the
    /// mode it requested.
    pub(super) waiters: VecDeque<(TxnId, LockMode)>,
}
