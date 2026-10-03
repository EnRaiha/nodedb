// SPDX-License-Identifier: BUSL-1.1

//! Shared handle for driving and awaiting clone materialization.
//!
//! A `CloneMaterializerHandle` is a bounded condvar-backed rendezvous
//! between the DDL command that requests materialization and the background
//! walker tasks. Callers can either fire-and-forget (`notify_start`) or
//! block until completion (`wait_until_done`). The channel is bounded so the
//! waiting path never allocates unboundedly.

use std::sync::{Arc, Condvar, Mutex};

use nodedb_types::DatabaseId;

/// Internal state of a single materialization rendezvous slot.
#[derive(Debug, Default)]
struct Slot {
    /// How many collections remain unfinished for the tracked database.
    remaining: usize,
    /// Set when the slot has been initialised.
    started: bool,
    /// Set once `remaining` drops to zero after a prior `start`.
    done: bool,
}

/// A shared, waitable completion handle for clone materialization of one database.
///
/// Intended use-cases:
///   1. Background scheduler: calls `notify_start(n)` once it begins N collections,
///      then calls `notify_collection_done()` for each that completes.
///   2. `ALTER DATABASE … MATERIALIZE` / `DROP DATABASE … FORCE`: pass the handle
///      to the awaited `force_materialize`, which drives it. `wait_until_done()`
///      blocks its thread, so only a thread outside the async runtime calls it.
#[derive(Debug)]
pub struct CloneMaterializerHandle {
    db_id: DatabaseId,
    inner: Arc<(Mutex<Slot>, Condvar)>,
}

impl CloneMaterializerHandle {
    /// Create a new handle for `db_id`.
    pub fn new(db_id: DatabaseId) -> Self {
        Self {
            db_id,
            inner: Arc::new((Mutex::new(Slot::default()), Condvar::new())),
        }
    }

    /// Returns the database this handle tracks.
    pub fn database_id(&self) -> DatabaseId {
        self.db_id
    }

    /// Clone the inner `Arc` so a second owner (e.g. the scheduler) can
    /// drive the slot while the DDL caller waits on it.
    pub fn clone_arc(&self) -> Self {
        Self {
            db_id: self.db_id,
            inner: Arc::clone(&self.inner),
        }
    }

    /// Initialise the slot with `collection_count` pending completions.
    ///
    /// Must be called exactly once before any `notify_collection_done`.
    /// Re-calling after the slot is already started is a no-op (idempotent
    /// so re-enqueued materializations after a crash restart are safe).
    pub fn notify_start(&self, collection_count: usize) {
        let (lock, cvar) = &*self.inner;
        let mut slot = lock.lock().unwrap_or_else(|p| p.into_inner());
        if slot.started {
            return;
        }
        slot.started = true;
        slot.remaining = collection_count;
        if collection_count == 0 {
            slot.done = true;
            cvar.notify_all();
        }
    }

    /// Mark one collection as fully materialized; wakes waiters if all done.
    pub fn notify_collection_done(&self) {
        let (lock, cvar) = &*self.inner;
        let mut slot = lock.lock().unwrap_or_else(|p| p.into_inner());
        if slot.remaining > 0 {
            slot.remaining -= 1;
        }
        if slot.started && slot.remaining == 0 {
            slot.done = true;
            cvar.notify_all();
        }
    }

    /// Block the calling thread until all collections are materialized.
    ///
    /// Returns immediately if already done.  Safe to call from
    /// `tokio::task::spawn_blocking` wrappers — never blocks the async runtime.
    pub fn wait_until_done(&self) {
        let (lock, cvar) = &*self.inner;
        let mut slot = lock.lock().unwrap_or_else(|p| p.into_inner());
        while !slot.done {
            slot = cvar.wait(slot).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Returns `true` if materialization has completed.
    pub fn is_done(&self) -> bool {
        let (lock, _) = &*self.inner;
        let slot = lock.lock().unwrap_or_else(|p| p.into_inner());
        slot.done
    }
}
