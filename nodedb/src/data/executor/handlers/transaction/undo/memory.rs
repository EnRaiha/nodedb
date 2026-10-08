// SPDX-License-Identifier: BUSL-1.1

//! Reverse the in-memory side effects of document writes whose redb
//! transaction drops uncommitted.
//!
//! A dropped redb transaction reverses the row, its btree and FTS entries
//! and its column stats. It does not reverse what the write did in memory:
//! R-tree and `spatial_doc_map` entries, vector nodes and their
//! `vector_doc_map` entries, sparse-vector postings, and the deleted-node
//! marks. `apply_point_put` and `apply_point_delete` return those as undo
//! entries, and an autocommit write that aborts reverses them here, through
//! the undo driver a rolled-back transaction uses.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::materialized_sum::apply::TargetWrite;
use crate::engine::document::store::StorageKey;

use super::UndoEntry;

impl CoreLoop {
    /// Reverse `undo`, last entry first.
    ///
    /// An entry that does not reverse leaves the core's state unknown. The
    /// result is then `RollbackFailed`, and the core fail-stops when that
    /// error leaves it in a response.
    pub(in crate::data::executor) fn undo_memory_effects(
        &mut self,
        database_id: u64,
        tid: u64,
        undo: Vec<UndoEntry>,
    ) -> crate::Result<()> {
        if undo.is_empty() {
            return Ok(());
        }
        self.rollback_undo_log(database_id, tid, undo)
            .map_err(crate::Error::from)
    }

    /// Abandon the target rows a materialized-sum pass wrote into a
    /// transaction that drops uncommitted.
    ///
    /// Each target write is a full document write: it cached its row and can
    /// have in-memory index entries of its own. This drops the cache entries
    /// and reverses the entries, last target first.
    pub(in crate::data::executor) fn abandon_target_writes(
        &mut self,
        database_id: u64,
        tid: u64,
        targets: Vec<TargetWrite>,
    ) -> crate::Result<()> {
        let mut undo = Vec::new();
        for target in targets {
            let key = StorageKey::for_surrogate(target.surrogate);
            self.doc_cache
                .invalidate(database_id, tid, &target.collection, &key);
            undo.extend(target.outcome.memory_undo);
        }
        self.undo_memory_effects(database_id, tid, undo)
    }
}

/// The error an abandoned write reports.
///
/// This is `original`, unless reversing the write's in-memory effects
/// failed. That failure leaves the core's state unknown, so it outranks the
/// error that caused the abort.
pub(in crate::data::executor) fn abort_error(
    original: crate::Error,
    undo: crate::Result<()>,
) -> crate::Error {
    match undo {
        Ok(()) => original,
        Err(fatal) => fatal,
    }
}
