// SPDX-License-Identifier: BUSL-1.1

//! Column-statistics undo entry application logic.
//!
//! Column stats live in the `COLUMN_STATS` redb table and are updated
//! READ-MODIFY-WRITE by `observe_document_in_txn`. Because each transaction
//! sub-plan commits its own per-row redb write txn, an aborted redb txn does
//! NOT reverse a stats mutation a prior sub-plan already committed — so undo
//! must explicitly restore the captured pre-image (mirroring the vector/spatial
//! undo paths, which reverse side-effects an aborted redb txn leaves behind).
//!
//! Returns `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;

use super::UndoEntry;

impl CoreLoop {
    pub(super) fn apply_undo_stats(
        &mut self,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::StatsRestore { key, prior } => {
                // Restore the exact pre-image via the stats store's own write
                // txn, reusing the same COLUMN_STATS table and key that the
                // forward observe produced. `Some(bytes)` rewrites the prior
                // stats; `None` removes a key the forward op created.
                self.stats_store
                    .restore(&key, prior.as_deref())
                    .map_err(|e| {
                        let detail = format!("stats restore {key}: {e}");
                        error!(
                            core = self.core_id,
                            entry_index,
                            error = %detail,
                            "transaction undo: column stats restore failed; shard state unknown"
                        );
                        (entry_index, detail)
                    })
            }
            _ => unreachable!("apply_undo_stats called with non-stats entry"),
        }
    }
}
