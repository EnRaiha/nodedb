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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn stats_key_str() -> String {
        format!("{DB}:{TID}:c:name")
    }

    /// Serialize a `ColumnStats` built from the given observed values, returning
    /// both the value and its wire bytes (the pre-image shape `StatsRestore` holds).
    fn make_stats(values: &[&str]) -> (crate::engine::sparse::stats::ColumnStats, Vec<u8>) {
        let mut stats = crate::engine::sparse::stats::ColumnStats::new();
        for v in values {
            stats.observe(Some(&serde_json::Value::String((*v).to_string())));
        }
        let bytes = zerompk::to_msgpack_vec(&stats).unwrap();
        (stats, bytes)
    }

    #[test]
    fn stats_restore_undo_rewrites_prior_image() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // Seed the original (pre-op) stats and capture its exact bytes.
        let (original, original_bytes) = make_stats(&["alice"]);
        core.stats_store
            .put(DB, TID, "c", "name", &original)
            .unwrap();

        // Simulate the read-modify-write op having merged another value and
        // committed the mutated stats.
        let (mutated, _) = make_stats(&["alice", "bob"]);
        core.stats_store
            .put(DB, TID, "c", "name", &mutated)
            .unwrap();
        assert_eq!(
            core.stats_store
                .get(DB, TID, "c", "name")
                .unwrap()
                .unwrap()
                .row_count,
            2,
            "mutated stats must be observed before undo"
        );

        // Rollback restores the exact pre-image.
        let undo = UndoEntry::StatsRestore {
            key: stats_key_str(),
            prior: Some(original_bytes),
        };
        core.apply_undo_stats(0, undo).unwrap();

        let restored = core.stats_store.get(DB, TID, "c", "name").unwrap().unwrap();
        assert_eq!(
            restored.row_count, original.row_count,
            "row_count must match pre-image"
        );
        assert_eq!(
            restored.non_null_count, 1,
            "non_null_count must match pre-image"
        );
        assert_eq!(restored.min_value.as_deref(), Some("alice"));
        assert_eq!(
            restored.max_value.as_deref(),
            Some("alice"),
            "'bob' merge must be reversed"
        );
    }

    #[test]
    fn stats_restore_undo_removes_key_when_no_prior() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        // The op created stats for a (coll, field) that had none before.
        let (created, _) = make_stats(&["carol"]);
        core.stats_store
            .put(DB, TID, "c", "name", &created)
            .unwrap();
        assert!(
            core.stats_store
                .get(DB, TID, "c", "name")
                .unwrap()
                .is_some()
        );

        // `prior = None` => undo removes the key entirely.
        let undo = UndoEntry::StatsRestore {
            key: stats_key_str(),
            prior: None,
        };
        core.apply_undo_stats(0, undo).unwrap();

        assert!(
            core.stats_store
                .get(DB, TID, "c", "name")
                .unwrap()
                .is_none(),
            "key with no prior image must be removed on undo"
        );
    }
}
