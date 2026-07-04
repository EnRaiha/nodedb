// SPDX-License-Identifier: BUSL-1.1

//! Document-engine undo entry application logic.
//!
//! `apply_undo_document` handles document-engine undo entries. All methods
//! return `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use tracing::error;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

use super::UndoEntry;

impl CoreLoop {
    // ── Document ────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_document(
        &mut self,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::PutDocument {
                collection,
                document_id,
                surrogate,
                old_value,
                bitemporal_sys_from_ms,
                bitemporal_index_tuples,
                secondary_index_added,
                secondary_index_removed,
                chain_hash_prior,
            } => {
                if let Some(sys_from_ms) = bitemporal_sys_from_ms {
                    // Bitemporal op: never wrote the non-versioned table, so
                    // physically remove the appended version row (+ its index
                    // entries) instead of a plain put/delete. `versioned_get_current`
                    // recomputes from the remaining rows, so removing the newest
                    // version restores the prior one automatically.
                    self.undo_bitemporal_write(
                        tid,
                        entry_index,
                        &collection,
                        &document_id,
                        sys_from_ms,
                        &bitemporal_index_tuples,
                    )?;
                } else {
                    let result = if let Some(old) = old_value {
                        self.sparse
                            .put(
                                crate::types::DatabaseId::DEFAULT.as_u64(),
                                tid,
                                &collection,
                                &document_id,
                                &old,
                            )
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    } else {
                        self.sparse
                            .delete(
                                crate::types::DatabaseId::DEFAULT.as_u64(),
                                tid,
                                &collection,
                                &document_id,
                            )
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    };
                    result.map_err(|e| {
                        error!(
                            core = self.core_id,
                            entry_index,
                            collection = %collection,
                            document_id = %document_id,
                            error = %e,
                            "transaction undo: document restore failed; shard state unknown"
                        );
                        (
                            entry_index,
                            format!("document restore on {collection}/{document_id}: {e}"),
                        )
                    })?;
                }
                // Reverse plain secondary-index mutations: undo the inserts and
                // restore the stale entries this put removed. Empty on the
                // bitemporal path (its index reversal happened in
                // `undo_bitemporal_write` above), so this is a no-op there.
                self.undo_secondary_index(
                    tid,
                    entry_index,
                    &collection,
                    &document_id,
                    &secondary_index_added,
                    &secondary_index_removed,
                )?;
                // Revert inverted index — best-effort; FTS index inconsistency is
                // recoverable via re-index, unlike primary store inconsistency.
                let _ = self.inverted.remove_document(
                    crate::types::DatabaseId::DEFAULT.as_u64(),
                    crate::types::TenantId::new(tid),
                    &collection,
                    surrogate,
                );
                // Evict any cached copy of the reversed document. Always safe:
                // a stale hit would otherwise resurrect a rolled-back put; the
                // worst case here is a cache miss.
                self.doc_cache.invalidate(
                    crate::types::DatabaseId::DEFAULT.as_u64(),
                    tid,
                    &collection,
                    &document_id,
                );
                self.undo_chain_hash(tid, &collection, chain_hash_prior);
                Ok(())
            }
            UndoEntry::DeleteDocument {
                collection,
                document_id,
                old_value,
                bitemporal_sys_from_ms,
                bitemporal_index_tuples,
                secondary_index_tuples,
                chain_hash_prior,
            } => {
                if let Some(sys_from_ms) = bitemporal_sys_from_ms {
                    self.undo_bitemporal_write(
                        tid,
                        entry_index,
                        &collection,
                        &document_id,
                        sys_from_ms,
                        &bitemporal_index_tuples,
                    )?;
                } else {
                    self.sparse
                        .put(
                            crate::types::DatabaseId::DEFAULT.as_u64(),
                            tid,
                            &collection,
                            &document_id,
                            &old_value,
                        )
                        .map(|_| ())
                        .map_err(|e| {
                            error!(
                                core = self.core_id,
                                entry_index,
                                collection = %collection,
                                document_id = %document_id,
                                error = %e,
                                "transaction undo: document re-insert failed; shard state unknown"
                            );
                            (
                                entry_index,
                                format!("document re-insert on {collection}/{document_id}: {e}"),
                            )
                        })?;
                }
                // Restore the plain secondary-index entries the forward delete
                // cascade removed. Empty on the bitemporal path (no plain
                // INDEXES entries there), so this is a no-op for it.
                self.undo_secondary_index(
                    tid,
                    entry_index,
                    &collection,
                    &document_id,
                    &[],
                    &secondary_index_tuples,
                )?;
                // Evict any cached copy of the reversed document (see the
                // PutDocument branch): reversing a delete restores the row, so a
                // stale post-delete cache entry must not linger.
                self.doc_cache.invalidate(
                    crate::types::DatabaseId::DEFAULT.as_u64(),
                    tid,
                    &collection,
                    &document_id,
                );
                self.undo_chain_hash(tid, &collection, chain_hash_prior);
                Ok(())
            }
            _ => unreachable!("apply_undo_document called with non-document entry"),
        }
    }

    /// Physically reverse a bitemporal versioned write inside a single
    /// caller-owned redb write transaction: remove the version/tombstone row
    /// appended at `sys_from_ms`, plus every versioned index entry written at
    /// the same system time. redb is single-writer, so all removals share one
    /// transaction.
    fn undo_bitemporal_write(
        &self,
        tid: u64,
        entry_index: usize,
        collection: &str,
        document_id: &str,
        sys_from_ms: i64,
        index_tuples: &[(String, String)],
    ) -> Result<(), (usize, String)> {
        let database_id = crate::types::DatabaseId::DEFAULT.as_u64();
        let map_err = |stage: &str, e: String| {
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %e,
                "transaction undo: bitemporal version removal failed; shard state unknown"
            );
            (
                entry_index,
                format!("bitemporal {stage} on {collection}/{document_id}: {e}"),
            )
        };
        let txn = self
            .sparse
            .db()
            .begin_write()
            .map_err(|e| map_err("begin_write", e.to_string()))?;
        self.sparse
            .versioned_remove_in_txn(&txn, database_id, tid, collection, document_id, sys_from_ms)
            .map_err(|e| map_err("version remove", e.to_string()))?;
        for (field, value) in index_tuples {
            self.sparse
                .versioned_index_remove_in_txn(
                    &txn,
                    VersionedIndexEntry {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        field,
                        value,
                        doc_id: document_id,
                        sys_from_ms,
                    },
                )
                .map_err(|e| map_err("index remove", e.to_string()))?;
        }
        txn.commit().map_err(|e| map_err("commit", e.to_string()))?;
        Ok(())
    }

    /// Reverse plain (non-bitemporal) secondary-index mutations from a
    /// rolled-back document write.
    ///
    /// `to_remove` were INSERTED on the forward path → delete them; `to_restore`
    /// were REMOVED on the forward path (stale UPDATE entries, or a DELETE's
    /// cascade) → re-insert them. Fatal on failure like the primary-store
    /// restore, so a partial index rollback surfaces as `RollbackFailed` rather
    /// than silently diverging the secondary index from the primary store.
    fn undo_secondary_index(
        &self,
        tid: u64,
        entry_index: usize,
        collection: &str,
        document_id: &str,
        to_remove: &[(String, String)],
        to_restore: &[(String, String)],
    ) -> Result<(), (usize, String)> {
        let database_id = crate::types::DatabaseId::DEFAULT.as_u64();
        let map_err = |stage: &str, e: String| {
            error!(
                core = self.core_id,
                entry_index,
                collection = %collection,
                document_id = %document_id,
                error = %e,
                "transaction undo: secondary-index reversal failed; shard state unknown"
            );
            (
                entry_index,
                format!("secondary-index {stage} on {collection}/{document_id}: {e}"),
            )
        };
        for (field, value) in to_remove {
            self.sparse
                .index_remove(database_id, tid, collection, field, value, document_id)
                .map_err(|e| map_err("remove", e.to_string()))?;
        }
        for (field, value) in to_restore {
            self.sparse
                .index_put(database_id, tid, collection, field, value, document_id)
                .map_err(|e| map_err("restore", e.to_string()))?;
        }
        Ok(())
    }

    /// Reverse a hash-chain mutation performed by a document write. `None` =
    /// the op never touched the chain; `Some(None)` = remove the key (genesis
    /// insert); `Some(Some(prev))` = restore the key to its pre-image.
    fn undo_chain_hash(
        &mut self,
        tid: u64,
        collection: &str,
        chain_hash_prior: Option<Option<String>>,
    ) {
        match chain_hash_prior {
            None => {}
            Some(None) => {
                self.chain_hashes
                    .remove(&(crate::types::TenantId::new(tid), collection.to_string()));
            }
            Some(Some(prev)) => {
                self.chain_hashes.insert(
                    (crate::types::TenantId::new(tid), collection.to_string()),
                    prev,
                );
            }
        }
    }
}
