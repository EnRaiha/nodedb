// SPDX-License-Identifier: BUSL-1.1

//! Undo log types and rollback logic for transaction batches.

pub(super) mod apply;
pub(super) mod balanced;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TenantId;

/// Tracks a write operation for rollback purposes.
pub(in crate::data::executor) enum UndoEntry {
    /// Undo a PointPut by deleting the document (or restoring the old value).
    PutDocument {
        collection: String,
        /// Hex-encoded surrogate (the redb storage key).
        document_id: String,
        /// Numeric surrogate for FTS index rollback.
        surrogate: nodedb_types::Surrogate,
        /// `None` if the document didn't exist before (inserted); `Some(bytes)`
        /// if it was overwritten (updated).
        old_value: Option<Vec<u8>>,
    },
    /// Undo a PointDelete by re-inserting the document.
    DeleteDocument {
        collection: String,
        /// Hex-encoded surrogate (the redb storage key).
        document_id: String,
        old_value: Vec<u8>,
    },
    /// Undo a VectorInsert by soft-deleting the inserted vector.
    InsertVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
    },
    /// Undo a VectorDelete by un-deleting (clearing tombstone).
    DeleteVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
    },
    /// Undo an EdgePut by deleting the edge (or restoring old properties).
    PutEdge {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        /// `None` if edge didn't exist before (inserted); `Some(bytes)` if overwritten.
        old_properties: Option<Vec<u8>>,
    },
    /// Undo an EdgeDelete by re-inserting the edge with its old properties.
    DeleteEdge {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        old_properties: Vec<u8>,
    },
    /// Undo a KV write (Put / Insert / InsertIfAbsent / InsertOnConflictUpdate /
    /// FieldSet / Incr / IncrFloat / Cas / GetSet) by restoring the prior value.
    ///
    /// `prior_value == None` means the key did not exist before — undo deletes it.
    /// `prior_value == Some(bytes)` means the key was overwritten — undo restores it.
    ///
    /// The KV hash table preserves existing non-ZERO surrogate bindings on `put`,
    /// so passing `Surrogate::ZERO` during undo is safe: the original surrogate
    /// remains bound in the entry.
    KvPut {
        collection: String,
        key: Vec<u8>,
        prior_value: Option<Vec<u8>>,
    },
    /// Undo a KV Delete by restoring one key's prior value.
    ///
    /// One entry per key that was actually deleted. If a batch delete removed
    /// N keys, N `KvDelete` entries are pushed.
    KvDelete {
        collection: String,
        key: Vec<u8>,
        prior_value: Vec<u8>,
    },
    /// Undo a KV BatchPut by restoring prior values for all affected keys.
    ///
    /// Each element is `(key, prior_value)` where `prior_value == None`
    /// means the key was newly inserted.
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
    /// Undo a KV Transfer (fungible) by restoring source and destination prior values.
    KvTransfer {
        collection: String,
        source_key: Vec<u8>,
        source_prior: Vec<u8>,
        dest_key: Vec<u8>,
        dest_prior: Option<Vec<u8>>,
    },
    /// Undo a KV TransferItem by restoring source and destination prior values.
    KvTransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        source_prior: Vec<u8>,
        dest_prior: Option<Vec<u8>>,
    },
    /// Undo a columnar insert by rolling back in-memory state.
    ///
    /// `row_count_before` is the memtable row count snapshot taken before the
    /// insert. `inserted_pks` are the PK bytes of each newly appended row (for
    /// PK index cleanup). `displaced` are `(pk_bytes, prior_location)` pairs for
    /// rows that were tombstoned by an upsert (their PK index entries must be
    /// restored and their tombstone bits cleared).
    ColumnarInsert {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
        row_count_before: usize,
        inserted_pks: Vec<Vec<u8>>,
        displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    /// Undo a timeseries ingest by truncating the in-memory columnar memtable.
    TimeseriesIngest {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
        row_count_before: u64,
    },
}

impl CoreLoop {
    /// Roll back completed writes in reverse order.
    ///
    /// Returns `Ok(())` if all undo entries were applied successfully.
    ///
    /// Returns `Err((entry_index, detail))` on the first undo failure —
    /// the entry index is the original forward-order position of the failed
    /// entry (before reversal). On failure the caller **must** return a
    /// `RollbackFailed` error to the client; the shard state is unknown
    /// and requires a restart to restore consistency via WAL replay.
    pub(super) fn rollback_undo_log(
        &mut self,
        did: u64,
        tid: u64,
        undo_log: Vec<UndoEntry>,
    ) -> Result<(), (usize, String)> {
        let total = undo_log.len();
        for (rev_idx, entry) in undo_log.into_iter().rev().enumerate() {
            // Convert reversed index back to original forward-order index for
            // diagnostics (makes it easier to correlate with the sub-plan that
            // produced this undo entry).
            let original_idx = total.saturating_sub(1 + rev_idx);
            self.apply_undo_entry(did, tid, original_idx, entry)?;
        }
        Ok(())
    }

    /// Apply a single undo entry. Returns `Err((entry_index, detail))` if the
    /// undo cannot be applied — this is a fatal condition: the shard's in-memory
    /// state is now partially rolled back and must not serve writes.
    fn apply_undo_entry(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::PutDocument { .. } | UndoEntry::DeleteDocument { .. } => {
                self.apply_undo_document(tid, entry_index, entry)
            }
            UndoEntry::InsertVector { .. } | UndoEntry::DeleteVector { .. } => {
                self.apply_undo_vector(tid, entry_index, entry)
            }
            UndoEntry::PutEdge { .. } | UndoEntry::DeleteEdge { .. } => {
                self.apply_undo_edge(tid, entry_index, entry)
            }
            UndoEntry::KvPut { .. }
            | UndoEntry::KvDelete { .. }
            | UndoEntry::KvBatchPut { .. }
            | UndoEntry::KvTransfer { .. }
            | UndoEntry::KvTransferItem { .. } => self.apply_undo_kv(did, tid, entry_index, entry),
            UndoEntry::ColumnarInsert { .. } => self.apply_undo_columnar(entry_index, entry),
            UndoEntry::TimeseriesIngest { .. } => self.apply_undo_timeseries(entry_index, entry),
        }
    }
}
