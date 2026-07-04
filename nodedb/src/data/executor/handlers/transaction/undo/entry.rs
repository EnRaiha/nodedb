// SPDX-License-Identifier: BUSL-1.1

//! `UndoEntry` — tracks a single write operation for rollback purposes.

use crate::data::executor::spatial_key::SpatialIndexKey;
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
        /// System-time key of the versioned/tombstone row this op appended on a
        /// bitemporal collection. `None` = plain non-bitemporal op → reverse via
        /// the non-versioned table exactly as before. `Some(t)` = physically
        /// remove the version row at `t` (and skip the plain-table reversal).
        bitemporal_sys_from_ms: Option<i64>,
        /// `(field, value)` pairs whose versioned index entries this op wrote at
        /// `bitemporal_sys_from_ms`. Empty = none.
        bitemporal_index_tuples: Vec<(String, String)>,
        /// `(field, value)` pairs this op INSERTED into the plain secondary
        /// index. Reversed by `index_remove` on undo. Empty = none.
        secondary_index_added: Vec<(String, String)>,
        /// `(field, value)` pairs this op REMOVED from the plain secondary index
        /// (stale entries on UPDATE). Restored by `index_put` on undo. Empty = none.
        secondary_index_removed: Vec<(String, String)>,
        /// Pre-image of `chain_hashes[(tenant, collection)]` before this op
        /// mutated it. Outer `None` = op didn't touch the chain (no-op on undo);
        /// `Some(None)` = no prior entry (genesis insert → remove key on undo);
        /// `Some(Some(prev))` = restore the key to `prev` on undo.
        chain_hash_prior: Option<Option<String>>,
    },
    /// Undo a PointDelete by re-inserting the document.
    DeleteDocument {
        collection: String,
        /// Hex-encoded surrogate (the redb storage key).
        document_id: String,
        old_value: Vec<u8>,
        /// System-time key of the versioned tombstone row this op appended on a
        /// bitemporal collection. `None` = plain op → re-insert via the
        /// non-versioned table as before. `Some(t)` = physically remove the
        /// tombstone row at `t` (and skip the plain-table re-insert).
        bitemporal_sys_from_ms: Option<i64>,
        /// `(field, value)` pairs whose versioned index entries this op wrote at
        /// `bitemporal_sys_from_ms`. Empty = none.
        bitemporal_index_tuples: Vec<(String, String)>,
        /// `(field, value)` pairs the plain secondary-index cascade removed for
        /// this document. Restored by `index_put` on undo, closing the
        /// rolled-back-DELETE secondary-index hole. Empty = none.
        secondary_index_tuples: Vec<(String, String)>,
        /// Pre-image of `chain_hashes[(tenant, collection)]` before this op
        /// mutated it (see [`UndoEntry::PutDocument`] for semantics).
        chain_hash_prior: Option<Option<String>>,
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
    /// Undo a spatial R-tree insert by removing the entry from the per-field
    /// R-tree and deleting its reverse `spatial_doc_map` record.
    ///
    /// `key` is the `(database, tenant, collection, field)` spatial index key;
    /// `entry_id` is the FNV-1a hash of the substrate row key used as the
    /// R-tree entry id.
    SpatialInsert { key: SpatialIndexKey, entry_id: u64 },
    /// Undo a spatial R-tree removal by re-inserting the entry (with its
    /// captured bounding box) into the per-field R-tree and re-populating the
    /// reverse `spatial_doc_map` record.
    ///
    /// `bbox` is the entry's geometry captured BEFORE the forward `delete`
    /// (the R-tree `delete` does not return it); `document_id` is the reverse
    /// map value removed by the forward cascade.
    SpatialDelete {
        key: SpatialIndexKey,
        entry_id: u64,
        bbox: nodedb_types::BoundingBox,
        document_id: String,
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
