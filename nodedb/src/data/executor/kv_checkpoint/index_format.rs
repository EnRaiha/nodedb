// SPDX-License-Identifier: BUSL-1.1

//! On-disk types for the index registrations a KV checkpoint carries alongside
//! its rows.
//!
//! ## Why the registrations must ride in the same file as the rows
//!
//! A registration's only durable record is its `kv_register_index` /
//! `kv_register_sorted_index` WAL record. A published checkpoint installs a
//! replay floor that gates those records out, and truncation then deletes the
//! segments holding them. So rows and registrations have to become durable in
//! the same atomically published generation: any arrangement that could publish
//! one without the other is a restart that comes back with rows and no index.
//!
//! ## Why these are primitives rather than the engine's own types
//!
//! `SortedIndexDef` is not `Serialize` — it owns a `SortKeyEncoder` and a
//! `WindowConfig`. Rather than derive serde onto engine internals (which would
//! freeze their in-memory shape into an on-disk contract), the checkpoint stores
//! exactly the primitive fields the `kv_register_sorted_index` WAL record
//! already carries, and rebuilds the def on load through the same
//! `build_sorted_index_def` the live registration and the WAL replay use. Every
//! field of `SortedIndexDef` is covered: `name`, `collection` and `key_column`
//! directly; `encoder` as the ordered `(column, direction)` list it is
//! constructed from; `window` as its type, timestamp column and — for a custom
//! window — its exact bounds. Nothing is dropped.

use serde::{Deserialize, Serialize};

/// One `(key, primary_keys)` bucket of a secondary index's B-Tree.
///
/// `key` is the indexed field's value bytes for a single-field index, and the
/// already-built composite key for a composite one. Composite keys are stored
/// built, not as the field values they came from: the builder joins values with
/// `\0`, so splitting one back apart is ambiguous for any value containing a
/// null byte.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointIndexEntry {
    /// Index key bytes.
    pub key: Vec<u8>,
    /// Primary keys filed under that key.
    pub primary_keys: Vec<Vec<u8>>,
}

/// A single-field secondary index registration plus its content.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointFieldIndex {
    /// Indexed field name.
    pub field: String,
    /// Field position in the schema column list. Held as `u64` rather than the
    /// engine's `usize` so the file does not encode the writer's pointer width.
    pub field_position: u64,
    /// The index's whole B-Tree. Stored rather than rebuilt from the restored
    /// rows because a `backfill=false` registration deliberately omits the rows
    /// that predate it — content is a function of the write history, and
    /// re-deriving it would silently turn a partial index into a full one.
    pub entries: Vec<KvCheckpointIndexEntry>,
}

/// A composite secondary index registration plus its content.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointCompositeIndex {
    /// Indexed field names, in index order.
    pub fields: Vec<String>,
    /// Field positions in the schema column list, parallel to `fields`.
    pub field_positions: Vec<u64>,
    /// The index's whole B-Tree, keyed by built composite key.
    pub entries: Vec<KvCheckpointIndexEntry>,
}

/// One column of a sorted index's composite sort key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointSortColumn {
    /// Column name.
    pub name: String,
    /// `"ASC"` or `"DESC"`, the same spelling the WAL record carries. Validated
    /// against that closed set on load rather than defaulted, so a corrupted
    /// direction cannot silently invert an index's order.
    pub direction: String,
}

/// One `(sort_key, primary_key)` pair of a sorted index's order-statistic tree.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointSortedEntry {
    /// Encoded sort key.
    pub sort_key: Vec<u8>,
    /// The primary key it ranks.
    pub primary_key: Vec<u8>,
}

/// A sorted index registration plus its content.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointSortedIndex {
    /// Index name, unique per `(database, tenant)`.
    pub name: String,
    /// Collection the index covers.
    pub collection: String,
    /// Column used as the index's primary key.
    pub key_column: String,
    /// The sort key's columns, in order.
    pub sort_columns: Vec<KvCheckpointSortColumn>,
    /// `""`, `"DAILY"`, `"WEEKLY"`, `"MONTHLY"` or `"CUSTOM"` — the same
    /// spellings the WAL record carries, validated on load.
    pub window_type: String,
    /// Timestamp column the window filters on (empty when unwindowed).
    pub window_timestamp_column: String,
    /// Inclusive start of a `CUSTOM` window; `0` otherwise.
    pub window_start_ms: u64,
    /// Exclusive end of a `CUSTOM` window; `0` otherwise.
    pub window_end_ms: u64,
    /// The index's whole tree, in sort order. Stored rather than rebuilt because
    /// the engine's two sort-key extraction paths (registration backfill and
    /// live PUT maintenance) do not agree on every column type, so a rebuild
    /// would rewrite the keys of an index built by the other path.
    pub entries: Vec<KvCheckpointSortedEntry>,
}

/// Every index registration on one collection.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) struct KvCheckpointIndexes {
    /// Single-field secondary indexes.
    pub fields: Vec<KvCheckpointFieldIndex>,
    /// Composite secondary indexes.
    pub composites: Vec<KvCheckpointCompositeIndex>,
    /// Sorted (order-statistic) indexes.
    pub sorted: Vec<KvCheckpointSortedIndex>,
}
