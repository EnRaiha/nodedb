// SPDX-License-Identifier: Apache-2.0

//! The KV operation enum — the wire shape and nothing else.

use nodedb_types::Surrogate;

use crate::physical_plan::document::ReturningSpec;

/// KV engine physical operations.
///
/// All operations target a hash-indexed collection with O(1) point lookups.
/// Keys and values are serialized as Binary Tuples.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum KvOp {
    /// Point lookup by primary key. Returns Binary Tuple value or nil.
    Get {
        collection: String,
        key: Vec<u8>,
        /// RLS post-fetch filters. Evaluated after fetching the value.
        /// Returns nil on denial (no info leak).
        rls_filters: Vec<u8>,
        /// Clone snapshot-isolation ceiling: when set, a fetched entry
        /// whose surrogate exceeds this value is treated as not-found.
        /// Populated by the clone resolver when rewriting a target-side
        /// `Get` for delegation to the source database — bindings the
        /// source allocated AFTER the clone's AS-OF must not leak
        /// through.  `None` for normal (non-clone-delegated) gets.
        #[serde(default)]
        surrogate_ceiling: Option<u32>,
    },

    /// Insert or update (RESP SET / SQL UPSERT semantics). Writes a Binary
    /// Tuple value keyed by primary key, overwriting any existing row.
    ///
    /// If the collection has secondary indexes, they are maintained synchronously.
    /// If no secondary indexes, takes the zero-index fast path.
    Put {
        collection: String,
        key: Vec<u8>,
        /// Binary Tuple encoded value (all value columns).
        value: Vec<u8>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner` from `(collection, key)`.
        /// `Surrogate::ZERO` only appears in test fixtures.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image of the written row
        /// projected per spec — the row as `SELECT` would show it, `key`
        /// included. Never the caller's submitted body: an echo of the
        /// request would report what was asked for rather than what landed.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. The write policy
        /// governs the write; this bounds what may be shown back, so a
        /// `RETURNING` row set never exceeds a `SELECT` by the same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT` semantics: write only if the key does not already exist.
    /// Returns `unique_violation` (SQLSTATE 23505, via `NodeDbError`) on
    /// duplicate key. Reserved for the `INSERT` SQL path — RESP `SET` and
    /// `UPSERT` continue to use `Put`.
    Insert {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image of the written row
        /// projected per spec — the row as `SELECT` would show it, `key`
        /// included. Never the caller's submitted body: an echo of the
        /// request would report what was asked for rather than what landed.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. The write policy
        /// governs the write; this bounds what may be shown back, so a
        /// `RETURNING` row set never exceeds a `SELECT` by the same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT ... ON CONFLICT DO NOTHING` semantics: write if the key
    /// does not exist, silently no-op on duplicate. No error on conflict.
    InsertIfAbsent {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image of the written row
        /// projected per spec — the row as `SELECT` would show it, `key`
        /// included. Never the caller's submitted body: an echo of the
        /// request would report what was asked for rather than what landed.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. The write policy
        /// governs the write; this bounds what may be shown back, so a
        /// `RETURNING` row set never exceeds a `SELECT` by the same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` semantics:
    /// write if absent; on duplicate, read-modify-write — apply the
    /// `updates` (which may reference `EXCLUDED.col` on the incoming row)
    /// to the existing value and write the merged result. `value` is the
    /// would-be-inserted row, used both as the write target when absent
    /// and as `EXCLUDED` when the handler evaluates expressions.
    InsertOnConflictUpdate {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        updates: Vec<(String, crate::physical_plan::document::UpdateValue)>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate, evaluated in the Data
        /// Plane against the body actually persisted — the incoming row on the
        /// insert branch, the merge of it with the stored row on the conflict
        /// branch, neither of which exists at plan time. Empty means no write
        /// policy restricts this identity here.
        ///
        /// Distinct from the read-side `rls_filters` slot beside it: that one
        /// bounds what may be shown back, this one bounds what may be written
        /// at all. Never conflate them — a write gate used as row redaction
        /// admits rows it should hide, and the reverse silently drops writes.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// When `Some`, return the STORED post-image projected per spec: the
        /// merged row on the conflict branch, the inserted row otherwise.
        /// Never the submitted body — on a conflict the caller's values are
        /// only part of what the row ends up holding.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Delete by primary key(s). Returns count of keys actually deleted.
    Delete {
        collection: String,
        keys: Vec<Vec<u8>>,
        /// Compiled row-level-security WRITE predicate, evaluated in the Data
        /// Plane against the stored row being removed. Empty means no write
        /// policy restricts this identity here; only a non-empty check makes
        /// the handler read the pre-image at all.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Cursor-based scan with optional filter predicate.
    Scan {
        collection: String,
        /// Opaque cursor from a previous scan. Empty = start from beginning.
        cursor: Vec<u8>,
        /// Maximum entries to return in this batch.
        count: usize,
        /// Optional filter predicates (same format as DocumentScan filters).
        filters: Vec<u8>,
        /// Optional glob pattern for key matching (e.g., "user:*").
        match_pattern: Option<String>,
        /// ORDER BY terms, each an expression, applied to the scan result
        /// before encoding. Empty = unsorted (engine native order).
        #[serde(default)]
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// Clone snapshot-isolation ceiling: when set, scan results
        /// drop entries whose surrogate exceeds this value.  Populated
        /// by the clone resolver when rewriting a target-side scan for
        /// delegation to the source database — bindings the source
        /// allocated AFTER the clone's AS-OF must not leak through.
        /// `None` for normal (non-clone-delegated) scans.
        #[serde(default)]
        surrogate_ceiling: Option<u32>,
    },

    /// Set or update TTL on an existing key.
    Expire {
        collection: String,
        key: Vec<u8>,
        /// TTL in milliseconds from now.
        ttl_ms: u64,
        /// Compiled row-level-security WRITE predicate. The body is unchanged
        /// by a TTL mutation, so the stored row is both the pre- and the
        /// post-image and the Data Plane decides it before touching the
        /// expiry metadata. Empty means no write policy restricts this
        /// identity here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Remove TTL from an existing key (make it persistent).
    Persist {
        collection: String,
        key: Vec<u8>,
        /// Compiled row-level-security WRITE predicate — see `Expire`, which
        /// this mirrors: the row body does not change, so the stored row is
        /// the image the policy decides.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Get remaining TTL for a key without fetching the value.
    ///
    /// Returns JSON `{"ttl_ms": N}` where N is:
    /// - `-2` — key does not exist
    /// - `-1` — key exists but has no TTL (persistent)
    /// - `>= 0` — remaining milliseconds until expiry
    GetTtl { collection: String, key: Vec<u8> },

    /// Batch get: fetch multiple keys in a single bridge round-trip.
    BatchGet {
        collection: String,
        keys: Vec<Vec<u8>>,
        /// Row-level-security filters applied to fetched rows before they are
        /// returned. This operation has no pushdown filter slot in storage, so
        /// the filters are evaluated post-fetch — the same shape `KvOp::Get`
        /// and `DocumentOp::PointGet` already use.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Batch put: insert/update multiple key-value pairs atomically.
    BatchPut {
        collection: String,
        /// `(key, value)` pairs.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        /// Per-key TTL override in milliseconds. 0 = use collection default.
        ttl_ms: u64,
        /// Stable cross-engine identity for each entry, same order and
        /// length as `entries`, assigned by the CP-side `SurrogateAssigner`
        /// from `(collection, key)` -- the same mechanism `Put`/`Insert`
        /// use. `Surrogate::ZERO` only appears in test fixtures.
        #[serde(default)]
        surrogates: Vec<Surrogate>,
        /// When `Some`, return one row per written entry — the STORED
        /// post-image of each, in `entries` order — projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `Put::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Register a secondary index on a value field (DDL).
    ///
    /// Dispatched when `CREATE INDEX idx ON kv_collection (field)` is executed.
    /// If `backfill` is true, scans all existing entries to populate the index.
    RegisterIndex {
        collection: String,
        /// Field name to index (must match a column in the KV schema).
        field: String,
        /// Position of the field in the schema column list.
        field_position: usize,
        /// Whether to backfill the index with existing entries.
        backfill: bool,
    },

    /// Remove a secondary index from a value field (DDL).
    DropIndex { collection: String, field: String },

    /// Extract one or more fields from a key's value (HGET/HMGET).
    ///
    /// Deserializes the stored value, extracts the named fields, and returns
    /// them as a JSON object. O(1) key lookup + field extraction.
    FieldGet {
        collection: String,
        key: Vec<u8>,
        /// Field names to extract.
        fields: Vec<String>,
        /// Row-level-security filters applied to fetched rows before they are
        /// returned. This operation has no pushdown filter slot in storage, so
        /// the filters are evaluated post-fetch — the same shape `KvOp::Get`
        /// and `DocumentOp::PointGet` already use.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Update specific fields in a key's value (HSET).
    ///
    /// Read-modify-write: reads the current value, merges field updates,
    /// writes back. Maintains secondary indexes if any.
    FieldSet {
        collection: String,
        key: Vec<u8>,
        /// Field name → new value (JSON-encoded bytes).
        updates: Vec<(String, Vec<u8>)>,
        /// Stable cross-engine identity, content-addressed on `(collection,
        /// key)` by the CP-side `SurrogateAssigner`. Threaded to the engine
        /// write-back so a row touched by a field merge keeps the same
        /// surrogate its original insert assigned. `Surrogate::ZERO` only in
        /// test fixtures / when no assigner is wired.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate, evaluated against the
        /// merged body — which exists only after the stored row has been read
        /// and the field updates applied. Empty means no write policy
        /// restricts this identity here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Truncate: delete ALL entries in a KV collection.
    Truncate { collection: String },

    /// Atomic increment on a numeric value. Returns new value.
    ///
    /// If key doesn't exist, initializes to 0 then adds delta.
    /// If value is not i64, returns `TypeMismatch`.
    /// On overflow (i64::MAX + 1), returns `OverflowError`.
    /// TTL: if `ttl_ms > 0` and key is new, sets TTL; if key exists, resets TTL.
    /// If `ttl_ms == 0`, preserves existing TTL (no change).
    Incr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        /// TTL in milliseconds. 0 = preserve existing TTL.
        ttl_ms: u64,
        /// Stable cross-engine identity, content-addressed on `(collection,
        /// key)` by the CP-side `SurrogateAssigner`. Threaded to the engine
        /// write-back so a row touched by an atomic op keeps the same
        /// surrogate its original insert assigned. `Surrogate::ZERO` only in
        /// test fixtures / when no assigner is wired.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate. The incremented value
        /// is computed inside the engine, so the engine consults this check
        /// with the computed image before making it durable rather than the
        /// handler guessing the result. Empty means no write policy restricts
        /// this identity here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Atomic float increment on a numeric value. Returns new value.
    ///
    /// Same semantics as `Incr` but for f64 values.
    /// If value is not f64, returns `TypeMismatch`.
    IncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate — see `Incr`, whose
        /// engine-internal compute-and-persist this mirrors.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Compare-and-swap: set value to `new_value` only if current equals `expected`.
    ///
    /// Returns JSON `{"success": bool, "current_value": "<base64>"}`.
    /// If key doesn't exist and `expected` is empty, creates the key (create-if-not-exists).
    Cas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate, evaluated against
        /// `new_value` before the swap is attempted. Empty means no write
        /// policy restricts this identity here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Atomic get-and-set: set new value, return old value.
    ///
    /// Returns the previous value (or null if key didn't exist).
    GetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
        /// Stable cross-engine identity. `Surrogate::ZERO` only in tests.
        surrogate: Surrogate,
        /// Row-level-security READ filters applied to the OLD value this op
        /// hands back. The reply is a row body, so a row the read policy hides
        /// must come back absent rather than being disclosed by the write that
        /// replaced it.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Compiled row-level-security WRITE predicate, evaluated against
        /// `new_value` before the swap. Never an alias of `rls_filters`: one
        /// decides what may be shown, the other what may be written.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    // ── Atomic Transfer Operations ───────────────────────────────────
    /// Atomic fungible transfer: read-validate-write in one Data Plane pass.
    ///
    /// Reads source and dest values, validates source.field >= amount,
    /// then atomically writes both updated values. No TOCTOU race.
    Transfer {
        collection: String,
        source_key: Vec<u8>,
        dest_key: Vec<u8>,
        field: String,
        /// Amount to transfer (encoded as f64 bytes).
        amount: f64,
        /// Stable cross-engine identity of the debit (source) row, content-
        /// addressed on `(collection, source_key)`. Threaded to the source
        /// write-back so the debited row keeps its surrogate. `Surrogate::ZERO`
        /// only in test fixtures / when no assigner is wired.
        debit_surrogate: Surrogate,
        /// Stable cross-engine identity of the credit (dest) row, content-
        /// addressed on `(collection, dest_key)`. Threaded to the dest
        /// write-back so the credited row keeps its surrogate. Distinct from
        /// `debit_surrogate` so the two rows never collapse onto one identity.
        credit_surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate for the collection both
        /// rows live in. Both post-images — the debited source and the credited
        /// dest — are decided against it before either is persisted, so a
        /// transfer cannot half-apply. Empty means no write policy restricts
        /// this identity here.
        #[serde(default)]
        rls_write_check: Vec<u8>,
    },

    /// Atomic non-fungible item transfer: verify + delete + insert in one pass.
    ///
    /// Verifies source owns the item, then atomically deletes from source
    /// and inserts at dest. Fails with NotFound if source doesn't own it.
    TransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        /// Stable cross-engine identity of the moved row at its destination,
        /// content-addressed on `(dest_collection, dest_key)`. Threaded to the
        /// dest write-back so the inserted row carries its surrogate.
        /// `Surrogate::ZERO` only in test fixtures / when no assigner is wired.
        surrogate: Surrogate,
        /// Compiled row-level-security WRITE predicate of the SOURCE
        /// collection, decided against the row being removed from it.
        #[serde(default)]
        source_rls_write_check: Vec<u8>,
        /// Compiled row-level-security WRITE predicate of the DESTINATION
        /// collection, decided against the same bytes as the row being
        /// inserted there. Kept separate from the source check because the two
        /// collections carry independent policies — one identity may be
        /// allowed to give a row up but not to receive it.
        #[serde(default)]
        dest_rls_write_check: Vec<u8>,
    },

    // ── Sorted Index (Leaderboard) Operations ──────────────────────────
    /// Register a sorted index on a KV collection (DDL).
    RegisterSortedIndex {
        collection: String,
        index_name: String,
        /// Sort columns: (column_name, direction "ASC"/"DESC").
        sort_columns: Vec<(String, String)>,
        /// Primary key column name.
        key_column: String,
        /// Window type: "none", "daily", "weekly", "monthly", or "custom".
        window_type: String,
        /// Window timestamp column (empty if window_type == "none").
        window_timestamp_column: String,
        /// Custom window start (ms since epoch, 0 if N/A).
        window_start_ms: u64,
        /// Custom window end (ms since epoch, 0 if N/A).
        window_end_ms: u64,
    },

    /// Drop a sorted index.
    DropSortedIndex { index_name: String },

    /// Get the 1-based rank of a key in a sorted index.
    SortedIndexRank {
        index_name: String,
        primary_key: Vec<u8>,
    },

    /// Get the top K entries from a sorted index.
    SortedIndexTopK { index_name: String, k: u32 },

    /// Get entries in a score range from a sorted index.
    SortedIndexRange {
        index_name: String,
        score_min: Option<Vec<u8>>,
        score_max: Option<Vec<u8>>,
    },

    /// Get total count of entries in a sorted index.
    SortedIndexCount { index_name: String },

    /// Get the sort key (score) for a key in a sorted index (ZSCORE equivalent).
    SortedIndexScore {
        index_name: String,
        primary_key: Vec<u8>,
    },

    /// Cursor-paginated raw scan for the clone materializer.
    ///
    /// Unlike `Scan`, this returns raw `(key, value)` byte pairs **plus** the
    /// next-cursor in a single payload, so the materializer can drive the
    /// scan to completion in O(N / count) round-trips. The response payload
    /// is msgpack-encoded as a 2-element array:
    ///   `[ next_cursor: bytes, entries: [[key: bytes, value: bytes], ...] ]`
    /// `next_cursor` is empty when the scan is complete.
    MaterializeScan {
        collection: String,
        cursor: Vec<u8>,
        count: usize,
    },
}
