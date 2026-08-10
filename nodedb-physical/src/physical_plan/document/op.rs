// SPDX-License-Identifier: Apache-2.0

use nodedb_types::{Surrogate, SurrogateBitmap, SystemTimeScope};

use super::merge_types::MergeClauseOp;
use super::ollp_edge::OllpPredictedEdge;
use super::sum_target::ResolvedSumTarget;
use super::timeseries_schema::TimeseriesSchema;
use super::types::{EnforcementOptions, RegisteredIndex, ReturningSpec, StorageMode, UpdateValue};

/// Document engine physical operations (schemaless + strict + DML).
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocumentOp {
    /// Point lookup by document ID.
    PointGet {
        collection: String,
        document_id: String,
        /// Stable cross-engine identity bound to `(collection, document_id)`
        /// in the catalog. The handler hex-encodes this to compute the
        /// substrate row key; user-PK strings are not used for storage
        /// addressing on the document path.
        surrogate: Surrogate,
        /// Raw primary-key bytes, used by follower-side WAL decode to
        /// re-derive the surrogate via the catalog rev table when the
        /// physical plan is reconstructed from the WAL stream.
        pk_bytes: Vec<u8>,
        /// RLS post-fetch filters (serialized `Vec<ScanFilter>`).
        /// If non-empty, the Data Plane evaluates these after fetching
        /// the document. Returns `NOT_FOUND` on denial (no info leak).
        /// Injected by the Control Plane planner from RLS policies.
        rls_filters: Vec<u8>,
        /// System-time selection. `Current` = current state. Honored only by
        /// bitemporal collections; the planner rejects temporal point-gets on
        /// non-bitemporal collections. `AllVersions` is rejected for point-gets.
        system_time: SystemTimeScope,
        /// `FOR VALID_TIME CONTAINS <ms>` filter.
        valid_at_ms: Option<i64>,
    },

    /// Point write: insert/update a document.
    ///
    /// This variant is unconditional-overwrite (upsert semantics). Use
    /// [`DocumentOp::PointInsert`] for SQL `INSERT` where duplicate PKs must
    /// raise `unique_violation`.
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        /// Catalog-bound identity for `(collection, document_id)`.
        /// Hex-encoded by the handler to compute the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes, used by follower-side WAL decode to
        /// re-derive the surrogate via the catalog rev table.
        pk_bytes: Vec<u8>,
        /// When `Some`, return the STORED post-image of the written row
        /// projected per spec — generated columns evaluated, `_rowid`
        /// injected, strict tuple re-decoded. Never the submitted body: an
        /// echo of the request would report what was asked for rather than
        /// what landed.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. The write policy
        /// governs the write; this bounds what may be shown back, so a
        /// `RETURNING` row set never exceeds a `SELECT` by the same principal.
        /// See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. Keyed on the PAIR because one source may drive
        /// two bindings that share a join column and name different targets.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Point insert: write one document, fail on duplicate primary key.
    ///
    /// When `if_absent` is true the handler silently skips conflicts
    /// (`INSERT ... ON CONFLICT DO NOTHING`). When false, a duplicate
    /// primary key raises a unique-violation error.
    ///
    /// Separate from [`DocumentOp::PointPut`] because the write path must
    /// probe the existence of `document_id` inside the same write txn as
    /// the insert — conflating the two routed `INSERT` to silent upsert.
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        if_absent: bool,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner` from `(collection, document_id_bytes)`.
        /// `Surrogate::ZERO` is reserved as a sentinel and only appears
        /// in test fixtures.
        surrogate: Surrogate,
        /// When `Some`, return the STORED post-image of the inserted row
        /// projected per spec — see `PointPut::returning`. A conflict skipped
        /// by `if_absent` inserts nothing and therefore returns no row.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. Keyed on the PAIR because one source may drive
        /// two bindings that share a join column and name different targets.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// Materialized-sum TARGET collections whose delta this write must NOT
        /// apply itself: the Control Plane settled each at plan time and
        /// appended an
        /// [`ApplyBalanceDelta`](DocumentOp::ApplyBalanceDelta) task of its
        /// own, homed on the target's vShard.
        ///
        /// A target that homes elsewhere has no rows on the core this write
        /// lands on, so applying its delta here would write the balance into a
        /// store no reader of the target collection consults — and, once the
        /// appended task runs, count it twice. Empty for every write whose
        /// targets are co-resident, and for every collection with no binding.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
    },

    /// Point delete: remove a document.
    PointDelete {
        collection: String,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// When `Some`, return the pre-deletion document projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits. The write policy
        /// governs the write; this bounds what may be shown back, so a
        /// `RETURNING` row set never exceeds a `SELECT` by the same principal.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Compiled write policy gating the PERSIST, evaluated in the Data
        /// Plane against the row image the statement actually writes — the
        /// pre-image for a delete, the post-image for an update. A row that
        /// fails it fails the whole statement with `RejectedAuthz`; never a
        /// silent skip, which would report a write that did happen as one that
        /// did not. Empty = no write policy restricts this identity here.
        ///
        /// A slot of its own, never an alias of `rls_filters`: that field is
        /// the READ policy bounding what `RETURNING` may show. Conflating the
        /// two would turn a write gate into row redaction, or the reverse.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. Keyed on the PAIR because one source may drive
        /// two bindings that share a join column and name different targets.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Point update: read-modify-write with field-level changes.
    PointUpdate {
        collection: String,
        document_id: String,
        /// Catalog-bound identity for `(collection, document_id)`. The
        /// handler hex-encodes this for the substrate row key.
        surrogate: Surrogate,
        /// Raw primary-key bytes for follower WAL decode rebind.
        pk_bytes: Vec<u8>,
        /// Field name → assignment RHS (literal bytes or row-scope expression).
        updates: Vec<(String, UpdateValue)>,
        /// When `Some`, return the post-update document projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating `returning` — see `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy gating the persist, evaluated against the post-update
        /// image — see `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. Keyed on the PAIR because one source may drive
        /// two bindings that share a join column and name different targets.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Full collection scan with filtering, sorting, and pagination.
    Scan {
        collection: String,
        limit: usize,
        offset: usize,
        sort_keys: Vec<crate::physical_plan::SortKeySpec>,
        /// Filter predicates serialized as JSON.
        filters: Vec<u8>,
        distinct: bool,
        projection: Vec<String>,
        /// Serialized `Vec<ComputedColumn>`.
        computed_columns: Vec<u8>,
        /// Serialized `Vec<WindowFuncSpec>`.
        window_functions: Vec<u8>,
        /// System-time selection. `Current` = current state; `AsOf(ms)` =
        /// point-in-time; `AllVersions` = every system-time version ordered
        /// ascending (audit log). Honored only by collections registered with
        /// bitemporal storage; the planner rejects temporal scans on
        /// non-bitemporal collections at SQL plan time.
        system_time: SystemTimeScope,
        /// `FOR VALID_TIME CONTAINS <ms>` filter. `None` = no filter.
        valid_at_ms: Option<i64>,
        /// Optional surrogate prefilter injected by a cross-engine sub-plan.
        /// When present, the scan skips rows whose surrogate is absent from
        /// this bitmap. `None` = no prefilter; full collection is scanned.
        #[serde(default)]
        prefilter: Option<SurrogateBitmap>,
    },

    /// Batch insert documents in a single redb transaction.
    BatchInsert {
        collection: String,
        /// (document_id, value_bytes) pairs.
        documents: Vec<(String, Vec<u8>)>,
        /// Per-row surrogates (parallel to `documents`). When non-empty and
        /// same length as `documents`, the handler uses these for FTS indexing.
        /// `Surrogate::ZERO` entries are silently skipped by the FTS path.
        surrogates: Vec<nodedb_types::Surrogate>,
        /// When `Some`, return one row per inserted document — the STORED
        /// post-image of each, in `documents` order — projected per spec.
        /// See `PointPut::returning`.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. One entry per DISTINCT pair across `documents`.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
        /// Materialized-sum TARGET collections whose delta this write must NOT
        /// apply itself: the Control Plane settled each at plan time and
        /// appended an
        /// [`ApplyBalanceDelta`](DocumentOp::ApplyBalanceDelta) task of its
        /// own, homed on the target's vShard.
        ///
        /// A target that homes elsewhere has no rows on the core this write
        /// lands on, so applying its delta here would write the balance into a
        /// store no reader of the target collection consults — and, once the
        /// appended task runs, count it twice. Empty for every write whose
        /// targets are co-resident, and for every collection with no binding.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
    },

    /// Range scan on a sparse/metadata index.
    RangeScan {
        collection: String,
        field: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
        limit: usize,
        /// Row-level-security filters applied to fetched rows before they are
        /// returned. This operation has no pushdown filter slot in storage, so
        /// the filters are evaluated post-fetch — the same shape `KvOp::Get`
        /// and `DocumentOp::PointGet` already use.
        #[serde(default)]
        rls_filters: Vec<u8>,
    },

    /// Register collection with secondary indexes and storage mode (DDL).
    Register {
        collection: String,
        /// Full secondary-index specs (name, path, unique, case_insensitive,
        /// state). Replaces the old `Vec<String>` path-only payload so the
        /// write handler can enforce UNIQUE and skip Building indexes.
        indexes: Vec<RegisteredIndex>,
        crdt_enabled: bool,
        /// Storage encoding mode. Determines how documents are serialized.
        storage_mode: StorageMode,
        /// Collection enforcement options propagated from catalog (boxed to reduce enum size).
        enforcement: Box<EnforcementOptions>,
        /// Bitemporal storage: every write becomes a new version keyed by
        /// `system_from_ms`; reads use the versioned table and Ceiling
        /// resolver.
        bitemporal: bool,
        /// Durable CRDT conflict-resolution policy (JSON-serialized
        /// `CollectionPolicy`), persisted on the collection's catalog record.
        /// `Some` rehydrates the per-core `PolicyRegistry` on register/reboot
        /// so `ALTER COLLECTION ... SET ON CONFLICT ...` survives a restart
        /// instead of silently reverting to `CollectionPolicy::ephemeral()`.
        /// `None` = no explicit policy persisted; the registry falls back to
        /// the ephemeral default.
        conflict_policy: Option<String>,
        /// Declared columns + designated `TIME_KEY` for a timeseries
        /// collection. `Some` for every `engine='timeseries'` collection;
        /// `None` for every other engine. The Data Plane builds the
        /// collection's memtable schema from this instead of inferring one
        /// from the first ingested batch.
        timeseries: Option<Box<TimeseriesSchema>>,
        /// Vector-primary access-path config. `Some` for every
        /// `WITH (primary='vector')` collection, `None` for every other.
        ///
        /// The Data Plane read path needs it to know that this collection's
        /// sparse rows are `zerompk` TAGGED metadata sidecars rather than
        /// ordinary document bodies. Both encodings are legal MessagePack
        /// maps, so no inspection of the stored bytes can tell them apart —
        /// the collection's declared kind is the only sound discriminator.
        vector_primary: Option<Box<nodedb_types::VectorPrimaryConfig>>,
    },

    /// Lookup documents by secondary index value.
    IndexLookup {
        collection: String,
        path: String,
        value: String,
    },

    /// Fetch full document rows via a secondary index.
    ///
    /// Emitted from `SqlPlan::DocumentIndexLookup` for SELECT queries where
    /// the WHERE clause has an equality predicate on an indexed field. The
    /// handler resolves doc IDs via `sparse.index_lookup`, fetches each
    /// document, and emits scan-compatible row output via `response_codec`.
    /// `filters` (the compound-predicate residual left over after the
    /// indexed equality) is applied to every fetched body — committed and a
    /// transaction's staged overlay rows alike; `projection` is not yet
    /// applied by the handler.
    ///
    /// Sort / distinct / window functions are handled by the planner
    /// falling back to a full scan — the planner only emits this variant
    /// when none of those are present.
    IndexedFetch {
        collection: String,
        /// Indexed field path (e.g. `$.email`).
        path: String,
        /// Equality lookup value. COLLATE NOCASE rewrites normalize to
        /// lowercase before emission, so the handler does not need to.
        value: String,
        /// Remaining post-filters (serialized `Vec<ScanFilter>`).
        filters: Vec<u8>,
        /// Column names to include in each row (empty = all fields).
        projection: Vec<String>,
        limit: usize,
        offset: usize,
    },

    /// Drop all secondary index entries for a field.
    DropIndex { collection: String, field: String },

    /// Backfill a secondary index from existing collection documents.
    ///
    /// Emitted by CREATE INDEX on a collection that already has rows.
    /// The handler scans every document, extracts the indexed value, and
    /// writes sparse-index entries — atomically detecting UNIQUE
    /// violations along the way. Running this inside a single write
    /// transaction is intentional: it mirrors Postgres's blocking CREATE
    /// INDEX lock semantics and guarantees the index is consistent when
    /// the Ready flip commits.
    BackfillIndex {
        collection: String,
        /// JSON-path-like field (e.g. `$.email`).
        path: String,
        is_array: bool,
        unique: bool,
        case_insensitive: bool,
        /// Partial-index predicate (raw SQL text of the `WHERE` body)
        /// or `None` for full indexes. Rows where the predicate is
        /// false are skipped — not indexed, not UNIQUE-checked.
        #[serde(default)]
        predicate: Option<String>,
    },

    /// Truncate: delete ALL documents in a collection.
    /// If `restart_identity` is true, sequences attached to this collection's
    /// fields are reset to their start value after truncation.
    Truncate {
        collection: String,
        restart_identity: bool,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane from a recon scan of the rows this statement will remove.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in the
        /// catalog redb, which is Control-Plane state. Because the set is
        /// derived from a scan taken before execution, the Data-Plane leader
        /// re-derives the actual join-key set and returns
        /// `ErrorCode::OllpRetryRequired` on divergence BEFORE writing.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Estimate count via HLL cardinality stats.
    EstimateCount { collection: String, field: String },

    /// INSERT ... SELECT: copy documents from source to target.
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },

    /// Upsert: insert or merge. When `on_conflict_updates` is non-empty,
    /// the conflict branch evaluates those assignments against the
    /// *existing* document instead of merging the inserted value —
    /// the `INSERT ... ON CONFLICT DO UPDATE SET ...` path.
    Upsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, UpdateValue)>,
        /// Stable cross-engine identity assigned by the CP-side
        /// `SurrogateAssigner`. `Surrogate::ZERO` only in test fixtures.
        surrogate: Surrogate,
        /// Write policy gating the persist, evaluated against the body actually
        /// stored by whichever branch runs: the insert body when the row is
        /// absent, the merge with the stored row (or the `on_conflict_updates`
        /// result) when it is present. See `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// When `Some`, return the STORED post-image projected per spec: the
        /// merged row on the conflict branch, the inserted row otherwise.
        /// Never the submitted body — on a conflict the caller's values are
        /// only part of what the row ends up holding.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Read filters gating the rows `returning` emits — see
        /// `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane at plan time. Keyed on the PAIR because one source may drive
        /// two bindings that share a join column and name different targets.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in
        /// the catalog redb, which is Control-Plane state.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Update target rows matched by a join with a source collection.
    ///
    /// Execution is two-phase within one Data Plane core:
    /// 1. Scan `source_collection` (all rows).
    /// 2. For each source row, find all target rows where
    ///    `target[target_join_col] == source_row[source_join_col]`.
    /// 3. Build a merged document with source fields qualified as
    ///    `<source_alias>.<field>` and evaluate `updates` against it,
    ///    then write back to the target row.
    UpdateFromJoin {
        target_collection: String,
        source_collection: String,
        /// Qualifier used for source columns in assignment expressions.
        source_alias: String,
        /// Field in the target used for the equi-join.
        target_join_col: String,
        /// Field in the source used for the equi-join.
        source_join_col: String,
        /// SET field assignments; RHS expressions reference the merged document.
        updates: Vec<(String, UpdateValue)>,
        /// Additional WHERE predicates applying only to the target (msgpack).
        target_filters: Vec<u8>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// RESOLVE-ONLY read pass (Control-Plane COMMIT expander). When `true`
        /// the handler runs the same target-scan, join-match, assignment-eval,
        /// and strict-encode logic that produces each matched row's post-image,
        /// but WITHOUT writing, re-indexing, accumulating a write-set, or
        /// emitting events. It returns the matched rows as msgpack
        /// `Vec<(doc_id, Option<surrogate_u32>, post_image_body)>` so the
        /// in-transaction expander can rewrite them into concrete `PointPut`
        /// ops carrying each target row's existing surrogate. `false` is the
        /// normal write path (autocommit / co-resident replay).
        #[serde(default)]
        resolve_only: bool,
        /// Control-Plane-shipped source rows for cross-core `UPDATE ... FROM`.
        /// When `Some`, the handler builds the source join-map from these
        /// pre-scanned `(source_doc_id, raw_stored_source_bytes)` rows INSTEAD
        /// of reading the source collection from local storage. On a multi-core
        /// node the source and target collections can map to different
        /// Data-Plane cores; the source no longer lives in the target core's
        /// local store, so the orchestrator scans the source on its OWN core
        /// (via the source-scan primitive) and ships the rows in here. Each body
        /// is the RAW stored source document (a Binary Tuple for a strict
        /// source, MessagePack for a schemaless source), decoded by the handler
        /// with the same schema-aware logic the local scan uses (the source's
        /// strict schema is present on every core because `Register` is
        /// broadcast), so the resulting join-map is byte-for-byte identical to
        /// the local-read path. `None` = legacy local-read path (co-resident /
        /// in-transaction buffered replay).
        #[serde(default)]
        source_rows: Option<Vec<(String, Vec<u8>)>>,
        /// Read filters gating `returning`, keyed on `target_collection` —
        /// every returned row is a target row. See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy of `target_collection` gating the persist, evaluated
        /// against each matched target row's post-image — every row this op
        /// writes is a target row. See `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// `target_collection`'s materialized-sum bindings, resolved on the
        /// Control Plane from a recon scan of the target rows this statement
        /// will rewrite. Both
        /// sides of a join-key change are resolved, so a row moved from one
        /// sum target to another can be debited and credited in one pass.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in the
        /// catalog redb, which is Control-Plane state. Because the set is
        /// derived from a scan taken before execution, the Data-Plane leader
        /// re-derives the actual join-key set and returns
        /// `ErrorCode::OllpRetryRequired` on divergence BEFORE writing.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Bulk update: scan + apply field updates to all matches.
    BulkUpdate {
        collection: String,
        filters: Vec<u8>,
        updates: Vec<(String, UpdateValue)>,
        /// When `Some`, return updated documents projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Optimistic pre-execution predicted matching surrogates (OLLP path).
        ///
        /// When `Some`, the executor verifies that the actual set of matching
        /// surrogates equals this sorted set before applying any write. On
        /// mismatch the executor returns `ErrorCode::OllpRetryRequired` without
        /// writing. `None` on the non-OLLP (static-set) path — no verification.
        #[serde(default)]
        ollp_predicted_surrogates: Option<Vec<u32>>,
        /// Optimistic pre-execution predicted implicit edges (OLLP path).
        ///
        /// Carried for symmetry/forward-use with `BulkDelete`. Edge-content
        /// drift validation currently runs only on the `BulkDelete` path
        /// (implicit-edge DELETE); the executor leaves this field unused for
        /// `BulkUpdate`. `None` on the non-OLLP path.
        #[serde(default)]
        ollp_predicted_edges: Option<Vec<OllpPredictedEdge>>,
        /// Read filters gating `returning` — see `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy gating the persist, evaluated against each matched
        /// row's post-update image — see `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane from a recon scan of the rows the predicate matches. Both
        /// sides of a
        /// join-key change are resolved, so a row moved from one sum target to
        /// another can be debited and credited in one pass.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in the
        /// catalog redb, which is Control-Plane state. Because the set is
        /// derived from a scan taken before execution, the Data-Plane leader
        /// re-derives the actual join-key set and returns
        /// `ErrorCode::OllpRetryRequired` on divergence BEFORE writing.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Bulk delete: scan + delete all matches.
    BulkDelete {
        collection: String,
        filters: Vec<u8>,
        /// When `Some`, return pre-deletion documents projected per spec.
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// Optimistic pre-execution predicted matching surrogates (OLLP path).
        ///
        /// When `Some`, the executor verifies that the actual set of matching
        /// surrogates equals this sorted set before applying any write. On
        /// mismatch the executor returns `ErrorCode::OllpRetryRequired` without
        /// writing. `None` on the non-OLLP (static-set) path — no verification.
        #[serde(default)]
        ollp_predicted_surrogates: Option<Vec<u32>>,
        /// Optimistic pre-execution predicted implicit edges (OLLP path).
        ///
        /// When `Some`, the executor recomputes the actual edge set
        /// (`(surrogate, _from, _to, _type)` per matched edge doc) from the
        /// stored docs and compares the sorted sets. On ANY divergence it
        /// returns `OllpRetryRequired` BEFORE any write, closing the
        /// recon→execute content TOCTOU on `_from`/`_to`/`_type`. `None` on the
        /// non-OLLP path (no edge-content verification).
        #[serde(default)]
        ollp_predicted_edges: Option<Vec<OllpPredictedEdge>>,
        /// Read filters gating `returning` — see `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy gating the persist, evaluated against each matched
        /// row's pre-deletion image — the only image a delete has. See
        /// `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// this collection's materialized-sum bindings, resolved on the Control
        /// Plane from a recon scan of the rows the predicate matches.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in the
        /// catalog redb, which is Control-Plane state. Because the set is
        /// derived from a scan taken before execution, the Data-Plane leader
        /// re-derives the actual join-key set and returns
        /// `ErrorCode::OllpRetryRequired` on divergence BEFORE writing.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// MERGE: join-based multi-action DML (INSERT / UPDATE / DELETE per WHEN arm).
    ///
    /// Execution:
    /// 1. Build a join map from the source collection keyed by `source_join_col`.
    /// 2. Walk all target rows; for each with a matching source row, find the
    ///    first `Matched` arm whose extra_predicate is satisfied and apply its
    ///    action.
    /// 3. Walk source rows with no matching target row; find the first `NotMatched`
    ///    arm whose extra_predicate is satisfied and apply its action (INSERT).
    /// 4. Optionally, walk target rows with no matching source row; find the first
    ///    `NotMatchedBySource` arm and apply it (UPDATE or DELETE).
    Merge {
        target_collection: String,
        source_collection: String,
        /// Qualifier used for source columns in assignment expressions.
        source_alias: String,
        target_join_col: String,
        source_join_col: String,
        clauses: Vec<MergeClauseOp>,
        #[serde(default)]
        returning: Option<ReturningSpec>,
        /// RESOLVE-ONLY read pass (Control-Plane orchestrator, phase 1). When
        /// `true` the handler classifies the merge WITHOUT writing and returns
        /// the NOT-MATCHED insert rows as msgpack `Vec<(join_key, body)>` so the
        /// orchestrator can allocate a fresh, registered surrogate per inserted
        /// row (surrogate registration is Control-Plane-only). No storage
        /// mutation happens on this pass.
        #[serde(default)]
        resolve_only: bool,
        /// Control-Plane-pre-assigned surrogates for the NOT-MATCHED insert
        /// rows, keyed by source join value (orchestrator phase 3). When
        /// `Some`, the handler runs the ATOMIC apply: it re-derives the insert
        /// set, verifies its join-key set equals these keys — returning
        /// `ErrorCode::OllpRetryRequired` WITHOUT writing on drift (closing the
        /// resolve→apply TOCTOU) — and applies every arm's writes with these
        /// surrogates in ONE redb transaction (matched UPDATE + NOT-MATCHED
        /// INSERT share the txn; a UNIQUE violation rolls back the whole set).
        /// `None` together with `resolve_only == false` is the UNRESOLVED shape
        /// every entry point intercepts on the Control Plane — autocommit via
        /// the merge orchestrator, in-transaction via the statement-time
        /// expander — so it never reaches the Data Plane, which rejects it.
        #[serde(default)]
        resolved_inserts: Option<Vec<(String, u32)>>,
        /// Control-Plane-shipped source rows for cross-core MERGE. When `Some`,
        /// the handler builds the source join-map from these pre-scanned
        /// `(source_doc_id, raw_stored_source_bytes)` rows INSTEAD of reading
        /// the source collection from local storage. On a multi-core node the
        /// source and target collections can map to different Data-Plane cores;
        /// the source no longer lives in the target core's local store, so the
        /// orchestrator scans the source on its OWN core (via the source-scan
        /// primitive) and ships the rows in here. Each body is the RAW stored
        /// source document (a Binary Tuple for a strict source, MessagePack for
        /// a schemaless source), exactly as the local scan would read it; the
        /// handler decodes it with the same schema-aware logic (the source's
        /// strict schema is present on every core because `Register` is
        /// broadcast), so the resulting join-map is byte-for-byte identical to
        /// the local-read path. `None` = legacy local-read path (co-resident /
        /// in-transaction buffered replay).
        #[serde(default)]
        source_rows: Option<Vec<(String, Vec<u8>)>>,
        /// Read filters gating `returning`, keyed on `target_collection` —
        /// every returned row is a target row. See `PointDelete::rls_filters`.
        #[serde(default)]
        rls_filters: Vec<u8>,
        /// Write policy of `target_collection` gating the persist: every arm
        /// writes a target row, gated against the image it stores — post for an
        /// UPDATE/INSERT arm, pre for a DELETE arm. See
        /// `PointDelete::rls_write_check`.
        #[serde(default)]
        rls_write_check: Vec<u8>,
        /// `(target collection, join-key value)` → target row surrogate for
        /// `target_collection`'s materialized-sum bindings, resolved on the
        /// Control Plane from the RESOLVE pass's classification. Every arm
        /// moves the total: an INSERT
        /// arm credits, a DELETE arm debits, an UPDATE arm applies the
        /// difference and, when the arm rewrites the join key, both sides.
        ///
        /// The Data Plane cannot derive this: the PK→surrogate map lives in the
        /// catalog redb, which is Control-Plane state. The APPLY pass already
        /// re-derives its classification and returns
        /// `ErrorCode::OllpRetryRequired` on drift BEFORE writing, which is the
        /// same guard this set relies on.
        #[serde(default)]
        resolved_sum_targets: Vec<ResolvedSumTarget>,
    },

    /// Cursor-paginated raw scan for the clone materializer.
    ///
    /// Returns raw `(document_id, surrogate, value_bytes)` triples plus
    /// next-cursor in one payload so the materializer can drive the scan to
    /// completion in O(N / count) round-trips.  The response payload is
    /// msgpack-encoded as a 2-element array:
    ///   `[ next_cursor: bin,
    ///      entries: [[document_id: str, surrogate: u32, value_bytes: bin], ...] ]`
    /// `next_cursor` is empty when the scan is complete.
    ///
    /// Honors `system_as_of_ms` so the materializer reads the source collection
    /// as-of the clone's `as_of_lsn`. For non-bitemporal collections the field
    /// is ignored and current state is scanned.
    MaterializeScan {
        collection: String,
        cursor: Vec<u8>,
        count: usize,
        system_as_of_ms: Option<i64>,
        // NOTE: clone materialization is a point-in-time snapshot; `AllVersions`
        // does not compose with snapshot clones and is rejected upstream.
    },

    /// Add a signed amount to a materialized-sum balance on a TARGET row.
    ///
    /// A collection homes to one vShard, so a binding's source and target are
    /// generally served by different cores. When they are, the balance write
    /// cannot ride the source write's transaction — that transaction belongs to
    /// the source's core and has no access to the target's rows. The Control
    /// Plane appends this op as a task of its OWN, homed on the target
    /// collection's vShard, exactly as an implicit graph edge is appended and
    /// homed on its source endpoint. The pair then classifies as multi-shard and
    /// commits atomically through Calvin.
    ///
    /// The Data Plane applies it as a read-modify-write through the full
    /// document write path, so the target row gets the same index, statistics
    /// and cache maintenance any other write of that row would get.
    ApplyBalanceDelta {
        /// TARGET collection, db-qualified exactly as every other plan names the
        /// collection it writes — this op's task homes on it.
        collection: String,
        /// Target row's storage key: the hex-encoded surrogate, the key every
        /// reader of that collection uses.
        document_id: String,
        /// Target row's stable cross-engine identity, resolved from the join
        /// value on the Control Plane at plan time.
        surrogate: Surrogate,
        /// The balance column this delta moves.
        column: String,
        /// Signed amount to add, as an exact decimal STRING. Never `f64`: a
        /// balance is precisely the column where 15 significant digits is not
        /// enough, which is also why the stored total is a string.
        delta: String,
        /// Binding's join column, carried so a target that has gone missing
        /// fails with the same typed error the co-resident path raises.
        join_column: String,
        /// Join value that resolved to `surrogate`, carried for the same reason
        /// as `join_column`.
        join_value: String,
    },
}
