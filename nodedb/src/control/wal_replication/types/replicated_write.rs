// SPDX-License-Identifier: BUSL-1.1

//! The [`ReplicatedWrite`] enum — a write operation serialized for Raft
//! replication. Mirrors the write variants of `PhysicalPlan` but uses only
//! types that are trivially serializable (no `Arc`, no `Instant`).

use super::aliases::{default_ivf_cells, default_ivf_nprobe, default_pq_m};
use super::wire_shapes::{ConstraintChangeOp, ReplicatedBatchEdge};
use nodedb_types::{PayloadIndexKind, VectorQuantization, VectorStorageDtype};

/// A write operation serialized for Raft replication.
///
/// Mirrors the write variants of [`PhysicalPlan`] but uses only types that
/// are trivially serializable (no `Arc`, no `Instant`).
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReplicatedWrite {
    PointPut {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        /// Leader-assigned global surrogate, carried verbatim so every
        /// replica binds the same identity to `document_id` (binding key
        /// = `document_id.as_bytes()`) instead of re-allocating.
        surrogate: u32,
    },
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        #[serde(default)]
        if_absent: bool,
        /// Leader-assigned global surrogate (binding key = `document_id`).
        surrogate: u32,
    },
    PointDelete {
        collection: String,
        document_id: String,
        /// Leader-assigned global surrogate (binding key = `document_id`).
        surrogate: u32,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        /// Leader-assigned global surrogate (binding key = `document_id`).
        surrogate: u32,
    },
    /// `UPSERT INTO` semantics (insert if absent, else merge/`ON CONFLICT DO
    /// UPDATE SET ...`). `value` is the would-be-inserted document, not a
    /// precomputed merge result -- replay re-runs the read-modify-write
    /// deterministically on the follower using `on_conflict_updates`, same
    /// as `KvInsertOnConflictUpdate` / `PointUpdate`.
    DocUpsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        /// Leader-assigned global surrogate (binding key = `document_id`).
        surrogate: u32,
    },
    /// Multi-document batch insert (`DocumentOp::BatchInsert`) — the shape the
    /// autocommit `INSERT ... SELECT` orchestrator emits for the copied rows.
    ///
    /// `documents` are the `(document_id, value_bytes)` pairs to insert;
    /// `surrogates` are the leader-assigned global identities parallel to
    /// `documents` (same order and length), carried verbatim so every replica
    /// binds the same identity to each `document_id` instead of re-allocating,
    /// exactly like `KvBatchPut`. No `ttl_ms` / `resolved_now_ms`: documents
    /// carry no TTL. Re-applying this entry is idempotent under exactly-once,
    /// LSN-ordered Raft apply: each row lands via `apply_point_put` keyed by its
    /// carried surrogate, so a replayed batch overwrites the identical rows.
    DocBatchInsert {
        collection: String,
        documents: Vec<(String, Vec<u8>)>,
        /// Leader-assigned global surrogate per document (binding key =
        /// `document_id` bytes), same order and length as `documents`.
        surrogates: Vec<u32>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        #[serde(default)]
        field_name: String,
        /// Leader-assigned global surrogate, carried verbatim. Bound on
        /// apply by `pk_bytes` when `Some`, else by the surrogate's own
        /// self-key (`as_u32().to_be_bytes()`) — never re-allocated.
        surrogate: u32,
        /// User PK bytes (UTF-8 of the document id) when the insert
        /// originates from a PK-bearing path; `None` for headless inserts.
        #[serde(default)]
        pk_bytes: Option<Vec<u8>>,
        /// Sync provenance encoded as zerompk bytes. `None` for non-sync
        /// inserts. Followers decode this back to `SyncProvenance` so the
        /// idempotency gate runs identically on every replica, advancing
        /// the same high-water mark as the leader.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
        /// Leader-assigned global surrogates, parallel to `vectors`. Each
        /// is bound on apply by its own self-key — never re-allocated.
        surrogates: Vec<u32>,
    },
    VectorDelete {
        collection: String,
        vector_id: u32,
    },
    SetVectorParams {
        collection: String,
        #[serde(default)]
        field_name: String,
        m: usize,
        ef_construction: usize,
        metric: String,
        #[serde(default)]
        index_type: String,
        #[serde(default = "default_pq_m")]
        pq_m: usize,
        #[serde(default = "default_ivf_cells")]
        ivf_cells: usize,
        #[serde(default = "default_ivf_nprobe")]
        ivf_nprobe: usize,
    },
    /// Insert a document into a sparse vector's inverted index. Keyed by
    /// `doc_id` (not a surrogate) — the sparse inverted index is a separate
    /// structure from the HNSW/surrogate identity space, so there is no
    /// surrogate to carry. No WAL LSN field: nothing here is a per-node
    /// watermark.
    SparseInsert {
        collection: String,
        field_name: String,
        doc_id: String,
        entries: Vec<(u32, f32)>,
    },
    /// Remove a document from a sparse vector's inverted index. Same
    /// doc_id-keyed identity as `SparseInsert`; no surrogate, no WAL LSN.
    SparseDelete {
        collection: String,
        field_name: String,
        doc_id: String,
    },
    /// Insert N vectors for one document (ColBERT-style), all bound to a
    /// single shared surrogate. `document_surrogate` is the leader-assigned
    /// global identity, carried verbatim so every replica binds the SAME
    /// surrogate to all `count` vectors instead of each re-allocating its
    /// own. No WAL LSN field.
    MultiVectorInsert {
        collection: String,
        field_name: String,
        document_surrogate: u32,
        /// Flat vector data: `count` * `dim` f32 values.
        vectors: Vec<f32>,
        count: usize,
        dim: usize,
    },
    /// Tombstone all vectors for a document from the multi-vector index.
    /// `document_surrogate` is the leader-assigned identity of the document
    /// being deleted, carried verbatim so every replica resolves the exact
    /// same set of vectors. No WAL LSN field.
    MultiVectorDelete {
        collection: String,
        field_name: String,
        document_surrogate: u32,
    },
    /// Soft-delete a vector by surrogate (sync inbound path / in-transaction
    /// delete). `surrogate` is carried verbatim — it identifies an
    /// already-bound HNSW node, so no re-binding is needed on apply. No WAL
    /// LSN field.
    DeleteBySurrogate {
        collection: String,
        surrogate: u32,
        field_name: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// Direct vector upsert for vector-primary collections (`WITH
    /// (primary='vector')`) — the SQL DML path bypassing MessagePack document
    /// encoding. `surrogate` is the leader-assigned global identity, carried
    /// verbatim so every replica binds the same identity instead of
    /// re-allocating. `payload` / `quantization` / `storage_dtype` /
    /// `payload_indexes` are carried in full so a follower's HNSW insert +
    /// payload bitmap update reproduce the leader's write byte-for-byte. No
    /// WAL LSN field.
    DirectUpsert {
        collection: String,
        field: String,
        surrogate: u32,
        vector: Vec<f32>,
        /// Pre-encoded MessagePack of only the payload-indexed fields.
        payload: Vec<u8>,
        quantization: VectorQuantization,
        storage_dtype: VectorStorageDtype,
        payload_indexes: Vec<(String, PayloadIndexKind)>,
    },
    CrdtApply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Sync provenance encoded as zerompk bytes. `None` for non-sync
        /// applies. Followers decode this back to `SyncProvenance` so the
        /// idempotency gate runs identically on every replica, advancing
        /// the same high-water mark as the leader.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
        /// The collection's constraint-set version (`constraint_version`)
        /// this delta was admitted against (stamped by the leader at
        /// admission from the catalog). Followers carry it verbatim so the
        /// apply-time write-gate runs identically on every replica. `0`
        /// means "no fence" (gate open).
        #[serde(default)]
        constraint_version_required: u64,
    },
    /// A columnar batch insert from a Lite peer, to be applied on all replicas.
    ///
    /// `surrogates` are the leader-assigned global identities (in row order),
    /// carried verbatim so followers use exactly the same values. `wal_lsn` is
    /// omitted — followers allocate their own WAL LSN at apply time. `provenance`
    /// is zerompk-encoded `SyncProvenance` so the idempotency gate runs
    /// identically on every replica.
    ColumnarIngest {
        collection: String,
        /// Row data in MessagePack format.
        payload: Vec<u8>,
        /// MessagePack-serialized `ColumnarSchema` from the DDL catalog.
        schema_bytes: Vec<u8>,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// A timeseries ingest from a Lite peer, to be applied on all replicas.
    ///
    /// `surrogates` are the leader-assigned global identities. `wal_lsn` is
    /// omitted — followers allocate their own WAL LSN at apply time.
    TimeseriesIngest {
        collection: String,
        payload: Vec<u8>,
        /// "ilp" or "samples".
        format: String,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// Index a document into the FTS inverted index, from a Lite peer.
    ///
    /// `surrogate` is the leader-assigned global identity, carried verbatim
    /// so followers insert the same row identity into the local FTS index.
    FtsIndex {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        /// Concatenated text to index.
        text: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// Remove a document from the FTS inverted index, from a Lite peer.
    FtsDelete {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// Insert a geometry into the spatial R-tree index, from a Lite peer.
    ///
    /// `surrogate` is the leader-assigned global identity. `geometry` is
    /// zerompk-encoded `Geometry` to avoid a typed dependency in this crate.
    SpatialInsert {
        collection: String,
        field: String,
        /// Leader-assigned global surrogate for the row.
        surrogate: u32,
        /// zerompk-encoded `nodedb_types::geometry::Geometry`.
        geometry_bytes: Vec<u8>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// Remove a geometry from the spatial R-tree index, from a Lite peer.
    SpatialDelete {
        collection: String,
        field: String,
        /// Leader-assigned global surrogate for the row.
        surrogate: u32,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    EdgePut {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        properties: Vec<u8>,
        /// Leader-assigned global surrogate for the source node (binding key =
        /// `src_id.as_bytes()`), carried verbatim so every replica binds the
        /// same identity instead of re-allocating.
        src_surrogate: u32,
        /// Leader-assigned global surrogate for the destination node (binding
        /// key = `dst_id.as_bytes()`).
        dst_surrogate: u32,
    },
    EdgeDelete {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        /// Leader-assigned global surrogate for the source node (binding key =
        /// `src_id.as_bytes()`), carried verbatim so every replica resolves the
        /// same identity and dual-homes the tombstone like the matching put.
        src_surrogate: u32,
        /// Leader-assigned global surrogate for the destination node (binding
        /// key = `dst_id.as_bytes()`).
        dst_surrogate: u32,
    },
    /// Set bitset-based labels on a graph node. Operates directly on the
    /// `node_id` string and allocates no surrogate, so no surrogate is
    /// carried — every replica applies the same labels to the same node.
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    /// Remove bitset-based labels from a graph node. Surrogate-free for the
    /// same reason as `SetNodeLabels`.
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    /// Batched edge insert (e.g. `CREATE GRAPH INDEX` materializing one edge
    /// per parent→child relation). Each edge carries its endpoint surrogates
    /// verbatim so every replica binds the same identities — never
    /// re-allocates — exactly like the single `EdgePut` variant.
    EdgePutBatch {
        edges: Vec<ReplicatedBatchEdge>,
    },
    /// Batched edge delete (rollback of a partial `EdgePutBatch`). Mirrors
    /// `EdgePutBatch`'s surrogate handling.
    EdgeDeleteBatch {
        edges: Vec<ReplicatedBatchEdge>,
    },
    KvPut {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Leader-assigned global surrogate (binding key = `key` raw bytes).
        surrogate: u32,
        /// The proposal-time-resolved absolute wall-clock instant (ms since
        /// epoch) for this write's `expire_at_ms`. Carried so every replica
        /// -- including the proposer itself, which installs its effect only
        /// through the Raft apply loop, never by executing the write locally
        /// before commit -- derives a byte-identical TTL expiry instead of
        /// each replica reading its own clock. `None` when `ttl_ms == 0`
        /// (no expiry is derived).
        resolved_now_ms: Option<u64>,
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
    /// SQL `INSERT` semantics (write only if absent). Mirrors `PointInsert`:
    /// carries the same fields `KvOp::Insert` does, and replay re-runs the
    /// duplicate check deterministically on the follower rather than
    /// replicating a precomputed outcome.
    KvInsert {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        /// Leader-assigned global surrogate (binding key = `key` raw bytes).
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    /// SQL `INSERT ... ON CONFLICT DO NOTHING` semantics. Replay re-runs the
    /// existence check deterministically on the follower, same as `KvInsert`.
    KvInsertIfAbsent {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    /// SQL `INSERT ... ON CONFLICT (key) DO UPDATE SET ...` semantics.
    /// `value` is the would-be-inserted row (`EXCLUDED`), not a precomputed
    /// merge result -- replay re-runs the read-modify-write deterministically
    /// on the follower using `updates`, same as `PointUpdate`.
    KvInsertOnConflictUpdate {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        ttl_ms: u64,
        /// Leader-assigned global surrogate per entry (binding key = entry
        /// key raw bytes), same order and length as `entries`.
        surrogates: Vec<u32>,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// See `KvPut::resolved_now_ms`. Always `Some` here: unlike the
        /// `Put` family, `ttl_ms == 0` on `EXPIRE` means "expire now", not
        /// "no TTL", so there is no "no expiry to derive" case to represent.
        resolved_now_ms: Option<u64>,
    },
    KvPersist {
        collection: String,
        key: Vec<u8>,
    },
    KvIncr {
        collection: String,
        key: Vec<u8>,
        delta: i64,
        ttl_ms: u64,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    KvIncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
        surrogate: u32,
    },
    KvCas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
        surrogate: u32,
    },
    KvGetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
        surrogate: u32,
    },
    KvRegisterSortedIndex {
        collection: String,
        index_name: String,
        sort_columns: Vec<(String, String)>,
        key_column: String,
        window_type: String,
        window_timestamp_column: String,
        window_start_ms: u64,
        window_end_ms: u64,
    },
    KvDropSortedIndex {
        index_name: String,
    },
    /// HSET-style read-modify-write field merge. Replay re-runs the merge
    /// deterministically on the follower against its own current value,
    /// same as `KvInsertOnConflictUpdate` / `PointUpdate`.
    KvFieldSet {
        collection: String,
        key: Vec<u8>,
        updates: Vec<(String, Vec<u8>)>,
        /// Leader-assigned cross-engine surrogate for the merged row. The
        /// follower binds this exact identity via `bind_or_lookup`.
        surrogate: u32,
    },
    /// Atomic fungible transfer (`source.field -= amount`, `dest.field +=
    /// amount`). Replay re-runs the read-validate-write deterministically on
    /// the follower against its own current source/dest values -- no
    /// precomputed balances are carried, matching the "replay recomputes"
    /// contract every other RMW `KvOp` variant follows here.
    KvTransfer {
        collection: String,
        source_key: Vec<u8>,
        dest_key: Vec<u8>,
        field: String,
        amount: f64,
        /// Leader-assigned surrogate of the debit (source) row.
        debit_surrogate: u32,
        /// Leader-assigned surrogate of the credit (dest) row.
        credit_surrogate: u32,
    },
    /// Atomic non-fungible item transfer (verify + delete + insert). Replay
    /// re-runs the same verify-then-move on the follower.
    KvTransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        /// Leader-assigned surrogate of the moved row at its destination.
        surrogate: u32,
    },
    /// An array CRDT op (Put or Delete) from a Lite peer, to be applied via
    /// the distributed applier on all replicas.
    ///
    /// `op_bytes` is the raw zerompk encoding of the `ArrayOp` as produced by
    /// `nodedb_array::sync::op_codec::encode_op`.
    /// `schema_hlc_bytes` carries the 18-byte HLC from the op header so the
    /// applier can perform the authoritative idempotency check.
    ArrayOp {
        array: String,
        op_bytes: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
        /// Sync provenance (zerompk-encoded `SyncProvenance`), `None` for
        /// legacy / unidentified producers. Carried so the epoch fence runs
        /// identically on every replica's apply path — a stale-epoch array
        /// producer is fenced cluster-wide and across leader failover, not
        /// just on the node that first received the op.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    /// An array schema CRDT snapshot from a Lite peer.
    ///
    /// `snapshot_payload` is the raw Loro export bytes as received in
    /// `ArraySchemaSyncMsg`.
    ArraySchema {
        array: String,
        snapshot_payload: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
    },

    /// A single-shard bulk predicate write (`BulkDelete` / `BulkUpdate`) to be
    /// replicated to the data group's Raft members and re-executed on apply.
    ///
    /// A single shard is ONE Raft group: proposing the bulk write as a log entry
    /// makes every replica apply it in log order, so re-evaluating the predicate
    /// at the apply position yields the byte-identical matching set on every
    /// replica (deterministic by Raft ordering). No OLLP / optimistic-lock
    /// machinery is required — that exists only to coordinate the matching set
    /// ACROSS independent Raft groups (≥2 vshards). On apply the bulk handler
    /// runs in its plain (non-OLLP) mode: `ollp_predicted_surrogates = None`, so
    /// it deletes/updates exactly the locally-scanned matches.
    ///
    /// `filters` is the msgpack-encoded `Vec<ScanFilter>` predicate (empty = no
    /// WHERE clause, match all). `updates` is empty for `BulkDelete` and carries
    /// the SET assignments for `BulkUpdate`. `is_update` disambiguates the two so
    /// apply reconstructs the correct `DocumentOp`. No surrogate sidecar is
    /// carried: the apply re-scans local state and re-derives matches by
    /// predicate, and cascade cleanup keys off each matched row's existing
    /// surrogate (identical on every replica).
    BulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
    },

    /// A single-shard columnar predicate write (`ColumnarOp::Delete` /
    /// `ColumnarOp::Update`) to be replicated to the data group's Raft members
    /// and re-executed on apply. The columnar sibling of [`Self::BulkDml`]:
    /// a single shard is ONE Raft group, so re-evaluating the predicate at the
    /// committed apply position re-derives the byte-identical matching set on
    /// every replica (deterministic by Raft ordering). Columnar predicate DML
    /// carries no OLLP path, so no predicted-surrogate machinery is involved.
    ///
    /// `filters` is the msgpack-encoded `Vec<ScanFilter>` predicate (empty = no
    /// WHERE clause, match all). `updates` is empty for the delete and carries
    /// the `(column_name, msgpack_value_bytes)` SET assignments for the update
    /// (the same raw-bytes shape `ColumnarOp::Update` carries — distinct from
    /// `BulkDml`'s `UpdateValue`); `is_update` disambiguates the two so apply
    /// reconstructs the correct `ColumnarOp`.
    ColumnarBulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, Vec<u8>)>,
    },

    /// A single-shard `INSERT INTO <target> SELECT ... FROM <source> WHERE
    /// <predicate>` to be replicated to the data group's Raft members and
    /// re-executed on apply. Like `BulkDml`, a single shard is ONE Raft group,
    /// so re-scanning the source at the committed log position yields the
    /// byte-identical copied set on every replica (deterministic by Raft
    /// ordering); the copied rows reuse each source row's own surrogate/doc_id.
    ///
    /// `source_filters` is the msgpack-encoded `Vec<ScanFilter>` predicate on
    /// the source (empty = no WHERE clause, match all); `source_limit` bounds
    /// the copied set. The affected collection is `target_collection`.
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },

    /// Per-collection Loro snapshot import. Used by RESTORE to durably
    /// replicate a collection's CRDT doc to all Raft replicas of a data group
    /// (replacing the race-prone per-node direct dispatch). `bytes` is one
    /// collection's `TenantCrdtEngine::export_snapshot_bytes(collection)`
    /// output; on apply every replica calls `import_snapshot_bytes` (a
    /// monotonic, idempotent Loro merge — deterministic across replicas).
    CrdtImportCollection {
        tenant_id: u64,
        collection: String,
        bytes: Vec<u8>,
    },

    /// Insert a block (LoroMap) into a document's block list
    /// (`CrdtOp::ListInsert`). Carries the operation's **intent** — not a
    /// Loro delta — because the delta can only be computed by re-running the
    /// live `execute_crdt_list_insert` handler against each replica's own
    /// Loro state (mirrors the `CrdtListOpWalRecord` WAL payload's replay
    /// contract). No surrogate field: the parent document's surrogate is
    /// re-resolved from `document_id` on decode via `Surrogate::ZERO` /
    /// assigner lookup, matching every other decode arm — the live dispatch
    /// handler ignores the `CrdtOp::ListInsert::surrogate` field entirely, so
    /// carrying it across the wire would be dead weight.
    CrdtListInsert {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
        /// JSON-encoded field map for the inserted block.
        fields_json: String,
    },

    /// Delete a block from a document's block list (`CrdtOp::ListDelete`).
    /// Same intent-carrying replay contract as `CrdtListInsert`.
    CrdtListDelete {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
    },

    /// Move a block within a document's block list, reordering it
    /// (`CrdtOp::ListMove`). `from_index` and `to_index` are two distinct
    /// required fields — never an `Option<u64>` pair a truncated record
    /// could decode with one silently defaulting to the other's value.
    CrdtListMove {
        collection: String,
        document_id: String,
        list_path: String,
        from_index: u64,
        to_index: u64,
    },

    /// Dependent-read result broadcast for a Calvin txn.
    ///
    /// A passive participant proposes this entry to the per-vshard Raft group
    /// after reading its declared keys. Active participants on all replicas
    /// receive this entry via the apply loop, look up the pending dependent
    /// barrier for `(epoch, position)`, and — once all passive vshards have
    /// delivered — assemble `injected_reads` and dispatch
    /// `MetaOp::CalvinExecuteActive`.
    ///
    /// **One entry per (passive_vshard, txn_id)**: all read values for a
    /// single passive participant for a single txn are batched here.  A txn
    /// reading 10K keys from one shard produces one Raft entry, not 10K.
    ///
    /// `values` is msgpack-encoded `Vec<(PassiveReadKeyId, Value)>`.
    CalvinReadResult {
        /// Sequencer epoch the transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// The vshard that performed the read.
        passive_vshard: u32,
        /// Tenant scope.
        tenant_id: u64,
        /// Msgpack-encoded `Vec<(PassiveReadKeyId, Value)>`.
        values: Vec<u8>,
    },

    /// Carries a collection's constraint set onto the per-vshard data Raft
    /// log so every replica installs the same constraints deterministically.
    ///
    /// `constraints` is an opaque list of zerompk-encoded constraint blobs:
    /// this crate stays decoupled from the constraint wire layout (mirroring
    /// the opaque-payload precedent of `op_bytes` / `geometry_bytes`) and
    /// never interprets the bytes. `op` selects whether the set is installed
    /// (`Set`) or removed (`Drop`) for `collection`.
    ///
    /// `constraint_version` is the collection's derived constraint-set
    /// version (bumped only when the constraint set itself changes — NOT the
    /// catalog descriptor version, which bumps on every unrelated ALTER) at
    /// the time the change was proposed. The apply path uses it as a monotonic
    /// fence: a replica installs (or drops) only when the incoming version is
    /// `>=` the version it last installed for the collection, so a stale set
    /// re-proposed by a partitioned leader cannot clobber a newer one even if
    /// it lands at a higher data-log index.
    ConstraintChange {
        collection: String,
        op: ConstraintChangeOp,
        constraint_version: u64,
        constraints: Vec<Vec<u8>>,
    },
}
