// SPDX-License-Identifier: BUSL-1.1

//! Distributed WAL write path — propose writes through Raft, apply after commit.
//!
//! Write flow:
//! 1. Handler serializes write as [`ReplicatedWrite`]
//! 2. Handler proposes to Raft via [`RaftLoop::propose`]
//! 3. Handler registers a waiter in [`ProposeTracker`] keyed by (group_id, log_index)
//! 4. Raft replicates to quorum and commits
//! 5. [`DistributedApplier`] receives committed entries, queues for async execution
//! 6. Background task dispatches each write to the local Data Plane
//! 7. If a waiter exists (leader path), sends the response; otherwise just applies (follower)

/// Type alias for the synchronous Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
/// Works only when the current node is the group leader. Use
/// [`AsyncRaftProposer`] when proposals may originate from non-leader nodes.
pub type RaftProposer =
    dyn Fn(u32, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

/// Type alias for the asynchronous Raft propose callback with leader forwarding.
///
/// Takes `(vshard_id, idempotency_key, serialized_entry)` and returns the Data
/// Plane apply payload bytes on success. The `idempotency_key` matches the one
/// embedded in the serialized `ReplicatedEntry`; the proposer registers the
/// tracker waiter with this key so apply-side mismatch detection can surface
/// `RetryableLeaderChange` when a new leader's entry overwrites this one.
pub type AsyncRaftProposer = dyn Fn(
        u32,
        u64,
        Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<Vec<u8>, crate::Error>> + Send>,
    > + Send
    + Sync;

/// Type alias for the Raft log-compaction callback.
///
/// Takes `(group_id, applied_index)` where `applied_index` is the index the
/// DATA-PLANE state machine has durably applied to (NOT raft's commit
/// index). Invoked from the apply-completion path so a log can only be
/// compacted up to an index the engines have actually persisted — never
/// past it, which would corrupt a rebuilt snapshot. Returns `true` when a
/// compaction was performed. A no-op when the group's
/// `log_compaction_threshold` is `None`.
pub type RaftCompactor = dyn Fn(u64, u64) -> std::result::Result<bool, crate::Error> + Send + Sync;

fn default_pq_m() -> usize {
    crate::engine::vector::index_config::DEFAULT_PQ_M
}
fn default_ivf_cells() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_CELLS
}
fn default_ivf_nprobe() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_NPROBE
}

// ── Replicated write envelope ───────────────────────────────────────

/// One edge of an `EdgePutBatch` / `EdgeDeleteBatch` in the cross-node wire
/// shape. Mirrors `nodedb_physical::physical_plan::BatchEdge` but carries the
/// endpoint surrogates as `u32` (not the `Surrogate` newtype) so the payload
/// uses only trivially serializable types, exactly like the single `EdgePut`
/// variant. Followers bind both surrogates verbatim on apply (never
/// re-allocate), so the same `src_id`/`dst_id` resolves to the same identity
/// on every replica.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedBatchEdge {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    /// Leader-assigned global surrogate for the source node (binding key =
    /// `src_id.as_bytes()`).
    pub src_surrogate: u32,
    /// Leader-assigned global surrogate for the destination node (binding key =
    /// `dst_id.as_bytes()`).
    pub dst_surrogate: u32,
}

/// Whether a `ConstraintChange` installs (`Set`) or removes (`Drop`) a
/// collection's constraint set on every replica.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ConstraintChangeOp {
    Set,
    Drop,
}

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
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        ttl_ms: u64,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
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
    },
    KvIncrFloat {
        collection: String,
        key: Vec<u8>,
        delta: f64,
    },
    KvCas {
        collection: String,
        key: Vec<u8>,
        expected: Vec<u8>,
        new_value: Vec<u8>,
    },
    KvGetSet {
        collection: String,
        key: Vec<u8>,
        new_value: Vec<u8>,
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
    ConstraintChange {
        collection: String,
        op: ConstraintChangeOp,
        constraints: Vec<Vec<u8>>,
    },
}

/// Metadata carried alongside the write for routing on the receiving node.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReplicatedEntry {
    pub tenant_id: u64,
    pub vshard_id: u32,
    /// Per-proposal idempotency key. Generated by the proposer when the
    /// entry is constructed and embedded in the Raft log payload so the
    /// apply path can match the entry that actually committed at a given
    /// `(group_id, log_index)` against the entry the proposer is waiting
    /// for. A mismatch means a leader change overwrote the proposer's
    /// reservation with a different proposer's entry — the apply path
    /// surfaces `RetryableLeaderChange` so the gateway re-proposes.
    ///
    /// Zero is reserved as "no key" (legacy / synthetic entries that
    /// pre-date the field). The tracker treats `0` as a wildcard to
    /// preserve backwards compatibility with any in-flight log on
    /// upgrade.
    pub idempotency_key: u64,
    pub write: ReplicatedWrite,
}

impl ReplicatedEntry {
    /// Construct a new `ReplicatedEntry` with a freshly generated
    /// idempotency key. All production write paths go through this
    /// constructor; only deserialized log entries skip it (the key is
    /// preserved through `from_bytes`).
    pub fn new(tenant_id: u64, vshard_id: u32, write: ReplicatedWrite) -> Self {
        // OR with 1 so the LSB is set: zero is reserved as the "no key"
        // sentinel and a fresh `rand::random::<u64>()` could in principle
        // hit zero (P ~= 2^-64 but cheap to make impossible).
        let idempotency_key = rand::random::<u64>() | 1;
        Self {
            tenant_id,
            vshard_id,
            idempotency_key,
            write,
        }
    }

    /// Serialize to bytes for Raft log entry data.
    pub fn to_bytes(&self) -> Vec<u8> {
        zerompk::to_msgpack_vec(self).expect("ReplicatedEntry serialization cannot fail")
    }

    /// Deserialize from Raft log entry data bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        zerompk::from_msgpack(data).ok()
    }
}
