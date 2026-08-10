// SPDX-License-Identifier: BUSL-1.1
//! Append-only Raft write wire ABI; variants must never be reordered.

use super::aliases::{default_ivf_cells, default_ivf_nprobe, default_pq_m};
use super::wire_shapes::{ConstraintChangeOp, ReplicatedBatchEdge, ReplicatedSumTarget};
use nodedb_types::{PayloadIndexKind, VectorQuantization, VectorStorageDtype};

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
        surrogate: u32,
        /// The materialized-sum resolution the PROPOSING node performed: each
        /// join-key VALUE this write's rows carry, paired with the surrogate of
        /// the target row that value names.
        ///
        /// Carried on the wire for the same reason `KvPut::resolved_now_ms` is:
        /// it is resolved once, by the node that accepted the statement, and no
        /// applying node may re-derive it. The pk → surrogate map lives in the
        /// catalog of the vShard that OWNS the target's primary key
        /// (`lookup_surrogate_routed` routes the probe to that vShard's leader),
        /// so an applier holding only the SOURCE vShard's replica cannot answer
        /// the question locally, and answering it remotely would put a network
        /// round-trip — and another node's clock of committed state — inside the
        /// apply loop. Shipping the answer makes the follower's derived total
        /// identical to the leader's by construction.
        ///
        /// Empty for every write whose collection drives no binding, which is
        /// nearly all of them.
        ///
        /// SUPERSEDED by `resolved_sum_target_bindings`, and retained only
        /// because this enum is an append-only wire ABI: a peer running an
        /// older binary reads this slot and behaves exactly as it does today.
        /// New appliers read it only when `resolved_sum_target_bindings` is
        /// empty — see there.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// The same resolution, keyed on the `(target collection, join value)`
        /// PAIR — the authoritative slot.
        ///
        /// `resolved_sum_targets` above cannot express the answer when a source
        /// drives two bindings that read the SAME join column into DIFFERENT
        /// target collections: one entry per join value means the second
        /// binding's fold finds the first binding's target row and writes its
        /// balance there. Both stored totals are then wrong and nothing reports
        /// it.
        ///
        /// Appended rather than replacing the older slot, which is what this
        /// wire ABI does with every added field. A record written before this
        /// field existed carries an empty vec here and a populated vec above;
        /// the decoder then lifts the older slot as untargeted entries, which
        /// match any binding by value alone — exactly what that record meant
        /// when it was written, and what lets a node replay its own committed
        /// log across the upgrade.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    PointInsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        #[serde(default)]
        if_absent: bool,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// Target collections whose delta the proposing node split onto its own
        /// `ApplyBalanceDelta` entry because the target homes to a different
        /// vShard.
        ///
        /// Travels with the write for the same reason the resolution does, and
        /// with sharper consequences: an applier that did not read this list
        /// would fold the delta locally AND receive the sibling entry, counting
        /// the same amount twice.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    PointDelete {
        collection: String,
        document_id: String,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    PointUpdate {
        collection: String,
        document_id: String,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    DocUpsert {
        collection: String,
        document_id: String,
        value: Vec<u8>,
        on_conflict_updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        surrogate: u32,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    DocBatchInsert {
        collection: String,
        documents: Vec<(String, Vec<u8>)>,
        surrogates: Vec<u32>,
        /// See `PointPut::resolved_sum_targets`.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointInsert::deferred_sum_targets`.
        #[serde(default)]
        deferred_sum_targets: Vec<String>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    VectorInsert {
        collection: String,
        vector: Vec<f32>,
        dim: usize,
        #[serde(default)]
        field_name: String,
        surrogate: u32,
        #[serde(default)]
        pk_bytes: Option<Vec<u8>>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    VectorBatchInsert {
        collection: String,
        vectors: Vec<Vec<f32>>,
        dim: usize,
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
        #[serde(default)]
        dim: usize,
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
    SparseInsert {
        collection: String,
        field_name: String,
        doc_id: String,
        entries: Vec<(u32, f32)>,
    },
    SparseDelete {
        collection: String,
        field_name: String,
        doc_id: String,
    },
    MultiVectorInsert {
        collection: String,
        field_name: String,
        document_surrogate: u32,
        vectors: Vec<f32>,
        count: usize,
        dim: usize,
    },
    MultiVectorDelete {
        collection: String,
        field_name: String,
        document_surrogate: u32,
    },
    DeleteBySurrogate {
        collection: String,
        surrogate: u32,
        field_name: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    DirectUpsert {
        collection: String,
        field: String,
        surrogate: u32,
        vector: Vec<f32>,
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
        #[serde(default)]
        provenance: Option<Vec<u8>>,
        /// Leader-admitted constraint fence; zero means no fence.
        #[serde(default)]
        constraint_version_required: u64,
    },
    ColumnarIngest {
        collection: String,
        payload: Vec<u8>,
        schema_bytes: Vec<u8>,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    TimeseriesIngest {
        collection: String,
        payload: Vec<u8>,
        format: String,
        /// Leader-assigned global surrogates, parallel to the rows in `payload`.
        surrogates: Vec<u32>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    FtsIndex {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        text: String,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    FtsDelete {
        collection: String,
        /// Leader-assigned global surrogate for the document.
        surrogate: u32,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    SpatialInsert {
        collection: String,
        field: String,
        /// Leader-assigned global surrogate for the row.
        surrogate: u32,
        geometry_bytes: Vec<u8>,
        /// Sync provenance encoded as zerompk bytes.
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
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
        /// Leader-assigned source surrogate.
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
        /// Leader-assigned source surrogate.
        src_surrogate: u32,
        /// Leader-assigned global surrogate for the destination node (binding
        /// key = `dst_id.as_bytes()`).
        dst_surrogate: u32,
    },
    SetNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    RemoveNodeLabels {
        node_id: String,
        labels: Vec<String>,
    },
    EdgePutBatch {
        edges: Vec<ReplicatedBatchEdge>,
    },
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
        /// Leader-resolved expiry clock; `None` when `ttl_ms == 0`.
        resolved_now_ms: Option<u64>,
    },
    KvDelete {
        collection: String,
        keys: Vec<Vec<u8>>,
    },
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
    KvInsertIfAbsent {
        collection: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_ms: u64,
        surrogate: u32,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
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
        surrogates: Vec<u32>,
        /// See `KvPut::resolved_now_ms`.
        resolved_now_ms: Option<u64>,
    },
    KvExpire {
        collection: String,
        key: Vec<u8>,
        ttl_ms: u64,
        /// Leader-resolved expiry clock; always `Some` for EXPIRE.
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
    KvRegisterIndex {
        collection: String,
        field: String,
        field_position: usize,
        backfill: bool,
    },
    KvDropIndex {
        collection: String,
        field: String,
    },
    KvFieldSet {
        collection: String,
        key: Vec<u8>,
        updates: Vec<(String, Vec<u8>)>,
        surrogate: u32,
    },
    KvTransfer {
        collection: String,
        source_key: Vec<u8>,
        dest_key: Vec<u8>,
        field: String,
        amount: f64,
        debit_surrogate: u32,
        credit_surrogate: u32,
    },
    KvTransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        surrogate: u32,
    },
    ArrayOp {
        array: String,
        op_bytes: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    ArraySchema {
        array: String,
        snapshot_payload: Vec<u8>,
        schema_hlc_bytes: [u8; 18],
    },
    BulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, nodedb_physical::physical_plan::UpdateValue)>,
        /// See `PointPut::resolved_sum_targets`.
        ///
        /// A predicate write re-derives its MATCHING rows on every replica, but
        /// not the identity of the target rows those matches credit: that came
        /// from a reconnaissance scan the proposing node ran against the target
        /// collection's catalog. The matches are deterministic; the resolution
        /// is not derivable.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    ColumnarBulkDml {
        collection: String,
        filters: Vec<u8>,
        is_update: bool,
        updates: Vec<(String, Vec<u8>)>,
    },
    InsertSelect {
        target_collection: String,
        source_collection: String,
        source_filters: Vec<u8>,
        source_limit: usize,
    },
    CrdtImportCollection {
        tenant_id: u64,
        collection: String,
        bytes: Vec<u8>,
    },
    CrdtListInsert {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
        fields_json: String,
    },
    CrdtListDelete {
        collection: String,
        document_id: String,
        list_path: String,
        index: u64,
    },
    CrdtListMove {
        collection: String,
        document_id: String,
        list_path: String,
        from_index: u64,
        to_index: u64,
    },
    CrdtDocUpsert {
        collection: String,
        document_id: String,
        surrogate: u32,
        fields_json: String,
        partial: bool,
    },
    CrdtDocDelete {
        collection: String,
        document_id: String,
        surrogate: u32,
    },
    CalvinReadResult {
        epoch: u64,
        position: u32,
        passive_vshard: u32,
        tenant_id: u64,
        values: Vec<u8>,
    },
    DocTruncate {
        collection: String,
        restart_identity: bool,
        /// See `BulkDml::resolved_sum_targets` — a truncate names its rows by
        /// the whole collection, and every one of them takes its contribution
        /// back off a target the proposing node had to resolve.
        #[serde(default)]
        resolved_sum_targets: Vec<(String, u32)>,
        /// See `PointPut::resolved_sum_target_bindings`.
        #[serde(default)]
        resolved_sum_target_bindings: Vec<ReplicatedSumTarget>,
    },
    KvTruncate {
        collection: String,
    },
    ConstraintChange {
        collection: String,
        op: ConstraintChangeOp,
        constraint_version: u64,
        constraints: Vec<Vec<u8>>,
    },
    ArrayCellPut {
        array: String,
        cells_msgpack: Vec<u8>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },
    ArrayCellDelete {
        array: String,
        coords_msgpack: Vec<u8>,
        #[serde(default)]
        provenance: Option<Vec<u8>>,
    },

    /// Fenced CRDT apply; appended to preserve legacy positional records.
    CrdtApplyFenced {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Sync provenance; `None` for non-sync applies.
        provenance: Option<Vec<u8>>,
        /// Leader-admitted collection constraint fence.
        constraint_version_required: u64,
        /// Mandatory exact preview frontier; absent on legacy `CrdtApply`.
        expected_frontier_digest: [u8; 32],
    },
    CrdtApplyAuthenticated {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        provenance: Option<Vec<u8>>,
        constraint_version_required: u64,
        expected_frontier_digest: Option<[u8; 32]>,
        auth_user_id: u64,
        auth_device_id: u64,
        auth_seq_no: u64,
        delta_signature: [u8; 32],
        signing_required: bool,
    },

    /// Tear down one vector index on every replica — the counterpart of
    /// `SetVectorParams`. Appended last to preserve the positional ABI.
    DropVectorIndex {
        collection: String,
        #[serde(default)]
        field_name: String,
    },

    /// Move a materialized-sum balance on a TARGET row that does not share the
    /// source write's vShard. Appended last to preserve the positional ABI.
    ///
    /// A DELTA on the wire, like `KvIncr`, not an absolute balance: the record
    /// says what the statement did, and every replica applies it once under
    /// exactly-once, LSN-ordered Raft apply. An absolute image would be the
    /// leader's arithmetic imposed on the follower, which is only equivalent
    /// while the two agree — and the point of replicating the delta is that it
    /// stays correct even when a follower reaches this entry from a different
    /// (earlier-committed) balance.
    ApplyBalanceDelta {
        /// TARGET collection, db-qualified as the plan names it.
        collection: String,
        /// Target row's storage key — the hex-encoded surrogate.
        document_id: String,
        /// Target row's global identity. Surrogates are Raft-replicated, so the
        /// follower addresses the same row the leader did.
        surrogate: u32,
        /// The balance column this delta moves.
        column: String,
        /// Signed amount, as an exact decimal string. `KvIncr` carries an `i64`
        /// because a KV counter is integral; a balance is not, and a `f64` here
        /// would lose precision the stored total deliberately keeps.
        delta: String,
        /// Binding's join column, for the typed not-found error on apply.
        join_column: String,
        /// Join value that resolved to `surrogate`, same purpose.
        join_value: String,
    },
}
