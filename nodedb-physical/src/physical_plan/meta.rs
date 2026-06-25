// SPDX-License-Identifier: Apache-2.0

//! Control plane meta-operations dispatched to the Data Plane.

use std::collections::BTreeMap;

use nodedb_types::calvin::PassiveReadKey;
use nodedb_types::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_types::{TenantId, Value};

/// Identity of a single key read by a passive Calvin participant.
///
/// Used as the map key in `CalvinExecuteActive::injected_reads` so active
/// participants can look up which value belongs to which key.
///
/// `String` is used for `collection` rather than `Arc<str>` because
/// `zerompk` derives work directly with `String`; callers may intern
/// the string downstream if needed.
///
/// `BTreeMap` key: `Ord` is derived lexicographically — collection first,
/// surrogate second. This is the determinism contract: all replicas must
/// iterate `injected_reads` in the same order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PassiveReadKeyId {
    /// Collection the key belongs to.
    pub collection: String,
    /// Global surrogate for the row.
    pub surrogate: u32,
}

/// Meta / maintenance physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MetaOp {
    /// WAL append (write path).
    WalAppend { payload: Vec<u8> },

    /// Cancellation signal. Data Plane stops the target at next safe point.
    Cancel {
        target_request_id: nodedb_types::id::RequestId,
    },

    /// Atomic transaction batch: execute all sub-plans atomically.
    TransactionBatch { plans: Vec<super::PhysicalPlan> },

    /// Create a snapshot: export all engine state for this core.
    CreateSnapshot,

    /// On-demand compaction.
    Compact,

    /// Checkpoint: flush all engine state to disk, report LSN.
    Checkpoint,

    /// Register a continuous aggregate definition on this core's manager.
    RegisterContinuousAggregate { def: ContinuousAggregateDef },

    /// Remove a continuous aggregate from this core's manager.
    UnregisterContinuousAggregate { name: String },

    /// List all continuous aggregates from this core's manager.
    /// Returns JSON-serialized `Vec<AggregateInfo>`.
    ListContinuousAggregates,

    /// Convert a collection's storage mode.
    ///
    /// Scans all documents in the collection and re-encodes them for the
    /// target type. The catalog update happens on the Control Plane after
    /// the Data Plane confirms success.
    ///
    /// `target_type`: "document_schemaless", "document_strict", "kv".
    /// `schema_json`: for "document_strict"/"kv", JSON-serialized column definitions.
    ConvertCollection {
        collection: String,
        target_type: String,
        schema_json: String,
    },

    /// Snapshot a tenant's data from the sparse engine.
    /// Returns serialized `(documents, indexes)` as JSON payload.
    CreateTenantSnapshot { tenant_id: u64 },

    /// Restore a tenant's data across all engines from a snapshot.
    /// `snapshot` is a MessagePack-serialized `TenantDataSnapshot`.
    ///
    /// `replace_mode` selects the collision policy for engines whose restore is
    /// otherwise fail-closed (columnar / vector / flushed-timeseries):
    /// - `false` (user RESTORE): refuse to overwrite keys already present.
    /// - `true` (Raft InstallSnapshot apply): OVERWRITE present keys, because a
    ///   Raft install must replace local state, not fail against it.
    RestoreTenantSnapshot {
        tenant_id: u64,
        snapshot: Vec<u8>,
        replace_mode: bool,
    },

    /// Purge ALL data for a tenant across every engine and cache.
    ///
    /// Deletes documents, indexes, vectors, graph edges, timeseries,
    /// KV entries, CRDT state, inverted index terms, and cache entries.
    /// Idempotent: safe to re-run after a crash.
    PurgeTenant { tenant_id: u64 },

    /// Purge a single collection across every engine on this core.
    ///
    /// Runs the per-collection equivalent of `PurgeTenant`: each
    /// engine (columnar, vector, FTS, spatial, document/strict/kv,
    /// graph, CRDT, timeseries) reclaims its L1 segment files,
    /// memtables, in-flight compactions, and snapshot references
    /// for this one collection, then a WAL `CollectionTombstoned`
    /// record is appended so replay after purge does not resurrect
    /// rows. The quiesce-drain primitive (see `bridge::quiesce`) is
    /// invoked first so in-flight scans complete before file unlink.
    ///
    /// `purge_lsn` is the metadata-raft commit LSN at which the
    /// `PurgeCollection` entry committed; used by the WAL tombstone
    /// filter and surfaced in the audit trail.
    ///
    /// Idempotent: re-running after partial completion picks up
    /// where the previous run left off.
    UnregisterCollection {
        tenant_id: u64,
        name: String,
        purge_lsn: u64,
    },

    /// Reclaim every local Data Plane resource for a single
    /// materialized view. Mirrors `UnregisterCollection` one level
    /// up: an MV has its own columnar segment files (populated by the
    /// CDC refresh loop) that outlive the MV's catalog row when the
    /// MV is dropped, unless reclaim runs on every follower. Drops
    /// the MV's in-memory refresh state + unlinks its segment files.
    ///
    /// Idempotent: missing in-memory state is a no-op; missing files
    /// are a no-op. Runs on every node.
    UnregisterMaterializedView { tenant_id: u64, name: String },

    /// Estimate the on-core data size (in bytes) for a single
    /// `(tenant_id, collection)` pair. Sums per-engine in-memory
    /// state: KV hash-table bytes, columnar flushed-segment byte
    /// count, vector-index byte count, and sparse-redb document
    /// range. Response payload is a u64 LE byte count.
    ///
    /// Used by `_system.dropped_collections.size_bytes_estimate` to
    /// surface "how much storage will a hard-delete reclaim?"
    /// without waiting for a purge cycle.
    QueryCollectionSize { tenant_id: u64, name: String },

    /// Enforce retention on a timeseries collection: drop segments older than
    /// the cutoff. Called by the retention policy enforcement loop.
    EnforceTimeseriesRetention { collection: String, max_age_ms: i64 },

    /// Bitemporal audit-retention purge for the graph edge store.
    ///
    /// Deletes versioned edge rows (in `edge_store/temporal` layout) where
    /// `system_from_ms < cutoff_system_ms` AND a newer version of the same
    /// base key exists. Never deletes the single surviving (latest) version
    /// of a base key, even if it is older than the cutoff — "audit retain"
    /// reclaims only *superseded* history, not the current row.
    /// Emits one `RecordType::TemporalPurge` WAL record per purged batch.
    TemporalPurgeEdgeStore {
        tenant_id: u64,
        collection: String,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for the DocumentStrict versioned
    /// tables (`documents_versioned` + `indexes_versioned`). Same semantics
    /// as `TemporalPurgeEdgeStore`: drop versions older than `cutoff_system_ms`
    /// when a newer version of the same `(tenant, coll, doc_id)` exists;
    /// never drop the single surviving version. Deletes index-version rows
    /// keyed to the purged document versions in the same transaction.
    TemporalPurgeDocumentStrict {
        tenant_id: u64,
        collection: String,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for plain columnar collections.
    /// Reuses the timeseries max-system-ts axis on partition meta: any
    /// partition whose `max_system_ts < cutoff_system_ms` and whose max
    /// column version has been superseded by a later partition is removed.
    /// Non-bitemporal columnar collections are a no-op (collection is
    /// expected to be flagged bitemporal by the caller).
    TemporalPurgeColumnar {
        tenant_id: u64,
        collection: String,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for CRDT (Loro-backed) collections.
    /// Drops archived row versions whose `_ts_system < cutoff_system_ms`
    /// from the per-collection bitemporal history sibling. Never removes
    /// the live row, so `AS OF now()` reads remain correct post-purge.
    TemporalPurgeCrdt {
        tenant_id: u64,
        collection: String,
        cutoff_system_ms: i64,
    },

    /// Bitemporal audit-retention purge for the array engine.
    ///
    /// Drops superseded tile versions (those with `system_from_ms <
    /// cutoff_system_ms` where a newer version of the same tile key
    /// exists) from the array's bitemporal storage. The single surviving
    /// (latest) version of each tile is never removed. Arrays are
    /// globally-scoped — `tenant_id` carries the sentinel value `0`.
    TemporalPurgeArray {
        tenant_id: u64,
        /// Global array id from `ArrayCatalogEntry::name`. Arrays are
        /// not yet tenant-scoped.
        array_id: String,
        cutoff_system_ms: i64,
    },

    /// Alter the bitemporal retention policy of an array.
    ///
    /// All real work (catalog rewrite + registry update) is done on the
    /// Control Plane before this op is emitted. The Data Plane handler
    /// returns an 8-byte LE u64 acknowledgement (new `audit_retain_ms`
    /// if set, or 0). This variant exists so the alter command travels
    /// through the standard plan dispatch path and its permission is
    /// classified as `Admin`.
    ///
    /// Double-`Option` semantics:
    /// - `None`          = field omitted from SET clause; do not change.
    /// - `Some(None)`    = SET to NULL (unregister from retention registry).
    /// - `Some(Some(v))` = SET to v.
    AlterArray {
        /// Global array id (name). Arrays are not yet tenant-scoped.
        array_id: String,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<Option<u64>>,
    },

    /// Apply retention to continuous aggregate buckets managed by
    /// the aggregate manager. Drops materialized buckets older than
    /// each aggregate's configured retention_period_ms.
    ApplyContinuousAggRetention,

    /// Query the watermark for a named continuous aggregate.
    /// Returns JSON-serialized `WatermarkState`.
    QueryAggregateWatermark { aggregate_name: String },

    /// Query all entries from a collection's last-value cache.
    /// Returns JSON-serialized `Vec<(u64, i64, f64)>` — (series_id, ts, value).
    QueryLastValues { collection: String },

    /// Query a single series from a collection's last-value cache.
    /// Returns JSON-serialized `Option<(i64, f64)>` — (ts, value).
    QueryLastValue { collection: String, series_id: u64 },

    /// Calvin deterministic executor: static-set multi-shard transaction.
    ///
    /// The Calvin scheduler dispatches this variant after lock acquisition for
    /// transactions whose read/write set is fully known at submission time (the
    /// common case). The Data Plane handler executes `plans` atomically (same
    /// semantics as `TransactionBatch`) and the scheduler writes a
    /// `WalRecord::CalvinApplied` after a successful response.
    ///
    /// NOTE: This variant occupies the same msgpack positional tag as the
    /// original `CalvinExecute` variant it replaces, preserving wire
    /// compatibility with any in-flight log entries from before the rename.
    CalvinExecuteStatic {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope for all plans in this batch.
        tenant_id: TenantId,
        /// Physical plans to execute atomically.
        plans: Vec<super::PhysicalPlan>,
        /// Wall-clock ms read once on the sequencer leader at epoch creation.
        /// Used by engine handlers as the deterministic time anchor (bitemporal
        /// sys_from, KV TTL expire_at, timeseries system_ms) instead of reading
        /// the wall clock independently. Wire-additive: defaults to 0 on decode
        /// of older entries.
        epoch_system_ms: i64,
    },

    /// Calvin dependent-read executor: passive participant reads keys and
    /// returns values for broadcast.
    ///
    /// Dispatched by the scheduler to passive vshards (those holding only
    /// read keys, not write keys) for a dependent-read Calvin transaction.
    /// The Data Plane handler reads each key from the local engine and
    /// returns a msgpack-encoded `Vec<(PassiveReadKeyId, Value)>` payload.
    /// The scheduler then proposes a `ReplicatedWrite::CalvinReadResult`
    /// to the per-vshard Raft group so all replicas see the same values.
    CalvinExecutePassive {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope.
        tenant_id: TenantId,
        /// Keys to read on this passive vshard.
        keys_to_read: Vec<PassiveReadKey>,
    },

    /// Calvin dependent-read executor: active participant writes with
    /// injected read values.
    ///
    /// Dispatched by the scheduler to active vshards (those holding write
    /// keys) once all passive read results have been received. The
    /// `injected_reads` map is keyed by `PassiveReadKeyId` and carries the
    /// values broadcast by the passive participants.
    ///
    /// OLLP verification: before writing, the handler checks whether the
    /// predicate match in the txn's read set still matches the actual rows
    /// in the engine. If mismatched, returns `OllpRetryRequired` status
    /// and does NOT write. The OLLP orchestrator on the Control Plane
    /// interprets this status and retries via `Inbox::submit`.
    CalvinExecuteActive {
        /// Sequencer epoch this transaction belongs to.
        epoch: u64,
        /// Zero-based position within the epoch batch.
        position: u32,
        /// Tenant scope.
        tenant_id: TenantId,
        /// Physical plans to execute atomically.
        plans: Vec<super::PhysicalPlan>,
        /// Read values injected from passive participants.
        /// `BTreeMap` for deterministic iteration order (determinism contract).
        injected_reads: BTreeMap<PassiveReadKeyId, Value>,
        /// Wall-clock ms read once on the sequencer leader at epoch creation.
        /// Same semantics as `CalvinExecuteStatic::epoch_system_ms`.
        epoch_system_ms: i64,
    },

    /// Rebuild all indexes (HNSW, FTS LSM, graph CSR) for a collection
    /// on this core in a shadow-build + atomic-swap manner.
    ///
    /// When `concurrent = true`, the build proceeds without blocking query
    /// handling: a background OS thread performs the rebuild and the Data
    /// Plane polls for completion on subsequent ticks, only swapping the
    /// live index in at cutover.  When `concurrent = false` the rebuild
    /// runs inline (same semantics as the legacy Checkpoint path).
    ///
    /// `index_name` narrows the rebuild to a single named index when set;
    /// `None` rebuilds all index types for the collection.
    ///
    /// Returns `Response::Ok` on successful cutover, or a typed error if:
    /// - another rebuild is already in progress for this collection
    ///   (`ErrorCode::Conflict`), or
    /// - the shadow build fails (`ErrorCode::Internal`).
    RebuildIndex {
        collection: String,
        index_name: Option<String>,
        concurrent: bool,
    },

    /// Persist a synonym group to the FTS backend on this core.
    ///
    /// Called after the Control Plane has already written to the catalog and
    /// updated the in-memory registry. The Data Plane handler writes the group
    /// to the `FtsIndex` meta store so query-time expansion can find it.
    PutSynonymGroup {
        tenant_id: u64,
        /// Serialized `SynonymGroupRecord` (JSON).
        record_json: String,
    },

    /// Remove a synonym group from the FTS backend on this core.
    ///
    /// Called after the Control Plane has already removed it from the catalog.
    DeleteSynonymGroup { tenant_id: u64, name: String },

    /// Re-key all documents and secondary indexes for a collection from
    /// `old_collection` (db-qualified source name) to `new_collection`
    /// (db-qualified target name) in the local Data Plane sparse engine.
    ///
    /// Called after `MoveTenantCutover` applies so that physical data is
    /// accessible under the new database context.  Both `old_collection` and
    /// `new_collection` are the `db_qualified` strings used as the logical
    /// collection identifier in the sparse store
    /// (e.g. `"2/orders"` for database 2, collection `orders`).
    RenameCollection {
        tenant_id: u64,
        old_database_id: u64,
        new_database_id: u64,
        old_collection: String,
        new_collection: String,
    },
}
