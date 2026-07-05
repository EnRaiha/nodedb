// SPDX-License-Identifier: BUSL-1.1

//! Convert write-side PhysicalPlan variants to ReplicatedWrite for Raft proposal.

use super::types::{ReplicatedBatchEdge, ReplicatedEntry, ReplicatedWrite};
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Serialize optional sync provenance into the cross-node wire shape.
///
/// `SyncProvenance` is a plain POD struct (producer_id / epoch / stream_id /
/// seq); its msgpack encoding is infallible — the same contract the
/// `geometry_bytes` encoding below relies on. We `.expect()` rather than
/// silently dropping provenance with `.ok()`: losing provenance on a follower
/// would defeat the idempotency gate and risk double-apply, so a (theoretical)
/// encode failure must fail loud, not replicate `None`.
fn encode_provenance(
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
) -> Option<Vec<u8>> {
    provenance
        .as_ref()
        .map(|p| zerompk::to_msgpack_vec(p).expect("SyncProvenance serialization is infallible"))
}

pub fn to_replicated_entry(
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> Option<ReplicatedEntry> {
    let write = match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            pk_bytes: _,
        }) => ReplicatedWrite::PointPut {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
        }) => ReplicatedWrite::PointInsert {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
            if_absent: *if_absent,
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        }) => ReplicatedWrite::PointDelete {
            collection: collection.clone(),
            document_id: document_id.clone(),
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            ..
        }) => ReplicatedWrite::PointUpdate {
            collection: collection.clone(),
            document_id: document_id.clone(),
            updates: updates.clone(),
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
        }) => ReplicatedWrite::DocUpsert {
            collection: collection.clone(),
            document_id: document_id.clone(),
            value: value.clone(),
            on_conflict_updates: on_conflict_updates.clone(),
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Vector(VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance,
        }) => ReplicatedWrite::VectorInsert {
            collection: collection.clone(),
            vector: vector.clone(),
            dim: *dim,
            field_name: field_name.clone(),
            // Carry the leader-assigned surrogate verbatim. Followers bind
            // (never re-allocate) by `pk_bytes` when present, else by the
            // surrogate's own self-key.
            surrogate: surrogate.as_u32(),
            pk_bytes: pk_bytes.clone(),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        }) => ReplicatedWrite::VectorBatchInsert {
            collection: collection.clone(),
            vectors: vectors.clone(),
            dim: *dim,
            surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
        },
        PhysicalPlan::Vector(VectorOp::Delete {
            collection,
            vector_id,
        }) => ReplicatedWrite::VectorDelete {
            collection: collection.clone(),
            vector_id: *vector_id,
        },
        PhysicalPlan::Vector(VectorOp::SetParams {
            collection,
            field_name,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        }) => ReplicatedWrite::SetVectorParams {
            collection: collection.clone(),
            field_name: field_name.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
            index_type: index_type.clone(),
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        },
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            delta,
            peer_id,
            mutation_id: _,
            surrogate: _,
            provenance,
            constraint_version_required,
        }) => ReplicatedWrite::CrdtApply {
            collection: collection.clone(),
            document_id: document_id.clone(),
            delta: delta.clone(),
            peer_id: *peer_id,
            provenance: encode_provenance(provenance),
            constraint_version_required: *constraint_version_required,
        },
        PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
            tenant_id,
            collection,
            bytes,
        }) => ReplicatedWrite::CrdtImportCollection {
            tenant_id: *tenant_id,
            collection: collection.clone(),
            bytes: bytes.clone(),
        },
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        }) => ReplicatedWrite::EdgePut {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            properties: properties.clone(),
            src_surrogate: src_surrogate.as_u32(),
            dst_surrogate: dst_surrogate.as_u32(),
        },
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
        }) => ReplicatedWrite::EdgeDelete {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
            src_surrogate: src_surrogate.as_u32(),
            dst_surrogate: dst_surrogate.as_u32(),
        },
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels }) => {
            ReplicatedWrite::SetNodeLabels {
                node_id: node_id.clone(),
                labels: labels.clone(),
            }
        }
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels }) => {
            ReplicatedWrite::RemoveNodeLabels {
                node_id: node_id.clone(),
                labels: labels.clone(),
            }
        }
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) => ReplicatedWrite::EdgePutBatch {
            edges: edges
                .iter()
                .map(|e| ReplicatedBatchEdge {
                    collection: e.collection.clone(),
                    src_id: e.src_id.clone(),
                    label: e.label.clone(),
                    dst_id: e.dst_id.clone(),
                    src_surrogate: e.src_surrogate.as_u32(),
                    dst_surrogate: e.dst_surrogate.as_u32(),
                })
                .collect(),
        },
        PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => {
            ReplicatedWrite::EdgeDeleteBatch {
                edges: edges
                    .iter()
                    .map(|e| ReplicatedBatchEdge {
                        collection: e.collection.clone(),
                        src_id: e.src_id.clone(),
                        label: e.label.clone(),
                        dst_id: e.dst_id.clone(),
                        src_surrogate: e.src_surrogate.as_u32(),
                        dst_surrogate: e.dst_surrogate.as_u32(),
                    })
                    .collect(),
            }
        }
        PhysicalPlan::Kv(KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => ReplicatedWrite::KvPut {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => ReplicatedWrite::KvDelete {
            collection: collection.clone(),
            keys: keys.clone(),
        },
        PhysicalPlan::Kv(KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => ReplicatedWrite::KvInsert {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Kv(KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => ReplicatedWrite::KvInsertIfAbsent {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
        }) => ReplicatedWrite::KvInsertOnConflictUpdate {
            collection: collection.clone(),
            key: key.clone(),
            value: value.clone(),
            ttl_ms: *ttl_ms,
            updates: updates.clone(),
            surrogate: surrogate.as_u32(),
        },
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
        }) => ReplicatedWrite::KvBatchPut {
            collection: collection.clone(),
            entries: entries.clone(),
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::Expire {
            collection,
            key,
            ttl_ms,
        }) => ReplicatedWrite::KvExpire {
            collection: collection.clone(),
            key: key.clone(),
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::Persist { collection, key }) => ReplicatedWrite::KvPersist {
            collection: collection.clone(),
            key: key.clone(),
        },
        PhysicalPlan::Kv(KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
        }) => ReplicatedWrite::KvIncr {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
            ttl_ms: *ttl_ms,
        },
        PhysicalPlan::Kv(KvOp::IncrFloat {
            collection,
            key,
            delta,
        }) => ReplicatedWrite::KvIncrFloat {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
        },
        PhysicalPlan::Kv(KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
        }) => ReplicatedWrite::KvCas {
            collection: collection.clone(),
            key: key.clone(),
            expected: expected.clone(),
            new_value: new_value.clone(),
        },
        PhysicalPlan::Kv(KvOp::GetSet {
            collection,
            key,
            new_value,
        }) => ReplicatedWrite::KvGetSet {
            collection: collection.clone(),
            key: key.clone(),
            new_value: new_value.clone(),
        },
        PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        }) => ReplicatedWrite::KvRegisterSortedIndex {
            collection: collection.clone(),
            index_name: index_name.clone(),
            sort_columns: sort_columns.clone(),
            key_column: key_column.clone(),
            window_type: window_type.clone(),
            window_timestamp_column: window_timestamp_column.clone(),
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        },
        PhysicalPlan::Kv(KvOp::DropSortedIndex { index_name }) => {
            ReplicatedWrite::KvDropSortedIndex {
                index_name: index_name.clone(),
            }
        }
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection,
            payload,
            surrogates,
            schema_bytes,
            provenance,
            // wal_lsn is omitted from the wire envelope; followers allocate
            // their own LSN at apply time. intent and on_conflict_updates are
            // always Insert/empty on the sync path and are hardcoded on decode.
            ..
        }) => ReplicatedWrite::ColumnarIngest {
            collection: collection.clone(),
            payload: payload.clone(),
            schema_bytes: schema_bytes.clone(),
            surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            surrogates,
            provenance,
            ..
        }) => ReplicatedWrite::TimeseriesIngest {
            collection: collection.clone(),
            payload: payload.clone(),
            format: format.clone(),
            surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance,
        }) => ReplicatedWrite::FtsIndex {
            collection: collection.clone(),
            surrogate: surrogate.as_u32(),
            text: text.clone(),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance,
        }) => ReplicatedWrite::FtsDelete {
            collection: collection.clone(),
            surrogate: surrogate.as_u32(),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Spatial(SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        }) => ReplicatedWrite::SpatialInsert {
            collection: collection.clone(),
            field: field.clone(),
            surrogate: surrogate.as_u32(),
            // Geometry is plain serializable data — encoding is infallible
            // (same contract as `ReplicatedEntry::to_bytes`). Fail loud rather
            // than replicate empty bytes that would error on follower decode.
            geometry_bytes: zerompk::to_msgpack_vec(geometry)
                .expect("Geometry serialization is infallible"),
            provenance: encode_provenance(provenance),
        },
        PhysicalPlan::Spatial(SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        }) => ReplicatedWrite::SpatialDelete {
            collection: collection.clone(),
            field: field.clone(),
            surrogate: surrogate.as_u32(),
            provenance: encode_provenance(provenance),
        },
        // Single-shard bulk predicate writes replicate as a plain `BulkDml`
        // entry: each replica re-scans local state at the committed log
        // position and applies the predicate deterministically (Raft log order
        // ⇒ identical prior state ⇒ identical matching set). An OLLP-prepared
        // bulk plan (carrying `ollp_predicted_surrogates` / `ollp_predicted_edges`)
        // belongs to the cross-shard Calvin path and is NOT encoded here — it
        // returns `None` and is dispatched via Calvin, which coordinates the
        // matching set across ≥2 Raft groups.
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            filters,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        }) => ReplicatedWrite::BulkDml {
            collection: collection.clone(),
            filters: filters.clone(),
            is_update: false,
            updates: Vec::new(),
        },
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        }) => ReplicatedWrite::BulkDml {
            collection: collection.clone(),
            filters: filters.clone(),
            is_update: true,
            updates: updates.clone(),
        },
        // `INSERT ... SELECT ... WHERE <predicate>` replicates as a plain
        // `InsertSelect` entry: each replica re-scans the source at the
        // committed log position and copies the predicate matches, reusing each
        // source row's surrogate/doc_id. Deterministic by Raft log order ⇒
        // identical prior state ⇒ identical copied set.
        PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        }) => ReplicatedWrite::InsertSelect {
            target_collection: target_collection.clone(),
            source_collection: source_collection.clone(),
            source_filters: source_filters.clone(),
            source_limit: *source_limit,
        },
        // Not a write — reads, system ops, etc. (Also: OLLP-prepared bulk plans
        // carrying predicted surrogates/edges, which route via Calvin.)
        _ => return None,
    };

    Some(ReplicatedEntry::new(
        tenant_id.as_u64(),
        vshard_id.as_u32(),
        write,
    ))
}
