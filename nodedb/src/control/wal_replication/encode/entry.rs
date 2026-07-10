// SPDX-License-Identifier: BUSL-1.1

//! Entry point: encode a write-side `PhysicalPlan` into a `ReplicatedEntry`
//! for Raft proposal, plus the shared provenance-encoding helper.

use super::super::types::ReplicatedEntry;
use super::{columnar, crdt, document, graph, kv, vector};
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, KvOp, SpatialOp, TextOp, TimeseriesOp,
};

/// Serialize optional sync provenance into the cross-node wire shape.
///
/// `SyncProvenance` is a plain POD struct (producer_id / epoch / stream_id /
/// seq); its msgpack encoding is infallible — the same contract the
/// `geometry_bytes` encoding relies on. We `.expect()` rather than silently
/// dropping provenance with `.ok()`: losing provenance on a follower would
/// defeat the idempotency gate and risk double-apply, so a (theoretical)
/// encode failure must fail loud, not replicate `None`.
pub(super) fn encode_provenance(
    provenance: &Option<nodedb_types::sync::wire::SyncProvenance>,
) -> Option<Vec<u8>> {
    provenance
        .as_ref()
        .map(|p| zerompk::to_msgpack_vec(p).expect("SyncProvenance serialization is infallible"))
}

pub fn to_replicated_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
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
        }) => document::point_put(collection, document_id, value, surrogate.as_u32()),
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
        }) => document::point_insert(
            collection,
            document_id,
            value,
            *if_absent,
            surrogate.as_u32(),
        ),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        }) => document::point_delete(collection, document_id, surrogate.as_u32()),
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
            ..
        }) => document::point_update(collection, document_id, updates, surrogate.as_u32()),
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
        }) => document::upsert(
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate.as_u32(),
        ),
        // All `VectorOp` write variants (including the sparse / multi-vector /
        // direct-upsert / delete-by-surrogate family) are dispatched via
        // `vector::encode`, which is exhaustive over `VectorOp` and returns
        // `None` for the read/DDL variants — see that function's doc.
        PhysicalPlan::Vector(op) => vector::encode(op)?,
        // All `CrdtOp` write variants (including the block-list `ListInsert`
        // / `ListDelete` / `ListMove` family) are dispatched via
        // `crdt::encode`, which is exhaustive over `CrdtOp` and returns
        // `None` for the read / still-buffered-unencoded variants — see
        // that function's doc.
        PhysicalPlan::Crdt(op) => crdt::encode(op)?,
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        }) => graph::edge_put(
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate.as_u32(),
            dst_surrogate.as_u32(),
        ),
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
        }) => graph::edge_delete(
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate.as_u32(),
            dst_surrogate.as_u32(),
        ),
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels }) => {
            graph::set_node_labels(node_id, labels)
        }
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels }) => {
            graph::remove_node_labels(node_id, labels)
        }
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) => graph::edge_put_batch(edges),
        PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => graph::edge_delete_batch(edges),
        PhysicalPlan::Kv(KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => kv::put(collection, key, value, *ttl_ms, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => kv::delete(collection, keys),
        PhysicalPlan::Kv(KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => kv::insert(collection, key, value, *ttl_ms, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        }) => kv::insert_if_absent(collection, key, value, *ttl_ms, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
        }) => kv::insert_on_conflict_update(
            collection,
            key,
            value,
            *ttl_ms,
            updates,
            surrogate.as_u32(),
        ),
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
        }) => kv::batch_put(collection, entries, *ttl_ms, surrogates),
        PhysicalPlan::Kv(KvOp::Expire {
            collection,
            key,
            ttl_ms,
        }) => kv::expire(collection, key, *ttl_ms),
        PhysicalPlan::Kv(KvOp::Persist { collection, key }) => kv::persist(collection, key),
        PhysicalPlan::Kv(KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
        }) => kv::incr(collection, key, *delta, *ttl_ms, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate,
        }) => kv::incr_float(collection, key, *delta, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
        }) => kv::cas(collection, key, expected, new_value, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate,
        }) => kv::get_set(collection, key, new_value, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        }) => kv::register_sorted_index(kv::RegisterSortedIndexFields {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        PhysicalPlan::Kv(KvOp::DropSortedIndex { index_name }) => kv::drop_sorted_index(index_name),
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate,
        }) => kv::field_set(collection, key, updates, surrogate.as_u32()),
        PhysicalPlan::Kv(KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        }) => kv::transfer(
            collection,
            source_key,
            dest_key,
            field,
            *amount,
            debit_surrogate.as_u32(),
            credit_surrogate.as_u32(),
        ),
        PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
        }) => kv::transfer_item(
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate.as_u32(),
        ),
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
        }) => columnar::columnar_ingest(
            collection,
            payload,
            surrogates,
            schema_bytes,
            encode_provenance(provenance),
        ),
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            surrogates,
            provenance,
            ..
        }) => columnar::timeseries_ingest(
            collection,
            payload,
            format,
            surrogates,
            encode_provenance(provenance),
        ),
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance,
        }) => columnar::fts_index(
            collection,
            surrogate.as_u32(),
            text,
            encode_provenance(provenance),
        ),
        PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance,
        }) => columnar::fts_delete(
            collection,
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),
        PhysicalPlan::Spatial(SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        }) => columnar::spatial_insert(
            collection,
            field,
            surrogate.as_u32(),
            geometry,
            encode_provenance(provenance),
        ),
        PhysicalPlan::Spatial(SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        }) => columnar::spatial_delete(
            collection,
            field,
            surrogate.as_u32(),
            encode_provenance(provenance),
        ),
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection,
            filters,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        }) => document::bulk_delete(collection, filters),
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection,
            filters,
            updates,
            returning: _,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
        }) => document::bulk_update(collection, filters, updates),
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection,
            filters,
        }) => columnar::bulk_delete(collection, filters),
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection,
            filters,
            updates,
        }) => columnar::bulk_update(collection, filters, updates),
        PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        }) => document::insert_select(
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        ),
        // Not a write — reads, system ops, etc. (Also: OLLP-prepared bulk plans
        // carrying predicted surrogates/edges, which route via Calvin.)
        _ => return None,
    };

    Some(ReplicatedEntry::new(
        tenant_id.as_u64(),
        database_id.as_u64(),
        vshard_id.as_u32(),
        write,
    ))
}
