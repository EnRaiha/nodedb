// SPDX-License-Identifier: BUSL-1.1

//! Entry point: decode a committed `ReplicatedEntry` into a `PhysicalPlan`,
//! plus the shared surrogate-binding helpers used across the per-engine
//! decode submodules.

use super::super::decode_sync_engines;
use super::super::types::{ReplicatedEntry, ReplicatedWrite};
use super::{columnar, crdt, document, graph, kv, vector};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::surrogate::SurrogateAssigner;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Bundles the ambient decode parameters (surrogate assigner + tenancy
/// scope) threaded through every per-engine decode helper.
pub(super) struct DecodeCtx<'a> {
    pub(super) assigner: Option<&'a SurrogateAssigner>,
    pub(super) database_id: DatabaseId,
    pub(super) tenant_id: TenantId,
}

/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
///
/// `assigner`, when `Some`, drives follower-local surrogate binding.
/// Single-row writers (documents, KV, vector, graph edges) carry the
/// leader-assigned surrogate verbatim on the wire and call
/// `assigner.bind(...)` to install that exact identity in the local catalog
/// (+ `SurrogateBind` WAL record) — they never re-allocate, so the same key
/// resolves to the same surrogate on every node. CRDT variants still
/// re-derive via `assign`. When `None`, surrogate fields fall back to the
/// carried value / `Surrogate::ZERO` without catalog writes (used by tests
/// that exercise the decoder without `SharedState`).
pub fn from_replicated_entry(
    data: &[u8],
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<Option<(TenantId, VShardId, PhysicalPlan)>> {
    let entry = match ReplicatedEntry::from_bytes(data) {
        Some(e) => e,
        None => return Ok(None),
    };
    // Array CRDT variants are handled by the distributed applier before this
    // function is called. Return None so the applier skips the generic dispatch
    // path for them.
    match &entry.write {
        ReplicatedWrite::ArrayOp { .. } | ReplicatedWrite::ArraySchema { .. } => {
            return Ok(None);
        }
        _ => {}
    }
    let tenant_id = TenantId::new(entry.tenant_id);
    // `0` decodes to `DatabaseId::DEFAULT` — the same convention used for
    // entries that pre-date the field (see `LegacyReplicatedEntry`).
    let database_id = DatabaseId::new(entry.database_id);
    let ctx = DecodeCtx {
        assigner,
        database_id,
        tenant_id,
    };
    let plan = to_physical_plan(&entry.write, &ctx)?;
    Ok(Some((tenant_id, VShardId::new(entry.vshard_id), plan)))
}

pub(super) fn assign_or_zero(
    ctx: &DecodeCtx,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    match ctx.assigner {
        Some(a) => a.assign(ctx.database_id, ctx.tenant_id, collection, pk_bytes),
        None => Ok(nodedb_types::Surrogate::ZERO),
    }
}

/// Resolve `carried` for a mutating op that does NOT create rows (UPDATE /
/// DELETE). When `carried` is authoritative (non-ZERO, from a member
/// coordinator) the binding is installed first-wins via `bind`. When `carried`
/// is ZERO (non-member coordinator that missed resolution) the catalog is
/// queried READ-ONLY; ZERO is never bound, so a later INSERT of the same pk
/// gets a freshly allocated surrogate instead of the corrupt ZERO entry.
pub(super) fn bind_or_lookup(
    ctx: &DecodeCtx,
    collection: &str,
    pk_bytes: &[u8],
    carried: nodedb_types::Surrogate,
) -> crate::Result<nodedb_types::Surrogate> {
    match ctx.assigner {
        Some(a) if carried != nodedb_types::Surrogate::ZERO => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            pk_bytes,
            carried,
        ),
        Some(a) => Ok(a
            .lookup(ctx.database_id, ctx.tenant_id, collection, pk_bytes)?
            .unwrap_or(nodedb_types::Surrogate::ZERO)),
        None => Ok(carried),
    }
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(write: &ReplicatedWrite, ctx: &DecodeCtx) -> crate::Result<PhysicalPlan> {
    Ok(match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
        } => document::point_put(ctx, collection, document_id, value, *surrogate)?,
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
        } => document::point_insert(ctx, collection, document_id, value, *if_absent, *surrogate)?,
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
        } => document::point_delete(ctx, collection, document_id, *surrogate)?,
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
        } => document::point_update(ctx, collection, document_id, updates, *surrogate)?,
        ReplicatedWrite::DocUpsert {
            collection,
            document_id,
            value,
            on_conflict_updates,
            surrogate,
        } => document::doc_upsert(
            ctx,
            collection,
            document_id,
            value,
            on_conflict_updates,
            *surrogate,
        )?,
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance,
        } => vector::insert(
            ctx,
            vector::InsertFields {
                collection,
                vector,
                dim: *dim,
                field_name,
                surrogate: *surrogate,
                pk_bytes,
                provenance,
            },
        )?,
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        } => vector::batch_insert(ctx, collection, vectors, *dim, surrogates)?,
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => vector::delete(collection, *vector_id),
        ReplicatedWrite::SetVectorParams {
            collection,
            field_name,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => vector::set_params(vector::SetParamsFields {
            collection,
            field_name,
            m: *m,
            ef_construction: *ef_construction,
            metric,
            index_type,
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        }),
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
            provenance,
            constraint_version_required,
        } => crdt::apply(
            ctx,
            collection,
            document_id,
            delta,
            *peer_id,
            provenance,
            *constraint_version_required,
        )?,
        ReplicatedWrite::CrdtImportCollection {
            tenant_id,
            collection,
            bytes,
        } => crdt::import_collection(*tenant_id, collection, bytes),
        ReplicatedWrite::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } => graph::edge_put(
            ctx,
            graph::EdgePutFields {
                collection,
                src_id,
                label,
                dst_id,
                properties,
                src_surrogate: *src_surrogate,
                dst_surrogate: *dst_surrogate,
            },
        )?,
        ReplicatedWrite::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
        } => graph::edge_delete(
            ctx,
            collection,
            src_id,
            label,
            dst_id,
            *src_surrogate,
            *dst_surrogate,
        )?,
        ReplicatedWrite::SetNodeLabels { node_id, labels } => {
            graph::set_node_labels(node_id, labels)
        }
        ReplicatedWrite::RemoveNodeLabels { node_id, labels } => {
            graph::remove_node_labels(node_id, labels)
        }
        ReplicatedWrite::EdgePutBatch { edges } => graph::edge_put_batch(ctx, edges)?,
        ReplicatedWrite::EdgeDeleteBatch { edges } => graph::edge_delete_batch(ctx, edges)?,
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        } => kv::put(ctx, collection, key, value, *ttl_ms, *surrogate)?,
        ReplicatedWrite::KvDelete { collection, keys } => kv::delete(collection, keys),
        ReplicatedWrite::KvInsert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        } => kv::insert(ctx, collection, key, value, *ttl_ms, *surrogate)?,
        ReplicatedWrite::KvInsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        } => kv::insert_if_absent(ctx, collection, key, value, *ttl_ms, *surrogate)?,
        ReplicatedWrite::KvInsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate,
        } => kv::insert_on_conflict_update(
            ctx, collection, key, value, *ttl_ms, updates, *surrogate,
        )?,
        ReplicatedWrite::KvBatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates,
        } => kv::batch_put(ctx, collection, entries, *ttl_ms, surrogates)?,
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
        } => kv::expire(collection, key, *ttl_ms),
        ReplicatedWrite::KvPersist { collection, key } => kv::persist(collection, key),
        ReplicatedWrite::KvIncr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate,
        } => kv::incr(ctx, collection, key, *delta, *ttl_ms, *surrogate)?,
        ReplicatedWrite::KvIncrFloat {
            collection,
            key,
            delta,
            surrogate,
        } => kv::incr_float(ctx, collection, key, *delta, *surrogate)?,
        ReplicatedWrite::KvCas {
            collection,
            key,
            expected,
            new_value,
            surrogate,
        } => kv::cas(ctx, collection, key, expected, new_value, *surrogate)?,
        ReplicatedWrite::KvGetSet {
            collection,
            key,
            new_value,
            surrogate,
        } => kv::get_set(ctx, collection, key, new_value, *surrogate)?,
        ReplicatedWrite::KvRegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => kv::register_sorted_index(kv::RegisterSortedIndexFields {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        ReplicatedWrite::KvDropSortedIndex { index_name } => kv::drop_sorted_index(index_name),
        ReplicatedWrite::KvFieldSet {
            collection,
            key,
            updates,
            surrogate,
        } => kv::field_set(ctx, collection, key, updates, *surrogate)?,
        ReplicatedWrite::KvTransfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        } => kv::transfer(
            ctx,
            kv::TransferFields {
                collection,
                source_key,
                dest_key,
                field,
                amount: *amount,
                debit_surrogate: *debit_surrogate,
                credit_surrogate: *credit_surrogate,
            },
        )?,
        ReplicatedWrite::KvTransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate,
        } => kv::transfer_item(
            ctx,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            *surrogate,
        )?,
        ReplicatedWrite::ColumnarIngest {
            collection,
            payload,
            schema_bytes,
            surrogates,
            provenance,
        } => decode_sync_engines::columnar_ingest(
            collection,
            payload,
            schema_bytes,
            surrogates,
            provenance,
        )?,
        ReplicatedWrite::TimeseriesIngest {
            collection,
            payload,
            format,
            surrogates,
            provenance,
        } => decode_sync_engines::timeseries_ingest(
            collection, payload, format, surrogates, provenance,
        )?,
        ReplicatedWrite::FtsIndex {
            collection,
            surrogate,
            text,
            provenance,
        } => decode_sync_engines::fts_index(collection, *surrogate, text, provenance)?,
        ReplicatedWrite::FtsDelete {
            collection,
            surrogate,
            provenance,
        } => decode_sync_engines::fts_delete(collection, *surrogate, provenance)?,
        ReplicatedWrite::SpatialInsert {
            collection,
            field,
            surrogate,
            geometry_bytes,
            provenance,
        } => decode_sync_engines::spatial_insert(
            collection,
            field,
            *surrogate,
            geometry_bytes,
            provenance,
        )?,
        ReplicatedWrite::SpatialDelete {
            collection,
            field,
            surrogate,
            provenance,
        } => decode_sync_engines::spatial_delete(collection, field, *surrogate, provenance)?,
        ReplicatedWrite::BulkDml {
            collection,
            filters,
            is_update,
            updates,
        } => document::bulk_dml(collection, filters, *is_update, updates),
        ReplicatedWrite::ColumnarBulkDml {
            collection,
            filters,
            is_update,
            updates,
        } => columnar::bulk_dml(collection, filters, *is_update, updates),
        ReplicatedWrite::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            source_limit,
        } => document::insert_select(
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        ),
        // The following variants are intercepted upstream (Array CRDT ops by
        // `from_replicated_entry`, CalvinReadResult by the apply loop) and never
        // dispatched through the generic Data Plane path. These arms exist only
        // to keep the match exhaustive.
        ReplicatedWrite::ArrayOp { .. } => {
            return Err(crate::Error::Internal {
                detail: "ArrayOp reached to_physical_plan (should have been intercepted)".into(),
            });
        }
        ReplicatedWrite::ArraySchema { .. } => {
            return Err(crate::Error::Internal {
                detail: "ArraySchema reached to_physical_plan (should have been intercepted)"
                    .into(),
            });
        }
        ReplicatedWrite::CalvinReadResult { .. } => {
            return Err(crate::Error::Internal {
                detail: "CalvinReadResult reached to_physical_plan (should have been intercepted)"
                    .into(),
            });
        }
        ReplicatedWrite::ConstraintChange {
            collection,
            op,
            constraint_version,
            constraints,
        } => crdt::constraint_change(collection, op, *constraint_version, constraints),
    })
}
