// SPDX-License-Identifier: BUSL-1.1

use nodedb_physical::physical_plan::VectorOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Encode the payload of a `VectorPut` WAL record for a single insert.
///
/// Produces the 7-element shape
/// `(collection, vector, dim, field_name, doc_id_compat, surrogate_u32, provenance)`
/// — the canonical vector-insert encoding. `doc_id_compat` is always `None`
/// (a compatibility slot for pre-surrogate follower decoders). This is the ONE
/// encoder for the shape: both the autocommit `VectorOp::Insert` arm in
/// `wal_append_if_write_with_creds`, the sync `wal_append_vector_put`, and the
/// transaction-resolve serializer call it so producer and replay never drift.
pub(crate) fn encode_vector_put_payload(
    collection: &str,
    vector: &[f32],
    dim: usize,
    field_name: &str,
    surrogate: nodedb_types::Surrogate,
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
) -> crate::Result<Vec<u8>> {
    let doc_id_compat: Option<String> = None;
    zerompk::to_msgpack_vec(&(
        collection,
        vector,
        dim,
        field_name,
        doc_id_compat,
        surrogate.as_u32(),
        provenance,
    ))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal vector insert: {e}"),
    })
}

/// Encode the payload of a `VectorPut` WAL record for a headless batch insert.
///
/// Produces the 3-element shape `(collection, vectors, dim)` that the batch arm
/// of `replay_vector_wal` decodes. Batch inserts carry no per-vector surrogate
/// on this shape (mirrors the autocommit `VectorOp::BatchInsert` arm).
pub(crate) fn encode_vector_batch_put_payload(
    collection: &str,
    vectors: &[Vec<f32>],
    dim: usize,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, vectors, dim)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal vector batch insert: {e}"),
    })
}

/// Encode the payload of a `VectorDelete` WAL record for a delete-by-node-id.
///
/// Produces the 3-element shape `(collection, vector_id, provenance)` with
/// `provenance = None`, matching the autocommit `VectorOp::Delete` arm. The
/// legacy 2-element decoder still parses the leading fields.
pub(crate) fn encode_vector_delete_payload(
    collection: &str,
    vector_id: u32,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, vector_id, prov)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal vector delete: {e}"),
        }
    })
}

/// Encode the payload of a `VectorDelete` WAL record for a delete-by-surrogate.
///
/// Produces the 4-element shape `(collection, surrogate_u32, field_name,
/// provenance)` the surrogate-aware arm of `replay_vector_wal` decodes,
/// routing to `execute_vector_delete_by_surrogate`.
pub(crate) fn encode_vector_delete_by_surrogate_payload(
    collection: &str,
    surrogate: nodedb_types::Surrogate,
    field_name: &str,
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, surrogate.as_u32(), field_name, provenance)).map_err(
        |e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal vector delete by surrogate: {e}"),
        },
    )
}

/// Operation fields for a vector put WAL record.
///
/// Groups the vector-identity and provenance fields that together describe a
/// single vector insert, reducing the call-site argument count.
pub struct VectorPutWalArgs<'a> {
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
}

/// Operation fields for a vector delete-by-surrogate WAL record.
///
/// Groups the collection, surrogate, field, and provenance fields that
/// together identify a single vector deletion.
pub struct VectorDeleteWalArgs<'a> {
    pub collection: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub field_name: &'a str,
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
}

/// Append a vector put (insert) to the WAL and return the assigned LSN.
///
/// Encodes `(collection, vector, dim, field_name, doc_id_compat, surrogate_u32, provenance)`
/// exactly as the non-sync `VectorOp::Insert` arm in `wal_append_if_write_with_creds` does,
/// so replay decodes both paths with the same 7-element shape.
pub fn wal_append_vector_put(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: VectorPutWalArgs<'_>,
) -> crate::Result<nodedb_types::Lsn> {
    let VectorPutWalArgs {
        collection,
        vector,
        dim,
        field_name,
        surrogate,
        provenance,
    } = args;
    let entry =
        encode_vector_put_payload(collection, vector, dim, field_name, surrogate, provenance)?;
    let lsn = wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?;
    Ok(lsn)
}

/// Encode the payload of a `VectorDirectUpsert` WAL record.
///
/// Produces the 8-element shape
/// `(collection, field, surrogate_u32, vector, payload, quantization,
/// storage_dtype, payload_indexes)` — the full post-image a vector-primary
/// insert needs so replay can reconstruct the HNSW node, the payload bitmap
/// indexes, the sparse-store body, and the collection's quantization /
/// payload-index registration. `dim` is not stored; replay derives it from
/// `vector.len()`, exactly as the live handler does. This is the ONE encoder
/// for the shape so producer and replay never drift.
pub(crate) struct VectorDirectUpsertPayload<'a> {
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

pub(crate) fn encode_vector_direct_upsert_payload(
    args: VectorDirectUpsertPayload<'_>,
) -> crate::Result<Vec<u8>> {
    let VectorDirectUpsertPayload {
        collection,
        field,
        surrogate,
        vector,
        payload,
        quantization,
        storage_dtype,
        payload_indexes,
    } = args;
    zerompk::to_msgpack_vec(&(
        collection,
        field,
        surrogate.as_u32(),
        vector,
        payload,
        quantization,
        storage_dtype,
        payload_indexes,
    ))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal vector direct upsert: {e}"),
    })
}

/// Encode the payload of a `SparseVectorPut` WAL record.
///
/// Produces the 4-element shape `(collection, field_name, doc_id, entries)`
/// where `entries` are the `(dimension, weight)` pairs. Replay re-inserts via
/// the sparse index's upsert-by-`doc_id`, so re-applying a record already in
/// a restored checkpoint is idempotent. This is the ONE encoder for the shape.
pub(crate) fn encode_sparse_vector_put_payload(
    collection: &str,
    field_name: &str,
    doc_id: &str,
    entries: &[(u32, f32)],
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, field_name, doc_id, entries)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal sparse vector put: {e}"),
        }
    })
}

/// Encode the payload of a `SparseVectorDelete` WAL record.
///
/// Produces the 3-element shape `(collection, field_name, doc_id)`. Replay
/// removes the document by id; deleting an absent document is a no-op, so
/// re-applying over a restored checkpoint is idempotent.
pub(crate) fn encode_sparse_vector_delete_payload(
    collection: &str,
    field_name: &str,
    doc_id: &str,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, field_name, doc_id)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal sparse vector delete: {e}"),
        }
    })
}

/// Encode the payload of a `MultiVectorPut` WAL record.
///
/// Produces the 6-element shape `(collection, field_name,
/// document_surrogate_u32, vectors_flat, count, dim)`, matching the fields the
/// multi-vector insert handler consumes. This is the ONE encoder for the shape.
pub(crate) fn encode_multi_vector_put_payload(
    collection: &str,
    field_name: &str,
    document_surrogate: nodedb_types::Surrogate,
    vectors_flat: &[f32],
    count: usize,
    dim: usize,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(
        collection,
        field_name,
        document_surrogate.as_u32(),
        vectors_flat,
        count,
        dim,
    ))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal multi-vector put: {e}"),
    })
}

/// Encode the payload of a `MultiVectorDelete` WAL record.
///
/// Produces the 3-element shape `(collection, field_name,
/// document_surrogate_u32)`.
pub(crate) fn encode_multi_vector_delete_payload(
    collection: &str,
    field_name: &str,
    document_surrogate: nodedb_types::Surrogate,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, field_name, document_surrogate.as_u32())).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal multi-vector delete: {e}"),
        }
    })
}

/// Append the WAL record for a single vector-engine physical op, returning the
/// allocated LSN for writes (`Some`) or `None` for reads / index-maintenance
/// ops that carry no durable per-write effect.
///
/// The match over [`VectorOp`] is **exhaustive**: a new variant fails to
/// compile until its durability is decided here, so a future write can never
/// silently become non-durable (the class of bug this function was hardened
/// against). Read and maintenance ops map to `None` explicitly, by name.
pub(crate) fn wal_append_vector_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &VectorOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        } => {
            // The local-WAL record carries the surrogate as a u32 so recovery
            // can rebind without consulting the catalog. See
            // `encode_vector_put_payload` for the compatibility slot.
            let entry = encode_vector_put_payload(
                collection,
                vector,
                *dim,
                field_name,
                *surrogate,
                provenance.as_ref(),
            )?;
            Some(wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        } => {
            let entry = encode_vector_batch_put_payload(collection, vectors, *dim)?;
            Some(wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::Delete {
            collection,
            vector_id,
        } => {
            let entry = encode_vector_delete_payload(collection, *vector_id)?;
            Some(wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => {
            // Durable by node-independent surrogate. The sync-inbound path logs
            // this via `wal_append_vector_delete_by_surrogate` before dispatch;
            // logging it here too keeps every path that reaches this function
            // durable without double-logging (the sync path bypasses it).
            let entry = encode_vector_delete_by_surrogate_payload(
                collection,
                *surrogate,
                field_name,
                provenance.as_ref(),
            )?;
            Some(wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::SetParams {
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => {
            // Fields are appended, never reordered, so older 4-/8-/9-element
            // WAL records still decode (replay reads the leading positions
            // first and falls back on the shorter shapes).
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
                field_name,
                dim,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            Some(wal.append_vector_params(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
        } => {
            let entry = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                collection,
                field,
                surrogate: *surrogate,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
            })?;
            Some(wal.append_vector_direct_upsert(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => {
            let entry = encode_sparse_vector_put_payload(collection, field_name, doc_id, entries)?;
            Some(wal.append_sparse_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => {
            let entry = encode_sparse_vector_delete_payload(collection, field_name, doc_id)?;
            Some(wal.append_sparse_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => {
            let entry = encode_multi_vector_put_payload(
                collection,
                field_name,
                *document_surrogate,
                vectors,
                *count,
                *dim,
            )?;
            Some(wal.append_multi_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => {
            let entry =
                encode_multi_vector_delete_payload(collection, field_name, *document_surrogate)?;
            Some(wal.append_multi_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Reads: no durable effect.
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::QueryStats { .. } => None,
        // Index maintenance: reorganizes an index that is itself rebuilt from
        // the replayed writes plus checkpoints. No logical row is created or
        // destroyed, so no durable record is needed.
        VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. } => None,
    };
    Ok(appended)
}

/// Append a vector delete-by-surrogate to the WAL and return the assigned LSN.
///
/// Encodes `(collection, surrogate_u32, field_name, provenance)` as a `VectorDelete`
/// record. The replay decoder uses a surrogate-aware arm (4-element shape) that maps
/// back to `execute_vector_delete_by_surrogate`; the legacy 2-element and 3-element
/// delete arms fall through to direct node-id deletion and remain backward-compatible.
pub fn wal_append_vector_delete_by_surrogate(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: VectorDeleteWalArgs<'_>,
) -> crate::Result<nodedb_types::Lsn> {
    let VectorDeleteWalArgs {
        collection,
        surrogate,
        field_name,
        provenance,
    } = args;
    let entry =
        encode_vector_delete_by_surrogate_payload(collection, surrogate, field_name, provenance)?;
    let lsn = wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?;
    Ok(lsn)
}
