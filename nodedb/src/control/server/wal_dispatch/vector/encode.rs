// SPDX-License-Identifier: BUSL-1.1

//! Payload encoders for every vector-engine WAL record shape.
//!
//! Each shape has exactly ONE encoder here, shared by the autocommit append
//! path ([`super::append`]), the sync append helpers, and the
//! transaction-resolve serializer, so producer and replay never drift.

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

/// Encode the payload of a `VectorIndexDrop` WAL record.
///
/// Produces the 2-element shape `(collection, field_name)` — the same
/// `(collection, field)` identity `SetParams` records, so replay can evict
/// exactly the index the params record created.
pub(crate) fn encode_vector_index_drop_payload(
    collection: &str,
    field_name: &str,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(collection, field_name)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal vector index drop: {e}"),
    })
}
