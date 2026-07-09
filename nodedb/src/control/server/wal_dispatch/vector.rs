// SPDX-License-Identifier: BUSL-1.1

use crate::types::{DatabaseId, TenantId, VShardId};
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
