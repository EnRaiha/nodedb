// SPDX-License-Identifier: BUSL-1.1

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;

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
    let doc_id_compat: Option<String> = None;
    let entry = zerompk::to_msgpack_vec(&(
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
        detail: format!("wal vector put (sync): {e}"),
    })?;
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
    let entry = zerompk::to_msgpack_vec(&(collection, surrogate.as_u32(), field_name, provenance))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal vector delete by surrogate (sync): {e}"),
        })?;
    let lsn = wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?;
    Ok(lsn)
}
