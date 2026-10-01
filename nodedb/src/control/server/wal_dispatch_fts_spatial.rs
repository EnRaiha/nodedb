// SPDX-License-Identifier: BUSL-1.1

//! Spatial WAL payload builders and append helpers.
//!
//! Each append helper accepts a prebuilt payload struct, serializes it, and
//! appends to the WAL through the caller's `WalAppender`. The idempotency gate
//! runs Data-Plane-side at the apply handler.

use nodedb_types::Surrogate;
use nodedb_types::geometry::Geometry;
use nodedb_types::sync::wire::SyncProvenance;

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Build a `SpatialPutPayload` from raw op fields, msgpack-encoding the
/// geometry.
///
/// This is the ONE builder for the shape: both the autocommit WAL append
/// and the transaction-resolve serializer call it so
/// producer and `replay_spatial_wal` never drift. `doc_id` is derived from
/// `surrogate` via `StorageKey::for_surrogate`, matching the hex-encoded key
/// both the R-tree entry and the sparse document body are keyed by.
pub(crate) fn encode_spatial_put_payload(
    collection: &str,
    field: &str,
    surrogate: Surrogate,
    geometry: &Geometry,
    provenance: &SyncProvenance,
) -> crate::Result<nodedb_wal::record::SpatialPutPayload> {
    let doc_id = crate::engine::document::store::StorageKey::for_surrogate(surrogate).to_string();
    let geometry_bytes =
        zerompk::to_msgpack_vec(geometry).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("spatial put: encode geometry for WAL: {e}"),
        })?;
    Ok(nodedb_wal::record::SpatialPutPayload::new(
        provenance.clone(),
        collection,
        field,
        doc_id,
        geometry_bytes,
    ))
}

/// Build a `SpatialDeletePayload` from raw op fields. The ONE builder for the
/// shape, shared by the autocommit WAL append and transaction resolve.
/// `None` names a key its home never bound.
pub(crate) fn encode_spatial_delete_payload(
    collection: &str,
    field: &str,
    surrogate: Option<Surrogate>,
    provenance: &SyncProvenance,
) -> nodedb_wal::record::SpatialDeletePayload {
    let doc_id =
        surrogate.map(|s| crate::engine::document::store::StorageKey::for_surrogate(s).to_string());
    nodedb_wal::record::SpatialDeletePayload::new(provenance.clone(), collection, field, doc_id)
}

/// Append a spatial put (insert) to the WAL and return the assigned LSN.
///
/// The `payload` carries provenance and the msgpack-encoded `Geometry`
/// (identical to what `SpatialInsertMsg.geometry_bytes` carries).
pub fn wal_append_spatial_put(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    payload: &nodedb_wal::record::SpatialPutPayload,
) -> crate::Result<nodedb_types::Lsn> {
    let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
    let lsn = wal.append_spatial_put(tenant_id, vshard_id, database_id, &bytes)?;
    Ok(lsn)
}

/// Append a spatial delete to the WAL and return the assigned LSN.
pub fn wal_append_spatial_delete(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    payload: &nodedb_wal::record::SpatialDeletePayload,
) -> crate::Result<nodedb_types::Lsn> {
    let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
    let lsn = wal.append_spatial_delete(tenant_id, vshard_id, database_id, &bytes)?;
    Ok(lsn)
}
