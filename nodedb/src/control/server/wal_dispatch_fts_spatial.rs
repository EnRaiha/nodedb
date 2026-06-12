// SPDX-License-Identifier: BUSL-1.1

//! WAL append helpers for FTS and Spatial sync ingest paths.
//!
//! Each helper accepts a prebuilt payload struct, serializes it, and appends
//! to the WAL via `WalManager`.  The CP allocates the LSN here; the gate runs
//! Data-Plane-side at the apply handler.
//!
//! Callers are responsible for constructing the payload (which bundles
//! provenance + all operation fields) before calling these helpers.

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append an FTS index operation to the WAL and return the assigned LSN.
///
/// The `payload` already carries provenance so replay routes through
/// `execute_fts_index_doc` and the idempotency gate fires on replay.
pub fn wal_append_fts_index(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    payload: &nodedb_wal::record::FtsIndexPayload,
) -> crate::Result<nodedb_types::Lsn> {
    let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
    let lsn = wal.append_fts_index(tenant_id, vshard_id, database_id, &bytes)?;
    Ok(lsn)
}

/// Append an FTS delete operation to the WAL and return the assigned LSN.
///
/// The `payload` already carries provenance so replay routes through
/// `execute_fts_delete_doc` and the idempotency gate fires on replay.
pub fn wal_append_fts_delete(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    payload: &nodedb_wal::record::FtsDeletePayload,
) -> crate::Result<nodedb_types::Lsn> {
    let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
    let lsn = wal.append_fts_delete(tenant_id, vshard_id, database_id, &bytes)?;
    Ok(lsn)
}

/// Append a spatial put (insert) to the WAL and return the assigned LSN.
///
/// The `payload` carries provenance and the msgpack-encoded `Geometry`
/// (identical to what `SpatialInsertMsg.geometry_bytes` carries).
pub fn wal_append_spatial_put(
    wal: &WalManager,
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
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    payload: &nodedb_wal::record::SpatialDeletePayload,
) -> crate::Result<nodedb_types::Lsn> {
    let bytes = payload.to_bytes().map_err(crate::Error::Wal)?;
    let lsn = wal.append_spatial_delete(tenant_id, vshard_id, database_id, &bytes)?;
    Ok(lsn)
}
