// SPDX-License-Identifier: BUSL-1.1

//! WAL append logic for write operations.
//!
//! Serializes write plans as MessagePack and appends to the appropriate
//! WAL record type. Read operations are no-ops.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::credential::CredentialStore;
use crate::engine::array::wal::{
    ArrayDeleteCell, ArrayDeletePayload, ArrayPutPayload, encode_delete_with_version,
    encode_put_with_version,
};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::{
    ArrayOp, CrdtOp, DocumentOp, GraphOp, TimeseriesOp, VectorOp,
};

use super::wal_dispatch_kv;

pub use super::wal_dispatch_fts_spatial::{
    wal_append_fts_delete, wal_append_fts_index, wal_append_spatial_delete, wal_append_spatial_put,
};

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    wal_append_if_write_with_creds(wal, tenant_id, vshard_id, database_id, plan, None)
}

/// WAL append with optional credential store for timeseries WAL bypass check.
pub fn wal_append_if_write_with_creds(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    credentials: Option<&CredentialStore>,
) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate: _,
            pk_bytes: _,
        }) => {
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry =
                zerompk::to_msgpack_vec(&(collection, document_id, value, prov)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point put: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent: _,
            surrogate: _,
        }) => {
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry =
                zerompk::to_msgpack_vec(&(collection, document_id, value, prov)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal point insert: {e}"),
                    }
                })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        }) => {
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(collection, document_id, prov)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point delete: {e}"),
                }
            })?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        }) => {
            // The local-WAL record carries the surrogate as a u32 so
            // recovery can rebind without consulting the catalog. The
            // `Option<String>` slot remains for follower decoders that
            // pre-date surrogate identity (compatibility shape only —
            // always None on this path). Provenance is appended last so
            // older 6-element decoders can still parse the leading fields.
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
                detail: format!("wal vector insert: {e}"),
            })?;
            wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        }) => {
            let entry = zerompk::to_msgpack_vec(&(collection, vectors, dim)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector batch insert: {e}"),
                }
            })?;
            wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Vector(VectorOp::Delete {
            collection,
            vector_id,
        }) => {
            // Provenance is always None for local delete-by-node-id; appended
            // as trailing element so older 2-element decoders fall back
            // gracefully via the legacy arity arm.
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(collection, vector_id, prov)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal vector delete: {e}"),
                }
            })?;
            wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Crdt(CrdtOp::Apply {
            delta, provenance, ..
        }) => {
            // Wrap delta bytes with provenance so the replay decoder can
            // reconstruct idempotency context. Older decoders that treated
            // the payload as raw bytes will fail to msgpack-decode and fall
            // back to the legacy raw-bytes path.
            let crdt_payload = zerompk::to_msgpack_vec(&(delta, provenance)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal crdt delta: {e}"),
                }
            })?;
            wal.append_crdt_delta(tenant_id, vshard_id, database_id, &crdt_payload)?;
        }
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate: _,
            dst_surrogate: _,
        }) => {
            let entry = zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id, properties))
                .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal edge put: {e}"),
            })?;
            wal.append_put(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        }) => {
            let entry =
                zerompk::to_msgpack_vec(&(collection, src_id, label, dst_id)).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal edge delete: {e}"),
                    }
                })?;
            wal.append_delete(tenant_id, vshard_id, database_id, &entry)?;
        }
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
        }) => {
            // `field_name` is appended last so older 4-/8-element WAL records
            // still decode (the replay reads the leading positions first).
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
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            wal.append_vector_params(tenant_id, vshard_id, database_id, &entry)?;
        }
        PhysicalPlan::Columnar(nodedb_physical::physical_plan::ColumnarOp::Insert {
            collection,
            payload,
            format: _,
            intent: _,
            on_conflict_updates: _,
            surrogates: _,
            schema_bytes: _,
            provenance,
            wal_lsn: _,
        }) => {
            // Provenance is appended last; older 3-element decoders ignore
            // the trailing field via their arity-fallback paths.
            let wal_payload = zerompk::to_msgpack_vec(&(
                "columnar", collection, payload, provenance,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal columnar batch: {e}"),
            })?;
            wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
        }
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format: _,
            provenance,
            ..
        }) => {
            // WAL bypass: skip WAL if collection has wal=false in timeseries_config.
            if let Some(creds) = credentials
                && let Some(catalog) = creds.catalog()
                && let Ok(Some(coll)) =
                    catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), collection)
                && let Some(config) = coll.get_timeseries_config()
                && config.get("wal").and_then(|v| v.as_str()) == Some("false")
            {
                // WAL bypassed — acceptable data loss of last flush interval on crash.
                return Ok(());
            }

            // Provenance is appended last; older 3-element decoders ignore
            // the trailing field via their arity-fallback paths.
            let wal_payload =
                zerompk::to_msgpack_vec(&("timeseries", collection, payload, provenance)).map_err(
                    |e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal timeseries batch: {e}"),
                    },
                )?;
            wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
        }
        // KV write operations — delegated to wal_dispatch_kv.
        PhysicalPlan::Kv(kv_op) => {
            wal_dispatch_kv::wal_append_kv_op(wal, tenant_id, vshard_id, database_id, kv_op)?;
        }
        PhysicalPlan::Array(ArrayOp::Put {
            array_id,
            cells_msgpack,
            wal_lsn: _,
            provenance,
        }) => {
            let cells = zerompk::from_msgpack::<Vec<crate::engine::array::wal::ArrayPutCell>>(
                cells_msgpack,
            )
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal array put decode cells: {e}"),
            })?;
            let payload = ArrayPutPayload {
                array_id: array_id.clone(),
                cells,
                provenance: provenance.clone(),
            };
            let bytes =
                encode_put_with_version(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal array put encode: {e}"),
                })?;
            wal.append_array_put(tenant_id, vshard_id, database_id, &bytes)?;
        }
        PhysicalPlan::Array(ArrayOp::Delete {
            array_id,
            coords_msgpack,
            wal_lsn: _,
            provenance,
        }) => {
            let cells =
                zerompk::from_msgpack::<Vec<ArrayDeleteCell>>(coords_msgpack).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("wal array delete decode cells: {e}"),
                    }
                })?;
            let payload = ArrayDeletePayload {
                array_id: array_id.clone(),
                cells,
                provenance: provenance.clone(),
            };
            let bytes =
                encode_delete_with_version(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal array delete encode: {e}"),
                })?;
            wal.append_array_delete(tenant_id, vshard_id, database_id, &bytes)?;
        }
        // Read operations and control commands: no WAL needed.
        _ => {}
    }
    Ok(())
}

/// Append a timeseries batch to WAL and return the assigned LSN.
///
/// Used by the ILP listener and the sync timeseries handler to propagate the
/// WAL LSN to the Data Plane for proper dedup tracking and `flush_wal_lsn` in
/// partition metadata. Returns `None` if WAL is bypassed for this collection.
///
/// `provenance` is `None` for the ILP direct-ingest path; the sync path passes
/// the frame's `SyncProvenance` so the WAL record carries full idempotency context.
pub fn wal_append_timeseries(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let database_id = DatabaseId::DEFAULT;
    // WAL bypass check.
    if let Some(creds) = credentials
        && let Some(catalog) = creds.catalog()
        && let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), collection)
        && let Some(config) = coll.get_timeseries_config()
        && config.get("wal").and_then(|v| v.as_str()) == Some("false")
    {
        return Ok(None);
    }

    let payload_vec = payload.to_vec();
    let wal_payload = zerompk::to_msgpack_vec(&("timeseries", collection, payload_vec, provenance))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch: {e}"),
        })?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}

/// Append a columnar batch to WAL and return the assigned LSN.
///
/// Mirrors `wal_append_timeseries` but encodes with kind `"columnar"` so the
/// WAL replay decoder routes to `replay_columnar_payload`.
/// Returns `None` if WAL is bypassed (columnar collections do not currently
/// support `wal=false`, so this always returns `Some`).
pub fn wal_append_columnar(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let payload_vec = payload.to_vec();
    let wal_payload = zerompk::to_msgpack_vec(&("columnar", collection, payload_vec, provenance))
        .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar batch: {e}"),
    })?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
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
