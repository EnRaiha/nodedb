// SPDX-License-Identifier: BUSL-1.1

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

use super::super::wal_dispatch_kv;

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
        PhysicalPlan::Crdt(CrdtOp::ImportSnapshot { bytes, .. }) => {
            // Whole-tenant snapshot import. `import_snapshot_bytes` and
            // `apply_committed_delta` are the same idempotent Loro `state.import`,
            // so the snapshot rides the CRDT delta record and replays identically.
            // No provenance to carry (whole-tenant import, not a per-doc sync op).
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let crdt_payload = zerompk::to_msgpack_vec(&(bytes, prov)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal crdt snapshot import: {e}"),
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
            src_surrogate: _,
            dst_surrogate: _,
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
            surrogates,
            schema_bytes: _,
            provenance,
            wal_lsn: _,
        }) => {
            // Encode a map-shaped `ColumnarWalRecord` carrying the per-row
            // cross-engine surrogates so replay restores the exact same
            // identity after a restart. `surrogates` is index-aligned with the
            // rows in `payload`. The map shape is distinct from the legacy
            // 4-tuple array, so old on-disk records still decode via the
            // replay fallback path.
            let record = nodedb_types::columnar::ColumnarWalRecord {
                kind: "columnar".to_string(),
                collection: collection.clone(),
                payload: payload.clone(),
                provenance: provenance.clone(),
                surrogates: surrogates.clone(),
            };
            let wal_payload =
                zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
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
