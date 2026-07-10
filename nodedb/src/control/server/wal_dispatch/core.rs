// SPDX-License-Identifier: BUSL-1.1

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::credential::CredentialStore;
use crate::engine::array::wal::{
    ArrayDeleteCell, ArrayDeletePayload, ArrayPutPayload, encode_delete_with_version,
    encode_put_with_version,
};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::{ArrayOp, CrdtOp, DocumentOp, GraphOp, TimeseriesOp};

use super::super::wal_dispatch_kv;

/// Outcome of [`wal_append_if_write`] / [`wal_append_if_write_with_creds`]:
/// the allocated WAL LSN (if a durable record was appended) and, for a
/// TTL-bearing KV write, the wall-clock instant resolved at append time.
///
/// `resolved_now_ms` mirrors `lsn`'s cross-plane contract: the caller stamps
/// it onto the dispatched `Request` (via `WriteDispatch` / `DataPlaneDispatch`)
/// so the Data Plane's live apply installs the SAME instant the durable WAL
/// record carries, rather than re-reading the wall clock at apply time — the
/// two must agree by construction, or a crash between WAL append and apply
/// lets replay recompute `now_ms` at restart time and drift the TTL's expiry
/// forward by the crash-to-restart delay. A plain struct rather than a
/// `(Option<Lsn>, Option<u64>)` tuple: both fields are the same "maybe a
/// number" shape and trivially swappable by position across the several call
/// sites this threads through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalAppendOutcome {
    /// WAL LSN allocated for this write, or `None` for reads / control ops /
    /// WAL-bypassed writes.
    pub lsn: Option<crate::types::Lsn>,
    /// Wall-clock instant (ms since epoch) resolved for a TTL-bearing KV
    /// write's `expire_at_ms`. `None` for every non-KV plan and every KV write
    /// without a TTL.
    pub resolved_now_ms: Option<u64>,
}

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
///
/// Returns the WAL LSN allocated for writes it appended (`Some`), or `None`
/// for reads / control ops that need no WAL record. The caller stamps the
/// returned LSN onto the dispatched `Request` so the Data Plane can record the
/// committed write version.
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<WalAppendOutcome> {
    wal_append_if_write_with_creds(wal, tenant_id, vshard_id, database_id, plan, None)
}

/// WAL append with optional credential store for timeseries WAL bypass check.
///
/// Returns the appended write's WAL LSN (`Some`), or `None` for reads / control
/// ops / WAL-bypassed writes.
pub fn wal_append_if_write_with_creds(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    credentials: Option<&CredentialStore>,
) -> crate::Result<WalAppendOutcome> {
    let mut resolved_now_ms: Option<u64> = None;
    let appended: Option<crate::types::Lsn> = match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            pk_bytes: _,
        }) => {
            // The row's global surrogate is appended as a trailing element so
            // startup replay can rebuild any secondary vector index bound to
            // this document with its real cross-engine identity (headless
            // local ids otherwise leak into vector-search projections after a
            // restart). Appending keeps the record an arity-cascade extension
            // of the legacy `(collection, document_id, value, provenance)`
            // shape, which older decoders still parse.
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                value,
                prov,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal point put: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent: _,
            surrogate,
        }) => {
            // Trailing surrogate element (see `PointPut` above) — carries the
            // row's global identity for restart-time vector-index rebuild.
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                value,
                prov,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal point insert: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // All vector-engine ops route through one exhaustive `VectorOp` match
        // so a future write variant cannot silently become non-durable.
        PhysicalPlan::Vector(op) => {
            super::vector::wal_append_vector_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            delta,
            provenance,
            ..
        }) => {
            // Wrap delta bytes with collection and provenance so the replay decoder can
            // reconstruct idempotency context and route to the correct collection.
            let payload = crate::wal::CrdtDeltaWalPayload {
                bytes: delta.clone(),
                collection: Some(collection.clone()),
                provenance: provenance.clone(),
            };
            let crdt_payload =
                zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal crdt delta: {e}"),
                })?;
            Some(wal.append_crdt_delta(tenant_id, vshard_id, database_id, &crdt_payload)?)
        }
        PhysicalPlan::Crdt(CrdtOp::ImportSnapshot {
            collection, bytes, ..
        }) => {
            // Per-collection snapshot import. `import_snapshot_bytes` and
            // `apply_committed_delta` are the same idempotent Loro `state.import`,
            // so the snapshot rides the CRDT delta record and replays identically,
            // routed to the same collection. No provenance (not a per-doc sync op).
            let payload = crate::wal::CrdtDeltaWalPayload {
                bytes: bytes.clone(),
                collection: Some(collection.clone()),
                provenance: None,
            };
            let crdt_payload =
                zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal crdt snapshot import: {e}"),
                })?;
            Some(wal.append_crdt_delta(tenant_id, vshard_id, database_id, &crdt_payload)?)
        }
        PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection,
            document_id,
            list_path,
            index,
            fields_json,
            surrogate: _,
        }) => {
            // The Data Plane never appends to the WAL and the Control Plane
            // has no `LoroDoc` to compute a delta from, so the intent is
            // logged here and re-executed deterministically at replay
            // (see `CrdtListOpWalRecord`'s doc comment).
            let payload = crate::wal::CrdtListOpWalRecord::Insert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                index: *index as u64,
                fields_json: fields_json.clone(),
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some(wal.append_crdt_list_op(tenant_id, vshard_id, database_id, &bytes)?)
        }
        PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection,
            document_id,
            list_path,
            index,
            surrogate: _,
        }) => {
            let payload = crate::wal::CrdtListOpWalRecord::Delete {
                collection: collection.clone(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                index: *index as u64,
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some(wal.append_crdt_list_op(tenant_id, vshard_id, database_id, &bytes)?)
        }
        PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
            surrogate: _,
        }) => {
            let payload = crate::wal::CrdtListOpWalRecord::Move {
                collection: collection.clone(),
                document_id: document_id.clone(),
                list_path: list_path.clone(),
                from_index: *from_index as u64,
                to_index: *to_index as u64,
            };
            let bytes = encode_crdt_list_op_payload(payload)?;
            Some(wal.append_crdt_list_op(tenant_id, vshard_id, database_id, &bytes)?)
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
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
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
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels }) => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_set(tenant_id, vshard_id, database_id, &entry)?)
        }
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels }) => {
            let entry = super::encode_graph_node_label_payload(node_id, labels)?;
            Some(wal.append_graph_node_label_remove(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Batched edge writes (`CREATE GRAPH INDEX` build / rollback). Each
        // edge is appended as its own single-edge `Put`/`Delete` record,
        // byte-identical in shape to the non-batch `EdgePut`/`EdgeDelete`
        // arms above — see `wal_dispatch/graph.rs` for the encoding and the
        // documented last-LSN-as-watermark / explicit-empty-batch contract.
        PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges }) => {
            super::graph::wal_append_graph_edge_put_batch(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                edges,
            )?
        }
        PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => {
            super::graph::wal_append_graph_edge_delete_batch(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                edges,
            )?
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
            let wal_payload = super::timeseries::encode_columnar_batch_payload(
                collection,
                payload,
                provenance.as_ref(),
                surrogates,
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        PhysicalPlan::Columnar(nodedb_physical::physical_plan::ColumnarOp::Update {
            collection,
            filters,
            updates,
        }) => {
            // Predicate UPDATE has no row post-image at append time (the
            // matching set is only known once the Data Plane scans current
            // state), so the durable record carries the predicate itself;
            // replay re-executes it through the same live handler. See
            // `encode_columnar_dml_payload` for the record shape and the
            // idempotence constraint on replay ordering.
            let wal_payload =
                super::timeseries::encode_columnar_dml_payload(collection, true, filters, updates)?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        PhysicalPlan::Columnar(nodedb_physical::physical_plan::ColumnarOp::Delete {
            collection,
            filters,
        }) => {
            // Mirrors the `Update` arm above; delete is idempotent (mark +
            // remove from PK index), so unlike update it tolerates a
            // hypothetical double-apply, but replay still runs it exactly
            // once by construction.
            let wal_payload =
                super::timeseries::encode_columnar_dml_payload(collection, false, filters, &[])?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
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
                && let Ok(Some(coll)) = creds.catalog().get_collection(
                    DatabaseId::DEFAULT,
                    tenant_id.as_u64(),
                    collection,
                )
                && let Some(config) = coll.get_timeseries_config()
                && config.get("wal").and_then(|v| v.as_str()) == Some("false")
            {
                // WAL bypassed — acceptable data loss of last flush interval on crash.
                return Ok(WalAppendOutcome {
                    lsn: None,
                    resolved_now_ms: None,
                });
            }

            // Provenance is appended last; older 3-element decoders ignore
            // the trailing field via their arity-fallback paths.
            let wal_payload = super::timeseries::encode_timeseries_batch_payload(
                collection,
                payload,
                provenance.as_ref(),
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        // KV write operations — delegated to wal_dispatch_kv.
        PhysicalPlan::Kv(kv_op) => {
            let outcome =
                wal_dispatch_kv::wal_append_kv_op(wal, tenant_id, vshard_id, database_id, kv_op)?;
            resolved_now_ms = outcome.resolved_now_ms;
            outcome.lsn
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
            Some(wal.append_array_put(tenant_id, vshard_id, database_id, &bytes)?)
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
            Some(wal.append_array_delete(tenant_id, vshard_id, database_id, &bytes)?)
        }
        // All Text-engine ops route through one match over `TextOp` (mirrors
        // `PhysicalPlan::Vector(op)` above) so a future write variant cannot
        // silently become non-durable.
        PhysicalPlan::Text(op) => {
            super::text::wal_append_text_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        // Likewise for Spatial.
        PhysicalPlan::Spatial(op) => {
            super::spatial::wal_append_spatial_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        // Non-vector reads and control commands: no WAL needed. Every vector
        // write is handled above by the exhaustive `wal_append_vector_op`, so
        // this arm covers only non-vector read/scan ops, the non-write sub-ops
        // of Document / Graph / Kv / Columnar / Timeseries, and the Query /
        // Meta / ClusterArray plans (whose durable writes, where any, are
        // logged on their own dedicated paths).
        _ => None,
    };
    Ok(WalAppendOutcome {
        lsn: appended,
        resolved_now_ms,
    })
}

/// Encode a `CrdtListOpWalRecord` for a `CrdtOp::ListInsert` / `ListDelete` /
/// `ListMove` append. Shared by all three arms above so the msgpack encode +
/// error-mapping logic is written once (mirrors `encode_graph_node_label_payload`).
fn encode_crdt_list_op_payload(payload: crate::wal::CrdtListOpWalRecord) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal crdt list op: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{SpatialOp, TextOp};
    use nodedb_types::Surrogate;
    use nodedb_types::geometry::Geometry;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn last_record_of_type(
        wal: &WalManager,
        record_type: nodedb_wal::record::RecordType,
    ) -> nodedb_wal::WalRecord {
        wal.sync().expect("sync wal");
        wal.replay()
            .expect("read wal")
            .into_iter()
            .rfind(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(record_type)
            })
            .expect("expected record of this type")
    }

    #[test]
    fn fts_index_doc_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: "docs".to_string(),
            surrogate: Surrogate::new(7),
            text: "hello world".to_string(),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "FtsIndexDoc must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::FtsIndex);
        let decoded =
            nodedb_wal::record::FtsIndexPayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "docs");
        assert_eq!(decoded.text, "hello world");
        assert_eq!(
            decoded.doc_id,
            crate::engine::document::store::surrogate_to_doc_id(Surrogate::new(7))
        );
    }

    #[test]
    fn fts_delete_doc_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: "docs".to_string(),
            surrogate: Surrogate::new(7),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "FtsDeleteDoc must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::FtsDelete);
        let decoded =
            nodedb_wal::record::FtsDeletePayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "docs");
        assert_eq!(
            decoded.doc_id,
            crate::engine::document::store::surrogate_to_doc_id(Surrogate::new(7))
        );
    }

    #[test]
    fn spatial_insert_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: "places".to_string(),
            field: "loc".to_string(),
            surrogate: Surrogate::new(9),
            geometry: Geometry::point(10.0, 20.0),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SpatialOp::Insert must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::SpatialPut);
        let decoded =
            nodedb_wal::record::SpatialPutPayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
        let geometry: Geometry =
            zerompk::from_msgpack(&decoded.geometry_bytes).expect("decode geometry");
        assert_eq!(geometry, Geometry::point(10.0, 20.0));
    }

    #[test]
    fn spatial_delete_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: "places".to_string(),
            field: "loc".to_string(),
            surrogate: Surrogate::new(9),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SpatialOp::Delete must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::SpatialDelete);
        let decoded =
            nodedb_wal::record::SpatialDeletePayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
    }
}
