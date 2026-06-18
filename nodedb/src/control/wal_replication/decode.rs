// SPDX-License-Identifier: BUSL-1.1

//! Convert committed ReplicatedWrite entries back to PhysicalPlan for Data Plane execution.

use super::decode_sync_engines;
use super::types::{ReplicatedEntry, ReplicatedWrite};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::surrogate::SurrogateAssigner;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, VectorOp};

///
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
    let plan = to_physical_plan(&entry.write, assigner)?;
    Ok(Some((
        TenantId::new(entry.tenant_id),
        VShardId::new(entry.vshard_id),
        plan,
    )))
}

fn assign_or_zero(
    assigner: Option<&SurrogateAssigner>,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    match assigner {
        Some(a) => a.assign(collection, pk_bytes),
        None => Ok(nodedb_types::Surrogate::ZERO),
    }
}

/// Convert a ReplicatedWrite back into a PhysicalPlan for Data Plane execution.
fn to_physical_plan(
    write: &ReplicatedWrite,
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<PhysicalPlan> {
    Ok(match write {
        ReplicatedWrite::PointPut {
            collection,
            document_id,
            value,
            surrogate,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => a.bind(collection, &pk_bytes, carried)?,
                None => carried,
            };
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                surrogate,
                pk_bytes,
            })
        }
        ReplicatedWrite::PointInsert {
            collection,
            document_id,
            value,
            if_absent,
            surrogate,
        } => {
            let pk_bytes = document_id.as_bytes();
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => a.bind(collection, pk_bytes, carried)?,
                None => carried,
            };
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: collection.clone(),
                document_id: document_id.clone(),
                value: value.clone(),
                if_absent: *if_absent,
                surrogate,
            })
        }
        ReplicatedWrite::PointDelete {
            collection,
            document_id,
            surrogate,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            // Bind the carried (authoritative) surrogate even on a delete:
            // a delete that applies before its insert still installs the
            // correct identity, and the engine op is a no-op on a row that
            // does not exist yet. Re-deriving via `lookup` would diverge.
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => a.bind(collection, &pk_bytes, carried)?,
                None => carried,
            };
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: collection.clone(),
                document_id: document_id.clone(),
                surrogate,
                pk_bytes,
                returning: None,
            })
        }
        ReplicatedWrite::PointUpdate {
            collection,
            document_id,
            updates,
            surrogate,
        } => {
            let pk_bytes = document_id.as_bytes().to_vec();
            // Same rationale as `PointDelete`: bind the carried surrogate so
            // identity is authoritative regardless of apply ordering.
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => a.bind(collection, &pk_bytes, carried)?,
                None => carried,
            };
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection: collection.clone(),
                document_id: document_id.clone(),
                surrogate,
                pk_bytes,
                updates: updates.clone(),
                returning: None,
            })
        }
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance: prov_bytes,
        } => {
            // Bind the leader-assigned surrogate verbatim — never re-allocate.
            // With a PK we bind by it; headless inserts self-key by the
            // surrogate's own big-endian bytes (mirrors `assign_anonymous`).
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => match pk_bytes {
                    Some(pk) => a.bind(collection, pk, carried)?,
                    None => a.bind(collection, &carried.as_u32().to_be_bytes(), carried)?,
                },
                None => carried,
            };
            let provenance = decode_sync_engines::decode_provenance(prov_bytes)?;
            PhysicalPlan::Vector(VectorOp::Insert {
                collection: collection.clone(),
                vector: vector.clone(),
                dim: *dim,
                field_name: field_name.clone(),
                surrogate,
                pk_bytes: pk_bytes.clone(),
                provenance,
            })
        }
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        } => {
            // The carried surrogate vector MUST be 1:1 with the vectors.
            // A mismatch is a corrupt/incompatible entry — fail loud rather
            // than truncate or zip-shorten (which would silently drop rows
            // or mis-bind identities).
            if surrogates.len() != vectors.len() {
                return Err(crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "VectorBatchInsert surrogate/vector count mismatch: {} surrogates, {} vectors",
                        surrogates.len(),
                        vectors.len()
                    ),
                });
            }
            // Bind each element by its self-key and use the *authoritative*
            // returned surrogate in the plan. Each is unique by construction
            // so first-wins returns the carried value, but consuming the
            // return keeps this consistent with the single-row arms.
            // Iterate `surrogates` (the raw u32 wire vec) directly to avoid
            // a needless intermediate `Vec<Surrogate>` allocation.
            let surrogates: Vec<nodedb_types::Surrogate> = match assigner {
                Some(a) => surrogates
                    .iter()
                    .map(|&raw| {
                        let c = nodedb_types::Surrogate::new(raw);
                        a.bind(collection, &c.as_u32().to_be_bytes(), c)
                    })
                    .collect::<crate::Result<Vec<_>>>()?,
                None => surrogates
                    .iter()
                    .map(|&raw| nodedb_types::Surrogate::new(raw))
                    .collect(),
            };
            PhysicalPlan::Vector(VectorOp::BatchInsert {
                collection: collection.clone(),
                vectors: vectors.clone(),
                dim: *dim,
                surrogates,
            })
        }
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => PhysicalPlan::Vector(VectorOp::Delete {
            collection: collection.clone(),
            vector_id: *vector_id,
        }),
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
        } => PhysicalPlan::Vector(VectorOp::SetParams {
            collection: collection.clone(),
            field_name: field_name.clone(),
            m: *m,
            ef_construction: *ef_construction,
            metric: metric.clone(),
            index_type: index_type.clone(),
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        }),
        ReplicatedWrite::CrdtApply {
            collection,
            document_id,
            delta,
            peer_id,
            provenance: prov_bytes,
        } => {
            let surrogate = assign_or_zero(assigner, collection, document_id.as_bytes())?;
            let provenance = decode_sync_engines::decode_provenance(prov_bytes)?;
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: collection.clone(),
                document_id: document_id.clone(),
                delta: delta.clone(),
                peer_id: *peer_id,
                mutation_id: 0,
                surrogate,
                provenance,
            })
        }
        ReplicatedWrite::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } => {
            let carried_src = nodedb_types::Surrogate::new(*src_surrogate);
            let src_surrogate = match assigner {
                Some(a) => a.bind(collection, src_id.as_bytes(), carried_src)?,
                None => carried_src,
            };
            let carried_dst = nodedb_types::Surrogate::new(*dst_surrogate);
            let dst_surrogate = match assigner {
                Some(a) => a.bind(collection, dst_id.as_bytes(), carried_dst)?,
                None => carried_dst,
            };
            PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: collection.clone(),
                src_id: src_id.clone(),
                label: label.clone(),
                dst_id: dst_id.clone(),
                properties: properties.clone(),
                src_surrogate,
                dst_surrogate,
            })
        }
        ReplicatedWrite::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
        } => PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: collection.clone(),
            src_id: src_id.clone(),
            label: label.clone(),
            dst_id: dst_id.clone(),
        }),
        ReplicatedWrite::KvPut {
            collection,
            key,
            value,
            ttl_ms,
            surrogate,
        } => {
            let carried = nodedb_types::Surrogate::new(*surrogate);
            let surrogate = match assigner {
                Some(a) => a.bind(collection, key, carried)?,
                None => carried,
            };
            PhysicalPlan::Kv(KvOp::Put {
                collection: collection.clone(),
                key: key.clone(),
                value: value.clone(),
                ttl_ms: *ttl_ms,
                surrogate,
            })
        }
        ReplicatedWrite::KvDelete { collection, keys } => PhysicalPlan::Kv(KvOp::Delete {
            collection: collection.clone(),
            keys: keys.clone(),
        }),
        ReplicatedWrite::KvBatchPut {
            collection,
            entries,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::BatchPut {
            collection: collection.clone(),
            entries: entries.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvExpire {
            collection,
            key,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Expire {
            collection: collection.clone(),
            key: key.clone(),
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvPersist { collection, key } => PhysicalPlan::Kv(KvOp::Persist {
            collection: collection.clone(),
            key: key.clone(),
        }),
        ReplicatedWrite::KvIncr {
            collection,
            key,
            delta,
            ttl_ms,
        } => PhysicalPlan::Kv(KvOp::Incr {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
            ttl_ms: *ttl_ms,
        }),
        ReplicatedWrite::KvIncrFloat {
            collection,
            key,
            delta,
        } => PhysicalPlan::Kv(KvOp::IncrFloat {
            collection: collection.clone(),
            key: key.clone(),
            delta: *delta,
        }),
        ReplicatedWrite::KvCas {
            collection,
            key,
            expected,
            new_value,
        } => PhysicalPlan::Kv(KvOp::Cas {
            collection: collection.clone(),
            key: key.clone(),
            expected: expected.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvGetSet {
            collection,
            key,
            new_value,
        } => PhysicalPlan::Kv(KvOp::GetSet {
            collection: collection.clone(),
            key: key.clone(),
            new_value: new_value.clone(),
        }),
        ReplicatedWrite::KvRegisterSortedIndex {
            collection,
            index_name,
            sort_columns,
            key_column,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
        } => PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
            collection: collection.clone(),
            index_name: index_name.clone(),
            sort_columns: sort_columns.clone(),
            key_column: key_column.clone(),
            window_type: window_type.clone(),
            window_timestamp_column: window_timestamp_column.clone(),
            window_start_ms: *window_start_ms,
            window_end_ms: *window_end_ms,
        }),
        ReplicatedWrite::KvDropSortedIndex { index_name } => {
            PhysicalPlan::Kv(KvOp::DropSortedIndex {
                index_name: index_name.clone(),
            })
        }
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
    })
}
