// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Vector`.

use super::super::decode_sync_engines;
use super::super::types::ReplicatedWrite;
use super::ctx::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::Surrogate;

/// Decode the full `ReplicatedWrite::Vector*` variant group: the four
/// original write shapes (`VectorInsert` / `VectorBatchInsert` /
/// `VectorDelete` / `SetVectorParams`) plus the six sparse / multi-vector /
/// direct-upsert / delete-by-surrogate additions, each delegated to its own
/// function below.
///
/// Delegated from `decode/entry.rs`'s single grouped match arm (all ten
/// `Vector*` patterns dispatch here) so that dispatcher stays under the file
/// size limit. `write` is guaranteed by that caller to already be one of
/// these ten variants — every other `ReplicatedWrite` variant is handled by
/// its own arm in `decode/entry.rs`'s exhaustive match and never reaches
/// here; the trailing arm below exists only because `write`'s static type is
/// the full enum, mirroring how `decode/entry.rs` itself handles `ArrayOp` /
/// `ArraySchema` reaching `to_physical_plan` (an internal dispatch-contract
/// violation, not a reachable production state).
pub(super) fn decode_arm(ctx: &DecodeCtx, write: &ReplicatedWrite) -> crate::Result<PhysicalPlan> {
    match write {
        ReplicatedWrite::VectorInsert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance,
        } => insert(
            ctx,
            InsertFields {
                collection,
                vector,
                dim: *dim,
                field_name,
                surrogate: *surrogate,
                pk_bytes,
                provenance,
            },
        ),
        ReplicatedWrite::VectorBatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        } => batch_insert(ctx, collection, vectors, *dim, surrogates),
        ReplicatedWrite::VectorDelete {
            collection,
            vector_id,
        } => Ok(delete(collection, *vector_id)),
        ReplicatedWrite::SetVectorParams {
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
        } => Ok(set_params(SetParamsFields {
            collection,
            field_name,
            dim: *dim,
            m: *m,
            ef_construction: *ef_construction,
            metric,
            index_type,
            pq_m: *pq_m,
            ivf_cells: *ivf_cells,
            ivf_nprobe: *ivf_nprobe,
        })),
        ReplicatedWrite::DropVectorIndex {
            collection,
            field_name,
        } => Ok(drop_index(collection, field_name)),
        ReplicatedWrite::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => Ok(sparse_insert(collection, field_name, doc_id, entries)),
        ReplicatedWrite::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => Ok(sparse_delete(collection, field_name, doc_id)),
        ReplicatedWrite::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => multi_vector_insert(
            ctx,
            collection,
            field_name,
            *document_surrogate,
            vectors,
            *count,
            *dim,
        ),
        ReplicatedWrite::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => Ok(multi_vector_delete(
            collection,
            field_name,
            *document_surrogate,
        )),
        ReplicatedWrite::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => delete_by_surrogate(collection, *surrogate, field_name, provenance),
        ReplicatedWrite::DirectUpsert {
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
        } => direct_upsert(
            ctx,
            DirectUpsertFields {
                collection,
                field,
                surrogate: *surrogate,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
            },
        ),
        _ => Err(crate::Error::Internal {
            detail: "vector::decode_arm called with a non-Vector ReplicatedWrite variant \
                (dispatch bug in decode/entry.rs's grouped Vector match arm)"
                .into(),
        }),
    }
}

/// Bind a leader-assigned surrogate by its own self-key (big-endian bytes of
/// the surrogate itself). Shared by every decode arm here that has no PK
/// sidecar to bind against (headless vector inserts, multi-vector inserts,
/// direct upserts, batch inserts) — each installs the exact carried identity
/// on every replica instead of re-allocating.
fn bind_self_keyed(
    ctx: &DecodeCtx,
    collection: &str,
    carried: Surrogate,
) -> crate::Result<Surrogate> {
    match ctx.assigner {
        Some(a) => a.bind(
            ctx.database_id,
            ctx.tenant_id,
            collection,
            &carried.as_u32().to_be_bytes(),
            carried,
        ),
        None => Ok(carried),
    }
}

/// Fields of the `VectorInsert` wire variant, bundled so [`insert`] stays
/// under the `too_many_arguments` clippy threshold.
pub(super) struct InsertFields<'a> {
    pub(super) collection: &'a str,
    pub(super) vector: &'a [f32],
    pub(super) dim: usize,
    pub(super) field_name: &'a str,
    pub(super) surrogate: u32,
    pub(super) pk_bytes: &'a Option<Vec<u8>>,
    pub(super) provenance: &'a Option<Vec<u8>>,
}

pub(super) fn insert(ctx: &DecodeCtx, f: InsertFields) -> crate::Result<PhysicalPlan> {
    // Bind the leader-assigned surrogate verbatim — never re-allocate.
    // With a PK we bind by it; headless inserts self-key by the
    // surrogate's own big-endian bytes (mirrors `assign_anonymous`).
    let carried = Surrogate::new(f.surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => match f.pk_bytes {
            Some(pk) => a.bind(ctx.database_id, ctx.tenant_id, f.collection, pk, carried)?,
            None => bind_self_keyed(ctx, f.collection, carried)?,
        },
        None => carried,
    };
    let provenance = decode_sync_engines::decode_provenance(f.provenance)?;
    Ok(PhysicalPlan::Vector(VectorOp::Insert {
        collection: f.collection.to_owned(),
        vector: f.vector.to_vec(),
        dim: f.dim,
        field_name: f.field_name.to_owned(),
        surrogate,
        pk_bytes: f.pk_bytes.clone(),
        provenance,
    }))
}

pub(super) fn batch_insert(
    ctx: &DecodeCtx,
    collection: &str,
    vectors: &[Vec<f32>],
    dim: usize,
    surrogates: &[u32],
) -> crate::Result<PhysicalPlan> {
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
    let surrogates: Vec<Surrogate> = surrogates
        .iter()
        .map(|&raw| bind_self_keyed(ctx, collection, Surrogate::new(raw)))
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PhysicalPlan::Vector(VectorOp::BatchInsert {
        collection: collection.to_owned(),
        vectors: vectors.to_vec(),
        dim,
        surrogates,
    }))
}

pub(super) fn delete(collection: &str, vector_id: u32) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::Delete {
        collection: collection.to_owned(),
        vector_id,
    })
}

/// Fields of the `SetVectorParams` wire variant, bundled so [`set_params`]
/// stays under the `too_many_arguments` clippy threshold.
pub(super) struct SetParamsFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field_name: &'a str,
    pub(super) dim: usize,
    pub(super) m: usize,
    pub(super) ef_construction: usize,
    pub(super) metric: &'a str,
    pub(super) index_type: &'a str,
    pub(super) pq_m: usize,
    pub(super) ivf_cells: usize,
    pub(super) ivf_nprobe: usize,
}

pub(super) fn set_params(f: SetParamsFields) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: f.collection.to_owned(),
        field_name: f.field_name.to_owned(),
        dim: f.dim,
        m: f.m,
        ef_construction: f.ef_construction,
        metric: f.metric.to_owned(),
        index_type: f.index_type.to_owned(),
        pq_m: f.pq_m,
        ivf_cells: f.ivf_cells,
        ivf_nprobe: f.ivf_nprobe,
    })
}

pub(super) fn drop_index(collection: &str, field_name: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DropIndex {
        collection: collection.to_owned(),
        field_name: field_name.to_owned(),
    })
}

pub(super) fn sparse_insert(
    collection: &str,
    field_name: &str,
    doc_id: &str,
    entries: &[(u32, f32)],
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SparseInsert {
        collection: collection.to_owned(),
        field_name: field_name.to_owned(),
        doc_id: doc_id.to_owned(),
        entries: entries.to_vec(),
    })
}

pub(super) fn sparse_delete(collection: &str, field_name: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SparseDelete {
        collection: collection.to_owned(),
        field_name: field_name.to_owned(),
        doc_id: doc_id.to_owned(),
    })
}

pub(super) fn multi_vector_insert(
    ctx: &DecodeCtx,
    collection: &str,
    field_name: &str,
    document_surrogate: u32,
    vectors: &[f32],
    count: usize,
    dim: usize,
) -> crate::Result<PhysicalPlan> {
    // Self-keyed bind (mirrors `batch_insert`'s headless path): all `count`
    // vectors share this one carried surrogate, so binding by the
    // surrogate's own bytes is enough to install the same identity on every
    // replica without a separate PK.
    let surrogate = bind_self_keyed(ctx, collection, Surrogate::new(document_surrogate))?;
    Ok(PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
        collection: collection.to_owned(),
        field_name: field_name.to_owned(),
        document_surrogate: surrogate,
        vectors: vectors.to_vec(),
        count,
        dim,
    }))
}

pub(super) fn multi_vector_delete(
    collection: &str,
    field_name: &str,
    document_surrogate: u32,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
        collection: collection.to_owned(),
        field_name: field_name.to_owned(),
        // Deletes an already-bound identity; no re-binding needed (mirrors
        // `VectorDelete`, which carries no ctx interaction).
        document_surrogate: Surrogate::new(document_surrogate),
    })
}

pub(super) fn delete_by_surrogate(
    collection: &str,
    surrogate: u32,
    field_name: &str,
    provenance: &Option<Vec<u8>>,
) -> crate::Result<PhysicalPlan> {
    let provenance = decode_sync_engines::decode_provenance(provenance)?;
    Ok(PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
        collection: collection.to_owned(),
        // Deletes an already-bound identity; no re-binding needed.
        surrogate: Surrogate::new(surrogate),
        field_name: field_name.to_owned(),
        provenance,
    }))
}

/// Fields of the `DirectUpsert` wire variant, bundled so [`direct_upsert`]
/// stays under the `too_many_arguments` clippy threshold.
pub(super) struct DirectUpsertFields<'a> {
    pub(super) collection: &'a str,
    pub(super) field: &'a str,
    pub(super) surrogate: u32,
    pub(super) vector: &'a [f32],
    pub(super) payload: &'a [u8],
    pub(super) quantization: nodedb_types::VectorQuantization,
    pub(super) storage_dtype: nodedb_types::VectorStorageDtype,
    pub(super) payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
}

pub(super) fn direct_upsert(ctx: &DecodeCtx, f: DirectUpsertFields) -> crate::Result<PhysicalPlan> {
    // Self-keyed bind: `DirectUpsert` has no PK sidecar (vector-primary
    // collections are keyed by the vector index itself), so the surrogate
    // binds by its own bytes, same as headless vector inserts.
    let surrogate = bind_self_keyed(ctx, f.collection, Surrogate::new(f.surrogate))?;
    Ok(PhysicalPlan::Vector(VectorOp::DirectUpsert {
        collection: f.collection.to_owned(),
        field: f.field.to_owned(),
        surrogate,
        vector: f.vector.to_vec(),
        payload: f.payload.to_vec(),
        quantization: f.quantization,
        storage_dtype: f.storage_dtype,
        payload_indexes: f.payload_indexes.to_vec(),
        // Replication applies a leader's write on a follower; there is no
        // client session to answer with rows.
        returning: None,
        rls_filters: Vec::new(),
    }))
}
