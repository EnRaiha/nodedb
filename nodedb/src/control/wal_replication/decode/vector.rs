// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Vector`.

use super::super::decode_sync_engines;
use super::entry::DecodeCtx;
use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::VectorOp;

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
    let carried = nodedb_types::Surrogate::new(f.surrogate);
    let surrogate = match ctx.assigner {
        Some(a) => match f.pk_bytes {
            Some(pk) => a.bind(ctx.database_id, ctx.tenant_id, f.collection, pk, carried)?,
            None => a.bind(
                ctx.database_id,
                ctx.tenant_id,
                f.collection,
                &carried.as_u32().to_be_bytes(),
                carried,
            )?,
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
    let surrogates: Vec<nodedb_types::Surrogate> = match ctx.assigner {
        Some(a) => surrogates
            .iter()
            .map(|&raw| {
                let c = nodedb_types::Surrogate::new(raw);
                a.bind(
                    ctx.database_id,
                    ctx.tenant_id,
                    collection,
                    &c.as_u32().to_be_bytes(),
                    c,
                )
            })
            .collect::<crate::Result<Vec<_>>>()?,
        None => surrogates
            .iter()
            .map(|&raw| nodedb_types::Surrogate::new(raw))
            .collect(),
    };
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
        m: f.m,
        ef_construction: f.ef_construction,
        metric: f.metric.to_owned(),
        index_type: f.index_type.to_owned(),
        pq_m: f.pq_m,
        ivf_cells: f.ivf_cells,
        ivf_nprobe: f.ivf_nprobe,
    })
}
