// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Vector` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_types::Surrogate;

pub(super) fn insert(
    collection: &str,
    vector: &[f32],
    dim: usize,
    field_name: &str,
    surrogate: u32,
    pk_bytes: &Option<Vec<u8>>,
    provenance: Option<Vec<u8>>,
) -> ReplicatedWrite {
    ReplicatedWrite::VectorInsert {
        collection: collection.to_owned(),
        vector: vector.to_vec(),
        dim,
        field_name: field_name.to_owned(),
        // Carry the leader-assigned surrogate verbatim. Followers bind
        // (never re-allocate) by `pk_bytes` when present, else by the
        // surrogate's own self-key.
        surrogate,
        pk_bytes: pk_bytes.clone(),
        provenance,
    }
}

pub(super) fn batch_insert(
    collection: &str,
    vectors: &[Vec<f32>],
    dim: usize,
    surrogates: &[Surrogate],
) -> ReplicatedWrite {
    ReplicatedWrite::VectorBatchInsert {
        collection: collection.to_owned(),
        vectors: vectors.to_vec(),
        dim,
        surrogates: surrogates.iter().map(|s| s.as_u32()).collect(),
    }
}

pub(super) fn delete(collection: &str, vector_id: u32) -> ReplicatedWrite {
    ReplicatedWrite::VectorDelete {
        collection: collection.to_owned(),
        vector_id,
    }
}

/// Fields of `VectorOp::SetParams`, bundled so [`set_params`] stays under the
/// `too_many_arguments` clippy threshold.
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

pub(super) fn set_params(f: SetParamsFields) -> ReplicatedWrite {
    ReplicatedWrite::SetVectorParams {
        collection: f.collection.to_owned(),
        field_name: f.field_name.to_owned(),
        m: f.m,
        ef_construction: f.ef_construction,
        metric: f.metric.to_owned(),
        index_type: f.index_type.to_owned(),
        pq_m: f.pq_m,
        ivf_cells: f.ivf_cells,
        ivf_nprobe: f.ivf_nprobe,
    }
}
