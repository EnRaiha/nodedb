// SPDX-License-Identifier: BUSL-1.1

//! Encode `PhysicalPlan::Vector` variants into `ReplicatedWrite`.

use super::super::types::ReplicatedWrite;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::Surrogate;

/// Encode a `VectorOp` write variant into its `ReplicatedWrite` wire shape.
///
/// Returns `None` for the read / DDL-Alter variants (`Search`,
/// `MultiSearch`, `QueryStats`, `Seal`, `CompactIndex`, `Rebuild`,
/// `SparseSearch`, `MultiVectorScoreSearch`) — none of those are replicated.
/// Exhaustive over `VectorOp` (not a catch-all): a new variant forces an
/// explicit decision here instead of silently falling through.
pub(super) fn encode(op: &VectorOp) -> Option<ReplicatedWrite> {
    Some(match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes,
            provenance,
        } => insert(
            collection,
            vector,
            *dim,
            field_name,
            surrogate.as_u32(),
            pk_bytes,
            super::entry::encode_provenance(provenance),
        ),
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates,
        } => batch_insert(collection, vectors, *dim, surrogates),
        VectorOp::Delete {
            collection,
            vector_id,
        } => delete(collection, *vector_id),
        VectorOp::SetParams {
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
        } => set_params(SetParamsFields {
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
        }),
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => ReplicatedWrite::SparseInsert {
            collection: collection.to_owned(),
            field_name: field_name.to_owned(),
            doc_id: doc_id.to_owned(),
            entries: entries.clone(),
        },
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => ReplicatedWrite::SparseDelete {
            collection: collection.to_owned(),
            field_name: field_name.to_owned(),
            doc_id: doc_id.to_owned(),
        },
        VectorOp::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => ReplicatedWrite::MultiVectorInsert {
            collection: collection.to_owned(),
            field_name: field_name.to_owned(),
            // All `count` vectors are bound to this one leader-assigned
            // surrogate; carried verbatim so every replica shares the same
            // document identity instead of re-allocating.
            document_surrogate: document_surrogate.as_u32(),
            vectors: vectors.clone(),
            count: *count,
            dim: *dim,
        },
        VectorOp::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => ReplicatedWrite::MultiVectorDelete {
            collection: collection.to_owned(),
            field_name: field_name.to_owned(),
            document_surrogate: document_surrogate.as_u32(),
        },
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => ReplicatedWrite::DeleteBySurrogate {
            collection: collection.to_owned(),
            surrogate: surrogate.as_u32(),
            field_name: field_name.to_owned(),
            provenance: super::entry::encode_provenance(provenance),
        },
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            // A projection is a client-session concern and must not cross the
            // replication wire: the follower applies the write, it does not
            // answer the statement that asked for rows.
            returning: _,
            rls_filters: _,
        } => ReplicatedWrite::DirectUpsert {
            collection: collection.to_owned(),
            field: field.to_owned(),
            surrogate: surrogate.as_u32(),
            vector: vector.clone(),
            payload: payload.clone(),
            quantization: *quantization,
            storage_dtype: *storage_dtype,
            payload_indexes: payload_indexes.clone(),
        },
        VectorOp::DropIndex {
            collection,
            field_name,
        } => ReplicatedWrite::DropVectorIndex {
            collection: collection.to_owned(),
            field_name: field_name.to_owned(),
        },
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => return None,
    })
}

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
    pub(super) dim: usize,
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
        dim: f.dim,
        m: f.m,
        ef_construction: f.ef_construction,
        metric: f.metric.to_owned(),
        index_type: f.index_type.to_owned(),
        pq_m: f.pq_m,
        ivf_cells: f.ivf_cells,
        ivf_nprobe: f.ivf_nprobe,
    }
}
