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
            collection.as_str(),
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
        } => batch_insert(collection.as_str(), vectors, *dim, surrogates),
        VectorOp::Delete {
            collection,
            vector_id,
        } => delete(collection.as_str(), *vector_id),
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
            collection: collection.as_str(),
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
            collection: collection.as_str().to_owned(),
            field_name: field_name.to_owned(),
            doc_id: doc_id.to_owned(),
            entries: entries.clone(),
        },
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => ReplicatedWrite::SparseDelete {
            collection: collection.as_str().to_owned(),
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
            collection: collection.as_str().to_owned(),
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
            collection: collection.as_str().to_owned(),
            field_name: field_name.to_owned(),
            document_surrogate: document_surrogate.as_u32(),
        },
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => ReplicatedWrite::DeleteBySurrogate {
            collection: collection.as_str().to_owned(),
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
            returning,
            rls_filters,
        } => ReplicatedWrite::DirectUpsert {
            collection: collection.as_str().to_owned(),
            field: field.to_owned(),
            surrogate: surrogate.as_u32(),
            vector: vector.clone(),
            payload: payload.clone(),
            quantization: *quantization,
            storage_dtype: *storage_dtype,
            payload_indexes: payload_indexes.clone(),
            returning: super::entry::encode_returning(returning),
            rls_filters: rls_filters.clone(),
        },
        VectorOp::DropIndex {
            collection,
            field_name,
        } => ReplicatedWrite::DropVectorIndex {
            collection: collection.as_str().to_owned(),
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

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::VectorOp;
    use nodedb_types::{PayloadIndexKind, QualifiedCollection, Surrogate};
    use nodedb_types::{VectorQuantization, VectorStorageDtype};

    use super::super::super::types::ReplicatedEntry;

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(
        tenant_id: TenantId,
        database_id: DatabaseId,
        vshard_id: VShardId,
        plan: &PhysicalPlan,
    ) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            tenant_id,
            database_id,
            vshard_id,
            &write,
        )
    }

    // ---- Regression coverage: six `VectorOp` writes must not fall through
    // `to_replicated_entry`'s `_ => return None` catch-all, or they never
    // reach Raft. Each test runs the real encode/decode path end to end.

    #[test]
    fn vector_extended_variants_all_encode_to_some() {
        let tenant = TenantId::new(1);
        let vshard = VShardId::new(0);
        let plans = vec![
            PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                surrogate: Surrogate::new(1),
                field_name: "emb".into(),
                provenance: None,
            }),
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                field_name: "sparse".into(),
                doc_id: "d1".into(),
                entries: vec![(1, 0.5)],
            }),
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                field_name: "sparse".into(),
                doc_id: "d1".into(),
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                field_name: "colbert".into(),
                document_surrogate: Surrogate::new(2),
                vectors: vec![0.1, 0.2, 0.3, 0.4],
                count: 2,
                dim: 2,
            }),
            PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                field_name: "colbert".into(),
                document_surrogate: Surrogate::new(2),
            }),
            PhysicalPlan::Vector(VectorOp::DirectUpsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
                field: "emb".into(),
                surrogate: Surrogate::new(3),
                vector: vec![0.5, 0.6],
                payload: vec![1, 2, 3],
                quantization: VectorQuantization::RaBitQ,
                storage_dtype: VectorStorageDtype::F16,
                payload_indexes: vec![("tenant_id".into(), PayloadIndexKind::Equality)],
                returning: None,
                rls_filters: Vec::new(),
            }),
        ];
        for plan in &plans {
            // Must not be `None` for any of the six.
            assert!(
                to_replicated_entry(tenant, DatabaseId::DEFAULT, vshard, plan)
                    .expect("encode must not error")
                    .is_some(),
                "expected {plan:?} to be replicated, but to_replicated_entry returned None \
                 (this Vector write would execute locally and never reach Raft)"
            );
        }
    }
}
