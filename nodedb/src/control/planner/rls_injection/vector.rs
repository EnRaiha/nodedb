// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for vector-engine operations.

use nodedb_physical::physical_plan::VectorOp;

use super::context::RlsCtx;
use super::plan::walk;

/// Exhaustive over [`VectorOp`] so a new vector operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_vector(ctx: &RlsCtx<'_>, op: &mut VectorOp) -> crate::Result<()> {
    match op {
        // Inject, then recurse: the policy lands in the post-candidate slot the
        // handler applies after HNSW returns candidates, and the prefilter
        // sub-plan is a full plan in its own right whose rows must be resolved
        // too — the redaction pass walks the same child for the same reason.
        VectorOp::Search {
            collection,
            rls_filters,
            inline_prefilter_plan,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            match inline_prefilter_plan {
                Some(child) => walk(ctx, child),
                None => Ok(()),
            }
        }

        // Inject: the fused per-field results are filtered post-candidate.
        VectorOp::MultiSearch {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: both return scored document identities with no filter slot,
        // so the rows a policy hides would still be ranked and returned.
        VectorOp::SparseSearch { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the search returns scored document identities through a response shape that carries \
             no row filter",
        ),

        // Refuse: index statistics count the indexed rows, including the ones
        // the policy hides, and a statistics payload carries no row to filter.
        VectorOp::QueryStats { collection, .. } => ctx.refuse_if_policy(
            collection,
            "index statistics are counts over the indexed rows, which the row filter cannot be \
             evaluated against",
        ),

        // Refuse: an index write carries an embedding and a surrogate, not the
        // row body a policy predicate names, so there is nothing here for the
        // write policy to be evaluated against. The vector entry is a claim
        // about a row, and admitting it unchecked would let an identity the
        // policy restricts make a hidden row reachable by search.
        VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. }
        | VectorOp::DirectUpsert { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a vector write carries an embedding and a surrogate rather than the row body the \
             policy names, so no row image is available for it to be evaluated against",
        ),

        // No-op: index parameters and index maintenance write no user row.
        VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, VectorOp};

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn vector_insert(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: collection.into(),
            vector: vec![0.0],
            dim: 1,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        })
    }

    /// Indexing a row the policy hides makes it reachable by search, which is
    /// the disclosure the policy exists to prevent — and the plan carries an
    /// embedding, not the row body, so nothing here can be evaluated.
    #[test]
    fn vector_insert_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("docs");
        let mut plan = vector_insert("docs");
        assert_write_refused(inject(&mut plan, &store), "docs");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn vector_insert_without_a_policy_is_untouched() {
        let mut plan = vector_insert("docs");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    fn search_with_prefilter(collection: &str, prefilter: Option<PhysicalPlan>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection: collection.into(),
            query_vector: vec![0.1, 0.2],
            top_k: 4,
            ef_search: 0,
            metric: nodedb_types::vector_distance::DistanceMetric::L2,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: prefilter.map(Box::new),
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        })
    }

    /// The prefilter sub-plan is a real read of its own collection, so a
    /// refusable op nested there is still caught.
    #[test]
    fn inline_prefilter_plan_is_walked() {
        let store = store_with_read_policy("users");
        let mut plan = search_with_prefilter(
            "embeddings",
            Some(PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: "users".into(),
                path: "$.email".into(),
                value: "a@b.c".into(),
            })),
        );
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// A sparse search has no filter slot, so a policy refuses it.
    #[test]
    fn sparse_search_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("docs");
        let mut plan = PhysicalPlan::Vector(VectorOp::SparseSearch {
            collection: "docs".into(),
            field_name: "sparse".into(),
            query_entries: vec![(1, 1.0)],
            top_k: 5,
        });
        assert_refused(inject(&mut plan, &store), "docs");
    }
}
