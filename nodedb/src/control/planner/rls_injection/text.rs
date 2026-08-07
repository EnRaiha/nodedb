// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for full-text-search operations.

use nodedb_physical::physical_plan::TextOp;

use super::context::RlsCtx;

/// Exhaustive over [`TextOp`] so a new text operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_text(ctx: &RlsCtx<'_>, op: &mut TextOp) -> crate::Result<()> {
    match op {
        // Inject: the policy lands in the post-score / post-fusion slot the
        // handler applies before the ranked hits are returned. The result may
        // hold fewer than `top_k` rows, which is the intended effect.
        TextOp::Search {
            collection,
            rls_filters,
            ..
        }
        | TextOp::HybridSearch {
            collection,
            rls_filters,
            ..
        }
        | TextOp::HybridSearchTriple {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: the score scan emits every document in the collection with a
        // score column appended, and the phrase search emits every positional
        // hit — neither carries a filter slot for the policy to occupy.
        TextOp::BM25ScoreScan { collection, .. } | TextOp::PhraseSearch { collection, .. } => ctx
            .refuse_if_policy(
                collection,
                "the search returns matched document rows through a response shape that carries \
                 no row filter",
            ),

        // Refuse: an index write carries the extracted text and a surrogate,
        // not the row body the policy names. Indexing a row the policy hides
        // makes it reachable by search, which is the disclosure the policy
        // exists to prevent, so a policy on the collection refuses the write.
        TextOp::FtsIndexDoc { collection, .. } | TextOp::FtsDeleteDoc { collection, .. } => ctx
            .refuse_if_write_policy(
                collection,
                "an index write carries extracted text and a surrogate rather than the row body \
                 the policy names, so no row image is available for it to be evaluated against",
            ),

        // No-op: the per-collection analyzer binding is configuration, not a
        // user row.
        TextOp::SetTextConfig { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TextOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn index_doc(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: collection.into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            text: "hello".into(),
            provenance: None,
        })
    }

    /// An index write carries extracted text and a surrogate, not the row body
    /// the policy names, so a write policy refuses it.
    #[test]
    fn fts_index_doc_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("articles");
        let mut plan = index_doc("articles");
        assert_write_refused(inject(&mut plan, &store), "articles");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn fts_index_doc_without_a_policy_is_untouched() {
        let mut plan = index_doc("articles");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A BM25 score scan returns every row of the collection with no slot for
    /// the policy, so it is refused rather than silently over-returning.
    #[test]
    fn bm25_score_scan_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("articles");
        let mut plan = PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: "articles".into(),
            query: "rust".into(),
            score_alias: "score".into(),
            fuzzy: false,
        });
        assert_refused(inject(&mut plan, &store), "articles");
    }

    /// A BM25 search does carry the slot, so the policy is injected.
    #[test]
    fn search_receives_the_policy_filter() {
        let store = store_with_read_policy("articles");
        let mut plan = PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "rust".into(),
            top_k: 10,
            fuzzy: false,
            prefilter: None,
            rls_filters: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Text(TextOp::Search { rls_filters, .. }) => {
                assert!(!rls_filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
