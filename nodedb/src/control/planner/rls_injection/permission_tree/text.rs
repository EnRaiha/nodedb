// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for full-text-search operations.

use nodedb_physical::physical_plan::TextOp;

use super::context::{PermCtx, PermTreeLevel};

/// Exhaustive over [`TextOp`] so a new text operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_text(ctx: &PermCtx<'_>, op: &mut TextOp) -> crate::Result<()> {
    match op {
        // Filter: the subtree lands in the post-score / post-fusion slot the
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
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: the score scan emits every document in the collection with a
        // score column appended, and the phrase search emits every positional
        // hit — neither carries a filter slot for the subtree to occupy.
        TextOp::BM25ScoreScan { collection, .. } | TextOp::PhraseSearch { collection, .. } => ctx
            .refuse_if_tree(
                collection,
                "the search returns matched document rows through a response shape that carries \
                 no subtree filter",
            ),

        // Filter (write level, blanket): indexing a document names the row it
        // indexes, so there is no predicate to narrow.
        TextOp::FtsIndexDoc { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),

        // Filter (delete level, blanket): removing a document from the index
        // removes the row's searchable presence.
        TextOp::FtsDeleteDoc { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),

        // No-op: per-collection analyzer configuration is DDL over the index,
        // not an operation on rows.
        TextOp::SetTextConfig { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TextOp;

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// A BM25 score scan returns every row of the collection with no slot for
    /// the subtree, so it is refused rather than silently over-returning.
    #[test]
    fn bm25_score_scan_is_refused_under_a_tree() {
        let cache = cache_with_tree("articles");
        let mut plan = PhysicalPlan::Text(TextOp::BM25ScoreScan {
            collection: "articles".into(),
            query: "rust".into(),
            score_alias: "score".into(),
            fuzzy: false,
        });
        assert_refused(apply(&mut plan, &cache), "articles");
    }

    /// A BM25 search does carry the slot, so the subtree is injected.
    #[test]
    fn search_receives_the_subtree_filter() {
        let cache = cache_with_tree("articles");
        let mut plan = PhysicalPlan::Text(TextOp::Search {
            collection: "articles".into(),
            query: "rust".into(),
            top_k: 10,
            fuzzy: false,
            prefilter: None,
            rls_filters: Vec::new(),
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Text(TextOp::Search { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }
}
