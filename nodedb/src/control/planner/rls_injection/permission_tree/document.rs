// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for document-engine operations.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_plan::document::MergeActionOp;

use super::context::{PermCtx, PermTreeLevel};

/// A source collection read into a mutation carries its values into rows the
/// caller can read afterwards, and no plan node in these shapes has a slot to
/// narrow that read to the caller's subtree.
const JOINED_SOURCE_REASON: &str = "the statement copies values out of the source collection through a join that carries no \
     subtree filter, so rows outside the caller's subtree would reach the target";

/// Exhaustive over [`DocumentOp`] so a new document operation forces a
/// decision between filtering, refusing, and no-op.
pub(super) fn apply_document(ctx: &PermCtx<'_>, op: &mut DocumentOp) -> crate::Result<()> {
    match op {
        // Filter: the scan pushes its predicate into storage, so the subtree
        // filter ANDs into the same slot the user's WHERE clause occupies.
        DocumentOp::Scan {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: the indexed equality resolves doc ids, then every fetched
        // body is tested against `filters` — the residual post-filter slot the
        // subtree filter ANDs into.
        DocumentOp::IndexedFetch {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: no storage pushdown slot, so the handler evaluates the
        // subtree filter on the rows it fetched. A row outside the subtree
        // reads back as absent, indistinguishable from a missing key.
        DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        }
        | DocumentOp::RangeScan {
            collection,
            rls_filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: returns index entries rather than rows, so there is no
        // resource column to evaluate the subtree filter against.
        DocumentOp::IndexLookup { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the lookup returns index entries, not row bodies, so the subtree filter has nothing \
             to evaluate against",
        ),

        // Refuse: an HLL cardinality estimate counts rows across the whole
        // collection, including the ones outside the caller's subtree, and a
        // scalar count carries no resource column to filter on.
        DocumentOp::EstimateCount { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the estimate is a row count, which the subtree filter cannot be evaluated against",
        ),

        // Refuse: the clone materializer streams raw `(id, surrogate, value)`
        // triples with no filter slot, so every stored body would be copied
        // regardless of the tree.
        DocumentOp::MaterializeScan { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the materializing scan streams raw stored bodies through a cursor payload that \
             carries no subtree filter",
        ),

        // Filter (write level, blanket): these name the rows they write
        // directly, so there is nothing to narrow — the identity must instead
        // hold write access somewhere in the tree. Unlike the RLS pass, which
        // defers writes to a separate write-path check, the permission tree
        // has its own write level and enforces it here.
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        // Named directly like the point writes above. The balance column is
        // maintained by the engine, not chosen by the writer, but the row it
        // lands on is still a row of the target collection.
        | DocumentOp::ApplyBalanceDelta { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Write)
        }

        // Filter (write level): the bulk update selects its rows through
        // `filters`, so the subtree narrows which rows are written in addition
        // to the blanket level check.
        DocumentOp::BulkUpdate {
            collection,
            filters,
            ..
        } => {
            ctx.authorize(collection, PermTreeLevel::Write)?;
            ctx.filter_into(collection, PermTreeLevel::Write, filters)
        }

        // Filter (delete level, blanket): a keyed delete and a truncate both
        // act on rows they do not select through a predicate.
        DocumentOp::PointDelete { collection, .. } | DocumentOp::Truncate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Delete)
        }

        // Filter (delete level): the bulk delete selects its rows through
        // `filters`, so the subtree narrows what is removed.
        DocumentOp::BulkDelete {
            collection,
            filters,
            ..
        } => {
            ctx.authorize(collection, PermTreeLevel::Delete)?;
            ctx.filter_into(collection, PermTreeLevel::Delete, filters)
        }

        // Filter, both sides: the target is written blind to the tree, while
        // the source is scanned through `source_filters` and narrows to the
        // readable subtree.
        DocumentOp::InsertSelect {
            target_collection,
            source_collection,
            source_filters,
            ..
        } => {
            ctx.authorize(target_collection, PermTreeLevel::Write)?;
            ctx.filter_into(source_collection, PermTreeLevel::Read, source_filters)
        }

        // Filter and refuse: the target is selected through `target_filters`,
        // so the subtree narrows what is updated; the joined source has no
        // slot at all and is refused when a tree governs it.
        DocumentOp::UpdateFromJoin {
            target_collection,
            source_collection,
            target_filters,
            ..
        } => {
            ctx.refuse_if_tree(source_collection, JOINED_SOURCE_REASON)?;
            ctx.authorize(target_collection, PermTreeLevel::Write)?;
            ctx.filter_into(target_collection, PermTreeLevel::Write, target_filters)
        }

        // Filter and refuse: the merge matches target rows by join key rather
        // than by a predicate, so the target gets the blanket check — at the
        // delete level too when any arm deletes — and the joined source is
        // refused for the same reason as `UpdateFromJoin`.
        DocumentOp::Merge {
            target_collection,
            source_collection,
            clauses,
            ..
        } => {
            ctx.refuse_if_tree(source_collection, JOINED_SOURCE_REASON)?;
            ctx.authorize(target_collection, PermTreeLevel::Write)?;
            if clauses
                .iter()
                .any(|clause| matches!(clause.action, MergeActionOp::Delete))
            {
                ctx.authorize(target_collection, PermTreeLevel::Delete)?;
            }
            Ok(())
        }

        // No-op: index DDL and collection registration. These describe the
        // collection rather than acting on its rows, and are authorized as
        // DDL rather than against a permission level.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// A point get has no pushdown slot, so the subtree lands in the
    /// post-fetch slot the handler evaluates.
    #[test]
    fn point_get_receives_the_subtree_filter() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".into(),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::PointGet { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A bulk delete narrows to the delete subtree, which is stricter than the
    /// readable one.
    #[test]
    fn bulk_delete_narrows_to_the_delete_subtree() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: "docs".into(),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::BulkDelete { filters, .. }) => {
                assert_eq!(
                    sorted(injected_resources(filters)),
                    vec!["doc_a".to_owned()]
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// An index lookup returns index entries with no resource column.
    #[test]
    fn index_lookup_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Document(DocumentOp::IndexLookup {
            collection: "docs".into(),
            path: "$.email".into(),
            value: "a@b.c".into(),
        });
        assert_refused(apply(&mut plan, &cache), "docs");
    }
}
