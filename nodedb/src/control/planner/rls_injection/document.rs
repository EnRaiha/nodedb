// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for document-engine operations.

use nodedb_physical::physical_plan::DocumentOp;

use super::context::RlsCtx;

/// Exhaustive over [`DocumentOp`] so a new document operation forces a
/// decision between injecting, refusing, and no-op.
pub(super) fn inject_document(ctx: &RlsCtx<'_>, op: &mut DocumentOp) -> crate::Result<()> {
    match op {
        // Inject: the scan pushes its predicate into storage, so the policy
        // ANDs into the same slot the user's WHERE clause occupies.
        DocumentOp::Scan {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: the indexed equality resolves doc ids, then every fetched
        // body is tested against `filters` — the residual post-filter slot the
        // policy ANDs into.
        DocumentOp::IndexedFetch {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: no storage pushdown slot, so the handler evaluates the
        // policy on the rows it fetched. An excluded row reads back as absent
        // — indistinguishable from a missing key, so a caller cannot probe for
        // rows it may not read.
        DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        }
        | DocumentOp::RangeScan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: returns index entries rather than rows, so there is no row
        // body to evaluate a policy against.
        DocumentOp::IndexLookup { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the lookup returns index entries, not row bodies, so the row filter has nothing to \
             evaluate against",
        ),

        // Refuse: an HLL cardinality estimate counts rows the policy hides,
        // and a scalar count carries no row for the filter to test. Redaction
        // ignores this shape (a count exposes no column value); RLS cannot,
        // because the row set itself is what the policy restricts.
        DocumentOp::EstimateCount { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the estimate is a row count, which the row filter cannot be evaluated against",
        ),

        // Refuse: the clone materializer streams raw `(id, surrogate, value)`
        // triples with no filter slot, so every stored body would be copied
        // regardless of the policy.
        DocumentOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored bodies through a cursor payload that \
             carries no row filter",
        ),

        // No-op: writes, bulk DML, and index DDL. The read policy does not
        // apply to them; write policies are enforced separately by
        // `RlsPolicyStore::check_write_with_auth`, and their `filters` /
        // `source_filters` / `target_filters` slots are the statement's own
        // write predicate, not a read filter slot.
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;

    use super::super::plan::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn indexed_fetch(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: collection.into(),
            path: "$.email".into(),
            value: "a@b.c".into(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
        })
    }

    /// The indexed fetch applies `filters` to every fetched body, so the
    /// policy lands there rather than refusing the plan.
    #[test]
    fn indexed_fetch_receives_the_policy_filter() {
        let store = store_with_read_policy("users");
        let mut plan = indexed_fetch("users");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::IndexedFetch { filters, .. }) => {
                assert!(!filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// With no policy the same fetch is untouched.
    #[test]
    fn indexed_fetch_without_a_policy_is_untouched() {
        let mut plan = indexed_fetch("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A cardinality estimate counts rows the policy hides.
    #[test]
    fn estimate_count_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: "users".into(),
            field: "id".into(),
        });
        assert_refused(inject(&mut plan, &store), "users");
    }
}
