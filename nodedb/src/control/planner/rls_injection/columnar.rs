// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for the three peer engines sharing the columnar storage
//! core: plain columnar, timeseries, and spatial.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::context::RlsCtx;

/// Exhaustive over [`ColumnarOp`].
pub(super) fn inject_columnar(ctx: &RlsCtx<'_>, op: &mut ColumnarOp) -> crate::Result<()> {
    match op {
        // Inject: the policy occupies the dedicated post-scan slot, applied
        // after block pruning and before rows are returned.
        ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: the clone materializer streams raw `(surrogate, row bytes)`
        // pairs through a cursor payload that carries no row filter.
        ColumnarOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored rows through a cursor payload that carries \
             no row filter",
        ),

        // Refuse: a columnar write lands as column vectors in a segment
        // builder, and an update or delete resolves its target rows by scanning
        // blocks inside the handler. No point in this plan holds the row image
        // a write policy decides, so a policy on the collection refuses the
        // write rather than letting it persist unchecked.
        ColumnarOp::Insert { collection, .. }
        | ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. } => {
            ctx.refuse_if_write_policy(collection, COLUMNAR_WRITE_REASON)
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{SpatialOp, TimeseriesOp};

    use super::super::plan::test_support::{
        assert_write_refused, inject, inject_without_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn ingest(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.into(),
            payload: Vec::new(),
            format: "samples".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
        })
    }

    /// A timeseries ingest appends points straight into the memtable, so a
    /// write policy on the collection refuses it rather than letting rows the
    /// policy was never evaluated against become durable.
    #[test]
    fn timeseries_ingest_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("metrics");
        let mut plan = ingest("metrics");
        assert_write_refused(inject(&mut plan, &store), "metrics");
    }

    /// …and runs untouched when no policy applies.
    #[test]
    fn timeseries_ingest_without_a_policy_is_untouched() {
        let mut plan = ingest("metrics");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A spatial write carries geometry and a surrogate, not the row body the
    /// policy names.
    #[test]
    fn spatial_delete_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("places");
        let mut plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: "places".into(),
            field: "geom".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
        });
        assert_write_refused(inject(&mut plan, &store), "places");
    }
}

/// Why a write to the columnar storage core cannot be gated by a row policy.
const COLUMNAR_WRITE_REASON: &str = "the columnar core persists column vectors and resolves updated or deleted rows by scanning \
     blocks inside the handler, so no row image is available for the policy to be evaluated \
     against";

/// Exhaustive over [`TimeseriesOp`].
pub(super) fn inject_timeseries(ctx: &RlsCtx<'_>, op: &mut TimeseriesOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied after time-range pruning, on the rows
        // the scan actually produced.
        TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: ingest appends batched points straight into the memtable, so
        // the same reasoning as the columnar write path applies.
        TimeseriesOp::Ingest { collection, .. } => {
            ctx.refuse_if_write_policy(collection, COLUMNAR_WRITE_REASON)
        }
    }
}

/// Exhaustive over [`SpatialOp`].
pub(super) fn inject_spatial(ctx: &RlsCtx<'_>, op: &mut SpatialOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied to the R-tree candidates before they
        // are returned, alongside the query's own attribute filters.
        SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: an R-tree write carries geometry and a surrogate rather than
        // the row body a policy predicate names.
        SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. } => {
            ctx.refuse_if_write_policy(collection, COLUMNAR_WRITE_REASON)
        }
    }
}
