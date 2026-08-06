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

        // No-op: writes. Write policies are enforced separately by
        // `RlsPolicyStore::check_write_with_auth`.
        ColumnarOp::Insert { .. } | ColumnarOp::Update { .. } | ColumnarOp::Delete { .. } => Ok(()),
    }
}

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

        // No-op: the ingest write path.
        TimeseriesOp::Ingest { .. } => Ok(()),
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

        // No-op: writes.
        SpatialOp::Insert { .. } | SpatialOp::Delete { .. } => Ok(()),
    }
}
