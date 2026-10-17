// SPDX-License-Identifier: BUSL-1.1

//! Column reads of the columnar-family engines: columnar, timeseries, and
//! spatial.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::reads::{Clause, NodeReads};

pub(super) fn columnar_reads(op: &ColumnarOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        ColumnarOp::Scan {
            filters,
            sort_keys,
            computed_columns,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.sort_keys(sort_keys);
            out.computed_bytes(computed_columns)?;
        }
        ColumnarOp::Insert {
            on_conflict_updates,
            ..
        } => out.updates(on_conflict_updates),
        ColumnarOp::Update {
            filters, updates, ..
        }
        | ColumnarOp::ResolveDml {
            filters, updates, ..
        } => {
            out.filter_bytes(filters)?;
            out.updates(updates);
        }
        ColumnarOp::Delete { filters, .. } => out.filter_bytes(filters)?,
        // Resolved rows, snapshot reads, and truncation read no column in a
        // predicate or an expression.
        ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::Truncate { .. } => {}
    }
    Ok(())
}

/// A timeseries scan's aggregates are refused by the aggregate check. Its
/// group keys, predicates, sort keys, and computed columns are read here.
pub(super) fn timeseries_reads(op: &TimeseriesOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        TimeseriesOp::Scan {
            filters,
            sort_keys,
            group_by,
            computed_columns,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.sort_keys(sort_keys);
            out.group_names(group_by);
            out.computed_bytes(computed_columns)?;
        }
        TimeseriesOp::Ingest { .. }
        | TimeseriesOp::ResolveIngest(_)
        | TimeseriesOp::Truncate { .. }
        | TimeseriesOp::RefreshAggregate { .. }
        | TimeseriesOp::Partitions { .. }
        | TimeseriesOp::ReadAggregate { .. } => {}
    }
    Ok(())
}

/// A spatial predicate reads its geometry column and its attribute filters.
pub(super) fn spatial_reads(op: &SpatialOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        SpatialOp::Scan {
            field,
            attribute_filters,
            ..
        } => {
            out.field(field, Clause::Filter);
            out.filter_bytes(attribute_filters)?;
        }
        SpatialOp::Insert { .. } | SpatialOp::Delete { .. } => {}
    }
    Ok(())
}
