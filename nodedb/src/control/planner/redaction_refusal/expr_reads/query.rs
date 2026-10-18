// SPDX-License-Identifier: BUSL-1.1

//! Column reads and child plans of the relational operations.

use nodedb_physical::physical_plan::{JoinProjection, QueryOp};

use super::reads::NodeReads;

/// The columns `op` reads outside its plain projection. The aggregate
/// functions themselves are refused by the aggregate check.
pub(super) fn reads(op: &QueryOp, out: &mut NodeReads) -> crate::Result<()> {
    match op {
        QueryOp::ProviderScan {
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            distinct,
            ..
        }
        | QueryOp::PostProcess {
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            distinct,
            ..
        } => {
            out.filter_bytes(filters)?;
            out.computed_bytes(computed_columns)?;
            out.window_bytes(window_functions)?;
            out.sort_keys(sort_keys);
            out.distinct(*distinct, projection);
            out.passthrough(projection);
        }
        QueryOp::Aggregate {
            group_by,
            filters,
            sub_group_by,
            sort_keys,
            ..
        } => {
            out.group_keys(group_by);
            out.group_names(sub_group_by);
            out.filter_bytes(filters)?;
            out.sort_keys(sort_keys);
        }
        QueryOp::PartialAggregate {
            group_by, filters, ..
        } => {
            out.group_names(group_by);
            out.filter_bytes(filters)?;
        }
        QueryOp::PartialAggregateState {
            group_by, filters, ..
        } => {
            out.group_keys(group_by);
            out.filter_bytes(filters)?;
        }
        QueryOp::ShuffleAggregateConsume {
            group_by,
            sort_keys,
            ..
        } => {
            out.group_names(group_by);
            out.sort_keys(sort_keys);
        }
        QueryOp::HashJoin {
            on,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_scan_filters,
            right_scan_filters,
            ..
        } => {
            out.join_keys(on);
            join_renames(projection, out);
            out.computed_bytes(computed_projection)?;
            for filters in [
                join_filters,
                post_filters,
                left_scan_filters,
                right_scan_filters,
            ] {
                out.filter_bytes(filters)?;
            }
        }
        QueryOp::ShuffleJoinConsume { on, .. } | QueryOp::SortMergeJoin { on, .. } => {
            out.join_keys(on);
        }
        QueryOp::NestedLoopJoin { condition, .. } => out.filter_bytes(condition)?,
        QueryOp::FacetCounts { filters, .. } => out.filter_bytes(filters)?,
        QueryOp::RecursiveScan {
            base_filters,
            recursive_filters,
            join_link,
            ..
        } => {
            out.filter_bytes(base_filters)?;
            out.filter_bytes(recursive_filters)?;
            if let Some(link) = join_link {
                out.join_keys(std::slice::from_ref(link));
            }
        }
        QueryOp::LateralTopK {
            inner_filters,
            inner_order_by,
            correlation_keys,
            projection,
            ..
        } => {
            out.filter_bytes(inner_filters)?;
            out.sort_keys(inner_order_by);
            out.join_keys(correlation_keys);
            join_renames(projection, out);
        }
        QueryOp::LateralLoop {
            inner_filters,
            correlation_predicates,
            projection,
            ..
        } => {
            out.filter_bytes(inner_filters)?;
            out.join_keys(correlation_predicates);
            join_renames(projection, out);
        }
        // A set operation compares whole rows of its inputs, each checked on
        // its own. A gather moves rows. A value CTE reads no collection.
        QueryOp::SetOp { .. } | QueryOp::Exchange(_) | QueryOp::RecursiveValue { .. } => {}
    }
    Ok(())
}

/// A join projection that emits a side's column under another name.
fn join_renames(projection: &[JoinProjection], out: &mut NodeReads) {
    for spec in projection {
        if spec.source != spec.output {
            out.renames.push((spec.output.clone(), spec.source.clone()));
        }
    }
}
