// SPDX-License-Identifier: BUSL-1.1

//! The child plans a query op reads its rows from.

use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

/// The child plans `op` reads its rows from.
pub(super) fn children(op: &QueryOp) -> Vec<&PhysicalPlan> {
    match op {
        QueryOp::Exchange(exchange) => vec![&*exchange.child],
        QueryOp::PostProcess { input, .. } => vec![&**input],
        QueryOp::SetOp { inputs, .. } => inputs.iter().collect(),
        QueryOp::Aggregate { input, .. } | QueryOp::PartialAggregateState { input, .. } => {
            input.iter().map(|child| &**child).collect()
        }
        QueryOp::HashJoin {
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        } => [left_input, right_input, left_bitmap, right_bitmap]
            .into_iter()
            .flatten()
            .map(|child| &**child)
            .collect(),
        QueryOp::NestedLoopJoin {
            left_input,
            right_input,
            ..
        }
        | QueryOp::SortMergeJoin {
            left_input,
            right_input,
            ..
        } => [left_input, right_input]
            .into_iter()
            .flatten()
            .map(|child| &**child)
            .collect(),
        QueryOp::LateralTopK { outer_plan, .. } | QueryOp::LateralLoop { outer_plan, .. } => {
            vec![&**outer_plan]
        }
        QueryOp::ProviderScan { .. }
        | QueryOp::PartialAggregate { .. }
        | QueryOp::ShuffleJoinConsume { .. }
        | QueryOp::ShuffleAggregateConsume { .. }
        | QueryOp::FacetCounts { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. } => Vec::new(),
    }
}
