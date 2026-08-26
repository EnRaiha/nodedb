// SPDX-License-Identifier: Apache-2.0

//! Cross-node routing predicates for physical plans.
//!
//! Decides whether a plan tree contains a cluster-partitioned leaf (graph /
//! array, needing scatter-gather) versus a single-vShard-homed source
//! (document / kv / columnar / ts / spatial / vector / text, routed directly).

use super::{ExchangeOp, GraphOp, PhysicalPlan, QueryOp};

/// `true` if the plan tree contains a leaf whose rows are distributed across
/// vShards by node-id or tile-id (graph traversal, Array/ClusterArray),
/// rather than wholly owned by one vShard hashed from the collection name.
///
/// Routing a single-vShard-homed plan (document/kv/columnar/ts/spatial/
/// vector/text) via the gateway's `other` arm returns exactly the right rows;
/// broadcasting a cluster-partitioned one would multiply rows across vShards
/// — callers must take the scatter-gather path instead. Recurses through
/// wrapper ops the same way `is_sharded_source` does.
pub fn plan_contains_cluster_partitioned_leaf(plan: &PhysicalPlan) -> bool {
    match plan {
        // Recurse through aggregate wrappers.
        PhysicalPlan::Query(QueryOp::Aggregate { input, .. }) => match input {
            Some(child) => plan_contains_cluster_partitioned_leaf(child),
            None => false,
        },

        // Recurse through both sides of a HashJoin (and their bitmap inputs).
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        }) => {
            left_input
                .as_deref()
                .map(plan_contains_cluster_partitioned_leaf)
                .unwrap_or(false)
                || right_input
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
                || left_bitmap
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
                || right_bitmap
                    .as_deref()
                    .map(plan_contains_cluster_partitioned_leaf)
                    .unwrap_or(false)
        }

        // Recurse through Exchange wrapper (child is the real plan).
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => {
            plan_contains_cluster_partitioned_leaf(child)
        }

        // Recurse through PostProcess (its materialized input is the real
        // plan). Normally resolved to a `ProviderScan` before routing, but a
        // conservative recursion keeps routing correct if one survives.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
            plan_contains_cluster_partitioned_leaf(input)
        }

        // Recurse through lateral outer plans.
        PhysicalPlan::Query(QueryOp::LateralTopK { outer_plan, .. })
        | PhysicalPlan::Query(QueryOp::LateralLoop { outer_plan, .. }) => {
            plan_contains_cluster_partitioned_leaf(outer_plan)
        }

        // Graph traversal / query ops are cluster-partitioned (node-id routed).
        PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::NeighborsMulti { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Graph(GraphOp::RagFusion { .. })
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::TemporalNeighbors { .. })
        | PhysicalPlan::Graph(GraphOp::TemporalAlgorithm { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::Stats { .. }) => true,

        // Array ops are cluster-partitioned by tile-id.
        PhysicalPlan::Array(_) | PhysicalPlan::ClusterArray(_) => true,

        // Graph write ops and all other engine ops are single-vShard-homed.
        PhysicalPlan::Graph(GraphOp::EdgePut { .. })
        | PhysicalPlan::Graph(GraphOp::EdgePutBatch { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { .. })
        | PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(_))
        | PhysicalPlan::Graph(GraphOp::SetNodeLabels { .. })
        | PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { .. })
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::ClusterEvent(_)
        | PhysicalPlan::Query(QueryOp::ProviderScan { .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregateState { .. })
        // ShuffleJoinConsume / ShuffleAggregateConsume consume node-local staged
        // files: terminal local ops, never a cluster-partitioned scan to fan out.
        | PhysicalPlan::Query(QueryOp::ShuffleJoinConsume { .. })
        | PhysicalPlan::Query(QueryOp::ShuffleAggregateConsume { .. })
        | PhysicalPlan::Query(QueryOp::NestedLoopJoin { .. })
        | PhysicalPlan::Query(QueryOp::SortMergeJoin { .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveScan { .. })
        | PhysicalPlan::Query(QueryOp::RecursiveValue { .. }) => false,
    }
}
