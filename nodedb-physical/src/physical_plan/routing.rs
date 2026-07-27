// SPDX-License-Identifier: Apache-2.0

//! Cross-node routing predicates for physical plans.
//!
//! Predicates used by the coordinator to decide how to route a plan: whether
//! a plan tree contains a genuinely cluster-partitioned leaf (graph / array)
//! that requires a dedicated scatter-gather path, versus a single-vShard-homed
//! source (document / kv / columnar / ts / spatial / vector / text) that can
//! be routed directly to the owning vShard via the gateway's `other` arm.

use super::{ExchangeOp, GraphOp, PhysicalPlan, QueryOp};

/// Returns `true` if the plan tree contains a genuinely cluster-partitioned
/// leaf — one where rows are distributed across vShards by node-id or tile-id
/// rather than being wholly owned by a single vShard determined by collection
/// name hash.
///
/// # Single-vShard-homed collections (returns `false`)
///
/// Standard collections (document, kv, columnar, timeseries, spatial, vector,
/// text) are single-vShard-homed: all rows for a collection live on the one
/// vShard whose id is `vshard_for_collection(database_id, &name)`.  Routing
/// the bare plan to that vShard via the gateway's `other` arm returns exactly
/// the right rows with no duplication.  Broadcasting to every vShard would
/// return the full collection from the owning node on every route that lands
/// there, multiplying rows by 1 024 ×.
///
/// # Cluster-partitioned sources (returns `true`)
///
/// Graph traversal ops (Hop, Neighbors, NeighborsMulti, Path, Subgraph,
/// RagFusion, Algo, Match, TemporalNeighbors, TemporalAlgorithm, Stats) and
/// Array / ClusterArray ops distribute data by graph node-id or array tile-id
/// across vShards.  A cross-node gather for these sources requires a dedicated
/// scatter-gather path; this function signals callers to take the appropriate
/// fallback rather than silently producing wrong results.
///
/// The function recurses through wrapper ops the same way `is_sharded_source`
/// does.
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
