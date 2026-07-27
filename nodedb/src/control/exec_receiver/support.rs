// SPDX-License-Identifier: BUSL-1.1

//! Support helpers for the local plan executor: the Exchange-rejection guard,
//! the streaming sink-outcome enum, and error mapping shared by the one-shot
//! and streaming execution paths.

use nodedb_cluster::rpc_codec::TypedClusterError;
use nodedb_physical::physical_plan::{PhysicalPlan, QueryOp};

/// Numeric code for `TypedClusterError::Internal` when plan bytes fail to decode.
pub(super) const PLAN_DECODE_FAILED: u32 = nodedb_cluster::rpc_codec::PLAN_DECODE_FAILED;

/// Result of draining a `ResultStream` into a `ChunkSink`.
pub(super) enum SinkOutcome {
    /// All frames delivered to the sink; the stream ended cleanly.
    CleanEnd,
    /// `send_chunk` failed — the coordinator dropped the stream. No terminal
    /// frame is written (there is no peer to receive it).
    CoordinatorGone,
    /// The result stream itself yielded an error (over-budget, data-plane
    /// error, etc.). Surface it as a terminal `ExecuteStreamEnd` error.
    StreamError(TypedClusterError),
}

/// Map a coordinator-side [`crate::Error`] from the result stream to a
/// [`TypedClusterError`] for the terminal `ExecuteStreamEnd` frame.
///
/// The numeric code is always derived from the error's real classification
/// via `NodeDbError::from(err).code()` — never hardcoded. A mid-stream
/// constraint violation or any other domain error must carry its own code,
/// not `PLAN_DECODE_FAILED` (that const is reserved for genuine plan-decode
/// failure sites elsewhere; using it as a catch-all here mislabels every
/// other error).
pub(super) fn stream_error_to_typed(err: crate::Error) -> TypedClusterError {
    let message = err.to_string();
    let code = u32::from(nodedb_types::error::NodeDbError::from(err).code().0);
    TypedClusterError::Internal { code, message }
}

/// Returns `true` if `plan` or any nested child plan still carries a
/// `QueryOp::Exchange` node.
///
/// Exchange is coordinator-resolved before cross-node dispatch, so a remote
/// executor must never receive one. This walks the realistic nesting points —
/// the `Exchange.child` box, the four `HashJoin` child/bitmap boxes, and the
/// `LateralTopK` / `LateralLoop` `outer_plan` box — and treats every other
/// variant as an Exchange-free leaf. Carriers are matched explicitly (no
/// catch-all) so a future plan variant that boxes a child plan fails to compile
/// here rather than silently bypassing the guard.
pub(super) fn plan_contains_exchange(plan: &PhysicalPlan) -> bool {
    match plan {
        // Query operations may embed child plans that themselves carry Exchange.
        PhysicalPlan::Query(query_op) => match query_op {
            QueryOp::Exchange(op) => plan_contains_exchange(&op.child),
            QueryOp::HashJoin {
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            } => [left_input, right_input, left_bitmap, right_bitmap]
                .into_iter()
                .flatten()
                .any(|child| plan_contains_exchange(child)),
            QueryOp::LateralTopK { outer_plan, .. } => plan_contains_exchange(outer_plan),
            QueryOp::LateralLoop { outer_plan, .. } => plan_contains_exchange(outer_plan),
            // PostProcess wraps a materialized child that, before coordinator
            // resolution, still carries an `Exchange{Gather}` — recurse so an
            // unresolved PostProcess is correctly flagged as Exchange-bearing.
            QueryOp::PostProcess { input, .. } => plan_contains_exchange(input),
            // Aggregate may carry a sub-plan input (catalog `ProviderScan`),
            // which could in principle nest an Exchange — recurse when present.
            QueryOp::Aggregate { input, .. } => {
                input.as_deref().is_some_and(plan_contains_exchange)
            }
            // Remaining query ops carry no nested PhysicalPlan child.
            QueryOp::ProviderScan { .. }
            | QueryOp::PartialAggregate { .. }
            | QueryOp::PartialAggregateState { .. }
            | QueryOp::ShuffleJoinConsume { .. }
            | QueryOp::ShuffleAggregateConsume { .. }
            | QueryOp::NestedLoopJoin { .. }
            | QueryOp::SortMergeJoin { .. }
            | QueryOp::FacetCounts { .. }
            | QueryOp::RecursiveScan { .. }
            | QueryOp::RecursiveValue { .. } => false,
        },

        // Leaf engine operations carry no nested PhysicalPlan child.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => false,
    }
}
