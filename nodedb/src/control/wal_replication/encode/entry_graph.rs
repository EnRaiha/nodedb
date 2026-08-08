// SPDX-License-Identifier: BUSL-1.1

//! Classify a `GraphOp` into an optional `ReplicatedWrite`.
//!
//! Exhaustive over `GraphOp` (not a catch-all): a new variant is a compile
//! error here, so no future graph write is silently left un-replicated.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::types::ReplicatedWrite;
use super::graph;
use nodedb_physical::physical_plan::GraphOp;

/// Encode a `GraphOp` write variant into its `ReplicatedWrite` wire shape, or
/// `None` when the op is a read / traversal / algorithm (never a write).
pub(super) fn graph_write(op: &GraphOp) -> Option<ReplicatedWrite> {
    Some(match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
        } => graph::edge_put(
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate.as_u32(),
            dst_surrogate.as_u32(),
        ),
        // The compiled write predicate is a planning-time artifact of the
        // originating session, already decided before replication, so it is not
        // carried on the wire.
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            ..
        } => graph::edge_delete(
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate.as_u32(),
            dst_surrogate.as_u32(),
        ),
        GraphOp::SetNodeLabels { node_id, labels } => graph::set_node_labels(node_id, labels),
        GraphOp::RemoveNodeLabels { node_id, labels } => graph::remove_node_labels(node_id, labels),
        GraphOp::EdgePutBatch { edges } => graph::edge_put_batch(edges),
        GraphOp::EdgeDeleteBatch { edges } => graph::edge_delete_batch(edges),

        // Not a write — traversals / pattern matching / algorithms / stats.
        GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. } => return None,
    })
}
