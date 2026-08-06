// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for graph-overlay operations.
//!
//! No graph read carries a slot the storage layer evaluates a resource-column
//! predicate in: a traversal returns topology, an algorithm returns per-node
//! scalars, a pattern match returns bindings, and RAG fusion returns fused
//! document rows through the fusion envelope. Each therefore refuses while a
//! permission tree governs the collection being read — the same verdicts the
//! RLS pass reaches for the same shapes.
//!
//! Edge writes do carry a collection, and unlike the RLS pass — which leaves
//! every write to a separate write-path check — this pass has write and delete
//! levels of its own and applies them here.

use nodedb_physical::physical_plan::GraphOp;

use super::context::{PermCtx, PermTreeLevel};

const TRAVERSAL_REASON: &str =
    "a traversal returns graph topology, which the subtree filter cannot be evaluated against";

const ALGORITHM_REASON: &str = "an algorithm returns per-node scalars computed over every edge, which the subtree filter \
     cannot be evaluated against";

const MATCH_REASON: &str = "a pattern match returns bindings over graph topology, which the subtree filter cannot be \
     evaluated against";

/// Exhaustive over [`GraphOp`] so a new graph operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_graph(ctx: &PermCtx<'_>, op: &GraphOp) -> crate::Result<()> {
    match op {
        // Refuse: a traversal returns node ids and edge labels, not row
        // bodies. What it discloses is topology — which nodes exist and how
        // they connect — and the edges of a resource outside the caller's
        // subtree are as much out of reach as the resource itself.
        //
        // A traversal with no collection (`None`) is a tree-index walk scoped
        // by edge label; no catalog record maps an index back to the
        // collection it was built on, so there is no tree definition to
        // consult.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_deref() {
            Some(collection) => ctx.refuse_if_tree(collection, TRAVERSAL_REASON),
            None => Ok(()),
        },

        // Refuse: same shape as `Neighbors`. The bitemporal form always names
        // its collection — the versioned edge key layout is collection-scoped.
        GraphOp::TemporalNeighbors { collection, .. } => {
            ctx.refuse_if_tree(collection, TRAVERSAL_REASON)
        }

        // Refuse: a pattern match returns variable bindings over topology with
        // no filter slot, and its own `WHERE` can probe a hidden row's field
        // one predicate at a time. The collection lives inside the serialized
        // query rather than on the plan node.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Refuse: the algorithm runs over the whole CSR for the collection and
        // returns ranks / component ids / counts derived from every row,
        // including the ones outside the subtree, through a payload with no
        // resource column to filter on.
        GraphOp::Algo { params, .. } | GraphOp::TemporalAlgorithm { params, .. } => {
            ctx.refuse_if_tree(&params.collection, ALGORITHM_REASON)
        }

        // Refuse: the distributed supersteps are the same algorithms one round
        // at a time, carrying the target collection in their params.
        GraphOp::BspSuperstep(plan) => {
            ctx.refuse_if_tree(&plan.params.collection, ALGORITHM_REASON)
        }
        GraphOp::WccSuperstep(plan) => {
            ctx.refuse_if_tree(&plan.params.collection, ALGORITHM_REASON)
        }

        // Refuse: RAG fusion returns fused document rows, but the fusion
        // envelope has no filter slot and embeds no sub-plan to recurse into —
        // the vector, text, and graph legs all run inside the handler.
        GraphOp::RagFusion { collection, .. } => ctx.refuse_if_tree(
            collection,
            "fusion returns ranked document rows through a fused response shape that carries no \
             subtree filter",
        ),

        // Refuse: the counters summarize the collection's edges, so they count
        // rows outside the subtree, and a counter carries no resource column.
        // `collection = None` reports every collection that has edges, so the
        // narrow per-collection question cannot be asked.
        GraphOp::Stats { collection, .. } => match collection.as_deref() {
            Some(collection) => ctx.refuse_if_tree(
                collection,
                "graph statistics are counters over the collection's edges, which the subtree \
                 filter cannot be evaluated against",
            ),
            None => ctx.refuse_if_any_tree(
                "graph statistics report counters for every collection holding edges, which the \
                 subtree filter cannot be evaluated against",
            ),
        },

        // Filter (write level, blanket): an edge write names its endpoints
        // directly, so there is no predicate to narrow.
        GraphOp::EdgePut { collection, .. } => ctx.authorize(collection, PermTreeLevel::Write),
        GraphOp::EdgePutBatch { edges } => {
            for edge in edges {
                ctx.authorize(&edge.collection, PermTreeLevel::Write)?;
            }
            Ok(())
        }

        // Filter (delete level, blanket): removing an edge removes stored
        // topology of the collection.
        GraphOp::EdgeDelete { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),
        GraphOp::EdgeDeleteBatch { edges } => {
            for edge in edges {
                ctx.authorize(&edge.collection, PermTreeLevel::Delete)?;
            }
            Ok(())
        }

        // No-op: node labels are keyed by node id alone and name no
        // collection, so this pass has no tree definition to resolve.
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => Ok(()),
    }
}

/// Refuse a pattern match whose target collection carries a permission tree.
///
/// The collection lives in the serialized `MatchQuery` — the plan node carries
/// only the encoded query — so it is decoded here to keep the refusal narrow:
/// a match scoped with `IN '<collection>'` to a collection no tree governs
/// still runs.
///
/// A query that names no collection may traverse any of the tenant's edges,
/// and one that fails to decode cannot be shown to avoid a governed
/// collection. Both fall back to the tenant-wide question, exactly as the RLS
/// pass does for the same shape.
fn refuse_match(ctx: &PermCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    match decoded.ok().and_then(|query| query.collection) {
        Some(collection) => ctx.refuse_if_tree(&collection, MATCH_REASON),
        None => ctx.refuse_if_any_tree(MATCH_REASON),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_graph::{AlgoParams, GraphAlgorithm};
    use nodedb_physical::physical_plan::GraphOp;

    use super::super::plan::test_support::{
        apply, apply_without_tree, assert_refused, cache_with_tree,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn neighbors(collection: Option<&str>) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Neighbors {
            collection: collection.map(str::to_string),
            node_id: "n1".into(),
            edge_label: None,
            direction: nodedb_types::graph::Direction::Out,
            rls_filters: Vec::new(),
        })
    }

    /// A traversal over a governed collection discloses topology the subtree
    /// filter cannot narrow, so it is refused.
    #[test]
    fn traversal_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = neighbors(Some("docs"));
        assert_refused(apply(&mut plan, &cache), "docs");
    }

    /// A traversal that names no collection has no tree to resolve.
    #[test]
    fn unscoped_traversal_is_untouched() {
        let cache = cache_with_tree("docs");
        let mut plan = neighbors(None);
        let before = plan.clone();
        assert!(apply(&mut plan, &cache).is_ok());
        assert_eq!(plan, before);
    }

    /// …and with no tree registered at all, the scoped form runs untouched.
    #[test]
    fn traversal_without_a_tree_is_untouched() {
        let mut plan = neighbors(Some("docs"));
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A graph algorithm runs over every edge of the collection.
    #[test]
    fn algo_is_refused_under_a_tree() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Graph(GraphOp::Algo {
            algorithm: GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: "docs".into(),
                edge_label: None,
                damping: None,
                max_iterations: None,
                tolerance: None,
                source_node: None,
                sample_size: None,
                direction: None,
                resolution: None,
                mode: None,
                personalization_vector: None,
            },
        });
        assert_refused(apply(&mut plan, &cache), "docs");
    }

    /// Tenant-wide graph stats cannot be narrowed, so any tree refuses them.
    #[test]
    fn unscoped_stats_is_refused_while_any_tree_applies() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert!(matches!(
            apply(&mut plan, &cache),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
