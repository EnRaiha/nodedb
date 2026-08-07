// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for graph-overlay operations.
//!
//! No graph read carries a row-filter slot the storage layer can honour: a
//! traversal returns topology, an algorithm returns per-node scalars, a
//! pattern match returns bindings, and RAG fusion returns fused document rows
//! through the fusion envelope. Each therefore refuses while a read policy
//! restricts the identity on the collection being read, rather than returning
//! rows — or the shape of rows — the policy says are not the caller's to see.
//!
//! The redaction pass refuses the traversal and match shapes here for the same
//! reason. It permits the algorithm, stats, and RAG-fusion shapes, because
//! those disclose no column value a rule could mask; RLS still refuses them,
//! because what a row policy restricts is the row set itself, and a rank
//! vector, a counter, and a fused document row all derive from rows the policy
//! hides.

use nodedb_physical::physical_plan::GraphOp;

use super::context::RlsCtx;

const TRAVERSAL_REASON: &str =
    "a traversal returns graph topology, which the row filter cannot be evaluated against";

const EDGE_WRITE_REASON: &str = "an edge write carries endpoints and a label rather than the row body the policy names, so no \
     row image is available for it to be evaluated against";

const ALGORITHM_REASON: &str = "an algorithm returns per-node scalars computed over every edge, which the row filter cannot \
     be evaluated against";

/// Exhaustive over [`GraphOp`] so a new graph operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_graph(ctx: &RlsCtx<'_>, op: &GraphOp) -> crate::Result<()> {
    match op {
        // Refuse: a traversal returns node ids and edge labels, not row bodies
        // — the rows are fetched later through `DocumentOp::PointGet`, which
        // applies the policy then. What the traversal itself discloses is
        // topology: which nodes exist and how they connect. A read policy says
        // some of those rows are not the caller's to see, and their edges are
        // equally not.
        //
        // A traversal with no collection (`None`) is a tree-index walk scoped
        // by edge label; no catalog record maps an index back to the
        // collection it was built on, so there is no policy to consult, and
        // the DDL that builds such an index is authorized separately.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_deref() {
            Some(collection) => ctx.refuse_if_policy(collection, TRAVERSAL_REASON),
            None => Ok(()),
        },

        // Refuse: same shape as `Neighbors`. The bitemporal form always names
        // its collection — the versioned edge key layout is collection-scoped.
        GraphOp::TemporalNeighbors { collection, .. } => {
            ctx.refuse_if_policy(collection, TRAVERSAL_REASON)
        }

        // Refuse: a pattern match returns variable bindings over topology with
        // no row-filter slot, and its own `WHERE` can probe a hidden row's
        // field one predicate at a time. The collection lives inside the
        // serialized query rather than on the plan node.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Refuse: the algorithm runs over the whole CSR for the collection and
        // returns ranks / component ids / counts derived from every row,
        // including the ones the policy hides, through a payload with no row
        // to filter.
        GraphOp::Algo { params, .. } | GraphOp::TemporalAlgorithm { params, .. } => {
            ctx.refuse_if_policy(&params.collection, ALGORITHM_REASON)
        }

        // Refuse: the distributed supersteps are the same algorithms one round
        // at a time, carrying the target collection in their params.
        GraphOp::BspSuperstep(plan) => {
            ctx.refuse_if_policy(&plan.params.collection, ALGORITHM_REASON)
        }
        GraphOp::WccSuperstep(plan) => {
            ctx.refuse_if_policy(&plan.params.collection, ALGORITHM_REASON)
        }

        // Refuse: RAG fusion returns fused document rows, but the fusion
        // envelope has no `rls_filters` slot and embeds no sub-plan to recurse
        // into — the vector, text, and graph legs all run inside the handler.
        // So the rows a policy hides would be ranked and returned.
        GraphOp::RagFusion { collection, .. } => ctx.refuse_if_policy(
            collection,
            "fusion returns ranked document rows through a fused response shape that carries no \
             row filter",
        ),

        // Refuse: the counters summarize the collection's edges, so they count
        // rows the policy hides, and a counter carries no row to filter.
        // `collection = None` reports every collection that has edges, so the
        // narrow per-collection question cannot be asked.
        GraphOp::Stats { collection, .. } => match collection.as_deref() {
            Some(collection) => ctx.refuse_if_policy(
                collection,
                "graph statistics are counters over the collection's edges, which the row filter \
                 cannot be evaluated against",
            ),
            None => ctx.refuse_if_any_policy(
                "graph statistics report counters for every collection holding edges, which the \
                 row filter cannot be evaluated against",
            ),
        },

        // Refuse: an edge write carries endpoints and a label, not the row body
        // a policy predicate names, so there is no image the write policy can
        // be evaluated against. Topology is exactly what a read policy on this
        // collection already refuses to disclose, and writing it is the same
        // claim in reverse.
        GraphOp::EdgePut { collection, .. } | GraphOp::EdgeDelete { collection, .. } => {
            ctx.refuse_if_write_policy(collection, EDGE_WRITE_REASON)
        }

        // Refuse: the batch forms name no collection — each edge carries its
        // own — so the narrow question cannot be asked, and node labels are
        // keyed on a node id that no plan field binds to a collection. Both
        // fall back to the tenant-wide question, the same fallback every
        // collection-less shape in this pass uses.
        GraphOp::EdgePutBatch { .. } | GraphOp::EdgeDeleteBatch { .. } => {
            ctx.refuse_if_any_write_policy(EDGE_WRITE_REASON)
        }
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => ctx
            .refuse_if_any_write_policy(
                "a node-label write is keyed on a node id that names no collection, and it carries \
                 no row body for the policy to be evaluated against",
            ),
    }
}

/// Refuse a pattern match whose target collection carries a read policy.
///
/// The collection lives in the serialized `MatchQuery` — the plan node carries
/// only the encoded query — so it is decoded here to keep the refusal narrow:
/// a match scoped with `IN '<collection>'` to a collection no policy restricts
/// still runs.
///
/// A query that names no collection may traverse any of the tenant's edges,
/// and one that fails to decode cannot be shown to avoid a protected
/// collection. Both fall back to the tenant-wide question, exactly as the
/// redaction pass does for the same shape.
fn refuse_match(ctx: &RlsCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    match decoded.ok().and_then(|query| query.collection) {
        Some(collection) => ctx.refuse_if_policy(
            &collection,
            "a pattern match returns bindings over graph topology, which the row filter cannot be \
             evaluated against",
        ),
        None => ctx.refuse_if_any_policy(
            "a pattern match returns bindings over graph topology, which the row filter cannot be \
             evaluated against, and the pattern's scope cannot be narrowed to an unrestricted \
             collection",
        ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_graph::{AlgoParams, GraphAlgorithm};
    use nodedb_physical::physical_plan::GraphOp;

    use super::super::plan::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::engine::graph::pattern::ast::MatchQuery;

    fn algo_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Algo {
            algorithm: GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: collection.into(),
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
        })
    }

    fn match_plan(collection: Option<&str>) -> PhysicalPlan {
        let query = MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: collection.map(str::to_string),
        };
        PhysicalPlan::Graph(GraphOp::Match {
            query: zerompk::to_msgpack_vec(&query).expect("encode match query"),
            frontier_bitmap: None,
            cluster_mode: false,
        })
    }

    /// A pattern match scoped to a policed collection is refused.
    #[test]
    fn scoped_match_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(Some("users"));
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// …and the same pattern scoped elsewhere still runs.
    #[test]
    fn match_on_an_unpoliced_collection_runs() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(Some("orders"));
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// An unscoped match may traverse anything the tenant holds, so it falls
    /// back to the tenant-wide question.
    #[test]
    fn unscoped_match_falls_back_to_the_tenant_wide_question() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(None);
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::PlanError { .. })
        ));
    }

    /// With no policy at all, both match shapes run untouched.
    #[test]
    fn match_without_a_policy_is_untouched() {
        for collection in [Some("users"), None] {
            let mut plan = match_plan(collection);
            let before = plan.clone();
            assert!(inject_without_policy(&mut plan).is_ok());
            assert_eq!(plan, before);
        }
    }

    /// An edge write carries endpoints and a label rather than the row body a
    /// write policy names, so it is refused rather than persisted unchecked.
    #[test]
    fn edge_put_is_refused_under_a_write_policy() {
        use super::super::plan::test_support::{assert_write_refused, store_with_write_policy};

        let store = store_with_write_policy("users");
        let mut plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "users".into(),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            properties: Vec::new(),
            src_surrogate: nodedb_types::Surrogate::ZERO,
            dst_surrogate: nodedb_types::Surrogate::ZERO,
        });
        assert_write_refused(inject(&mut plan, &store), "users");
    }

    /// A graph algorithm runs over every edge of the collection.
    #[test]
    fn algo_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = algo_plan("users");
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn algo_without_a_policy_is_untouched() {
        let mut plan = algo_plan("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// Collection-scoped graph stats count edges of rows the policy hides.
    #[test]
    fn scoped_stats_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: Some("users".into()),
            as_of: None,
        });
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// Tenant-wide stats cannot be narrowed, so any read policy refuses them.
    #[test]
    fn unscoped_stats_is_refused_while_any_policy_applies() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::PlanError { .. })
        ));
    }

    /// …and run normally when the tenant has no read policy.
    #[test]
    fn unscoped_stats_without_a_policy_is_untouched() {
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
