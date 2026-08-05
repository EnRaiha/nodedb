// SPDX-License-Identifier: BUSL-1.1

//! Refusal of graph reads that column redaction cannot cover.
//!
//! A traversal or a pattern match returns graph topology — node ids and edge
//! labels — not row bodies, so there are no columns in its result for a
//! redaction rule to rewrite. What it does disclose is the shape of the data
//! whose columns a policy protects, and the pattern's own `WHERE` can probe a
//! redacted column value one predicate at a time. Both are refused while a
//! rule exists, mirroring how RLS resolves the same shape (see
//! `rls_injection::plan`).

use nodedb_physical::physical_plan::GraphOp;

use super::lookup::RefusalCtx;

pub(super) fn refuse_graph_op(op: &GraphOp, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    match op {
        // A traversal with no collection (`None`) is a tree-index walk scoped
        // by edge label; no catalog record maps the index back to a
        // collection, so there is no policy to consult — identical to how RLS
        // treats the same shape.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_deref() {
            Some(collection) => refuse_traversal(ctx, collection),
            None => Ok(()),
        },

        // The bitemporal neighbor lookup always names its collection: the
        // versioned edge key layout is collection-scoped.
        GraphOp::TemporalNeighbors { collection, .. } => refuse_traversal(ctx, collection),

        // A pattern match names its collection inside the serialized query
        // (the `IN '<collection>'` clause), not on the plan node.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Edge and label writes carry no read result; the algorithm,
        // superstep, and stats ops return whole-graph scalars (ranks,
        // component ids, counters) rather than a collection's columns, and
        // RAG fusion returns document rows that the result-path redaction
        // hook rewrites like any other scan. None of these has an
        // unredactable column read to refuse.
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::Stats { .. } => Ok(()),
    }
}

pub(super) fn refuse_traversal(ctx: &RefusalCtx<'_>, collection: &str) -> crate::Result<()> {
    if collection.is_empty() || !ctx.collection_is_redacted(collection) {
        return Ok(());
    }
    Err(crate::Error::PlanError {
        detail: format!(
            "redaction policies on '{collection}' are not supported with graph traversal: a \
             traversal returns graph topology, which column redaction cannot be applied to"
        ),
    })
}

/// Refuse a pattern match whose target collection carries a rule.
///
/// The collection lives in the serialized `MatchQuery` — the plan node carries
/// only the encoded query — so it is decoded here to keep the refusal narrow:
/// a match scoped with `IN '<collection>'` to a collection with no rule for
/// this identity still runs.
///
/// A query that names no collection may traverse any of the tenant's edges, and
/// one that fails to decode cannot be shown to avoid a protected collection.
/// Both fall back to the tenant-wide question — the plan is refused only when
/// this identity actually holds a redaction rule somewhere.
pub(super) fn refuse_match(ctx: &RefusalCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    refuse_match_scoped(
        ctx,
        decoded.ok().and_then(|query| query.collection).as_deref(),
    )
}

/// Refuse a pattern match already known to be scoped (or not) to `collection`.
///
/// Shares the fail-closed fallback with [`refuse_match`] for a caller that
/// already holds the decoded `MatchQuery` and would otherwise have to
/// re-serialize it just to decode it back here.
pub(super) fn refuse_match_scoped(
    ctx: &RefusalCtx<'_>,
    collection: Option<&str>,
) -> crate::Result<()> {
    match collection {
        Some(collection) => refuse_traversal(ctx, collection),
        None => refuse_unscoped_match(ctx),
    }
}

fn refuse_unscoped_match(ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    if !ctx.identity_has_any_rule() {
        return Ok(());
    }
    Err(crate::Error::PlanError {
        detail: "graph pattern matching is not supported while a redaction policy applies to \
                 this role and the pattern names no collection: the match returns graph \
                 topology, which column redaction cannot be applied to, and its scope cannot be \
                 narrowed to an unprotected collection"
            .to_string(),
    })
}
