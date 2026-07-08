// SPDX-License-Identifier: BUSL-1.1

//! MATCH execution functions — top-level entry points and triple evaluation.

mod binding;
mod join;
#[cfg(test)]
pub(super) mod tests;
mod triple;

use std::collections::HashMap;

use super::super::ast::*;
use super::continuation;
use super::expansion;
use super::expansion::VarLenCaps;
use super::predicates::PropertyLookup;
use super::types::{BindingRow, ExecutionState, MatchOutcome};
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::EdgeStore;

pub(in crate::engine::graph::pattern::executor) use binding::{bind_node, binding_compatible};
use join::left_join_rows;
pub(in crate::engine::graph::pattern::executor) use triple::execute_triple;

/// Borrowed execution context shared by every MATCH entry point: the CSR
/// index, edge store, cross-shard frontier/remote-node hooks, variable-length
/// caps, property lookup, and the in-transaction staged-edge overlay.
///
/// Bundles the parameters that travel together on every `execute*` call so
/// each entry point stays within clippy's argument budget.
#[derive(Clone, Copy)]
pub struct MatchExecCtx<'a> {
    pub csr: &'a CsrIndex,
    pub edge_store: &'a EdgeStore,
    pub frontier_bitmap: Option<&'a nodedb_types::SurrogateBitmap>,
    pub is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
    pub varlen_caps: VarLenCaps,
    pub props: &'a PropertyLookup<'a>,
    pub overlay: Option<&'a GraphOverlayDelta>,
}

/// Execute a MATCH query on a CSR index and edge store.
///
/// Applies join order optimization before execution: triples within each
/// PatternChain are reordered by selectivity (lowest edge count first,
/// bound variables preferred).
///
/// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
/// bitmap are eligible as pattern anchors. Bound variables (already resolved
/// from a prior binding row) bypass the bitmap check — only free-variable
/// anchor enumeration is restricted.
///
/// `is_remote_node`: when `Some(pred)`, `pred(node_name)` returns `true` for
/// nodes homed on a remote shard. Only nodes that (a) were reached via a
/// **bound** source variable (resolved from `input_row`, not free-ranged),
/// AND (b) satisfy this predicate, AND (c) have zero raw directional
/// adjacency, are added to `unresolved_frontier`.  Free-ranging anchors never
/// emit, even when the predicate and degree conditions hold, because each
/// shard's own pass covers all its local nodes.
/// Pass `None` (the production default on a fully-local CSR) to guarantee
/// an always-empty frontier, preserving byte-identical single-node behaviour.
///
/// `overlay`: when `Some(delta)` and the delta is non-empty, the query runs
/// inside a transaction and each fixed-hop triple observes the transaction's
/// own staged edge writes/deletes (read-your-own-writes) via the name-keyed
/// merge in [`super::overlay_expand`]. `None` (or an empty delta) is the
/// autocommit path and is byte-identical to committed-CSR-only execution.
pub fn execute<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
) -> Result<MatchOutcome, crate::Error> {
    // Optimize query before execution (reorder triples by selectivity). The
    // optimizer only REORDERS triples within a chain (it never drops one), and
    // a staged-only edge label has zero CSR edges so it scores as most
    // selective and simply sorts first — every triple is still visited, so a
    // staged edge/node cannot be pruned out of the plan.
    let mut optimized = query.clone();
    super::super::optimizer::optimize(&mut optimized, ctx.csr);
    execute_query(&optimized, ctx)
}

/// Execute a pre-optimized MATCH query (internal, skip optimizer).
fn execute_query<'a>(
    query: &MatchQuery,
    ctx: MatchExecCtx<'a>,
) -> Result<MatchOutcome, crate::Error> {
    let MatchExecCtx {
        csr,
        edge_store,
        frontier_bitmap,
        is_remote_node,
        varlen_caps,
        props,
        overlay,
    } = ctx;
    let mut rows: Vec<BindingRow> = vec![HashMap::new()];
    let mut state = ExecutionState::new(is_remote_node, varlen_caps);
    // Resolve the `IN '<collection>'` scoping once against this partition's
    // collection interning; every edge expansion in this execution is filtered
    // by it so a collection-scoped MATCH never traverses another collection's
    // edges (they share one CSR partition).
    state.collection_filter =
        expansion::resolve_collection_filter(query.collection.as_deref(), csr);

    for clause in &query.clauses {
        let clause_rows = execute_clause(clause, csr, &rows, &mut state, frontier_bitmap, overlay)?;
        if clause.optional {
            rows = left_join_rows(&rows, &clause_rows, clause);
        } else {
            rows = clause_rows;
        }
    }

    let rows = continuation::finalize_rows(
        query,
        rows,
        csr,
        edge_store,
        state.varlen_caps,
        props,
        overlay,
    )?;

    Ok(MatchOutcome {
        rows,
        truncation: state.varlen_resume,
        unresolved_frontier: state.frontier,
    })
}

/// Serialize binding rows to MessagePack for SPSC transport.
///
/// The Data Plane MUST produce MessagePack so that broadcast merge
/// (`extract_msgpack_elements`) can correctly split and re-merge rows
/// from multiple cores. BindingRow is `HashMap<String, String>` — all
/// values are strings, so we write raw msgpack directly.
pub fn rows_to_msgpack(rows: &[BindingRow]) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::{write_array_header, write_map_header, write_str};

    // MATCH bindings now carry user-visible node ids directly. The
    // CSR partition that produced them is tenant-scoped by
    // construction, so there is no `<tid>:` prefix to strip — what
    // the user inserted is what the user sees back.
    let mut buf = Vec::with_capacity(rows.len() * 64);
    write_array_header(&mut buf, rows.len());
    for row in rows {
        write_map_header(&mut buf, row.len());
        for (k, v) in row {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }
    }
    Ok(buf)
}

/// Execute a single MATCH clause.
pub(super) fn execute_clause(
    clause: &MatchClause,
    csr: &CsrIndex,
    input_rows: &[BindingRow],
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut result_rows = input_rows.to_vec();

    for chain in &clause.patterns {
        let mut next_rows = Vec::new();
        for row in &result_rows {
            next_rows.extend(execute_chain(
                chain,
                csr,
                row,
                state,
                frontier_bitmap,
                overlay,
            )?);
        }
        result_rows = next_rows;
    }

    Ok(result_rows)
}

/// Execute a single pattern chain against a binding row.
///
/// Thin wrapper over [`continuation::run_chain_from`] that starts at triple 0
/// with the single supplied input row — the from-scratch execution path.
fn execute_chain(
    chain: &PatternChain,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    continuation::run_chain_from(
        chain,
        0,
        vec![input_row.clone()],
        csr,
        state,
        frontier_bitmap,
        overlay,
    )
}
