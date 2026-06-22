// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard MATCH resume — the executor RESUME entry-point.
//!
//! When a shard cannot expand a bound source node (its edges are homed on
//! another shard) it emits an `UnresolvedExpansion` frontier entry. The
//! Control Plane dispatches a *continuation* to the owning shard, which
//! resumes the SAME pattern from where the originating shard left off via
//! [`execute_continuation`].

use super::super::ast::{MatchQuery, PatternChain};
use super::{BindingRow, ExecutionState, MatchOutcome, execute_triple, predicates};
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;

/// Chain-execution core: expand a pattern chain's triples starting at
/// `start_idx`, threading `initial_rows` through each remaining triple.
///
/// This is the single source of truth for triple iteration. The from-scratch
/// path calls it with `start_idx = 0` and a single seed row (via
/// `execute_chain`); the cross-shard resume path calls it with
/// `start_idx = resume_triple_idx` and a seed row whose first
/// `resume_triple_idx` triples are already bound (via [`execute_continuation`]).
///
/// `triple_idx` passed to [`execute_triple`] is the absolute 0-based index of
/// the triple WITHIN ITS CHAIN — identical to the index recorded in any
/// emitted `UnresolvedExpansion`. Skipped triples `[0, start_idx)` are assumed
/// already satisfied by the bindings present in `initial_rows`.
pub(super) fn run_chain_from(
    chain: &PatternChain,
    start_idx: usize,
    initial_rows: Vec<BindingRow>,
    csr: &CsrIndex,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut rows = initial_rows;

    for (triple_idx, triple) in chain.triples.iter().enumerate().skip(start_idx) {
        let mut next_rows = Vec::new();
        for row in &rows {
            next_rows.extend(execute_triple(
                triple,
                triple_idx,
                csr,
                row,
                state,
                frontier_bitmap,
            )?);
        }
        rows = next_rows;
        if rows.is_empty() {
            break;
        }
    }

    Ok(rows)
}

/// Apply the query tail — WHERE predicates, LIMIT, RETURN projection, and
/// DISTINCT — to a fully-expanded set of binding rows.
///
/// This is the shared post-chain finalization step. Both the from-scratch
/// path (`execute_query`) and the cross-shard resume path
/// ([`execute_continuation`]) funnel their expanded rows through here so the
/// tail semantics are identical regardless of where expansion started.
pub(super) fn finalize_rows(
    query: &MatchQuery,
    mut rows: Vec<BindingRow>,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    for predicate in &query.where_predicates {
        rows = predicates::apply_predicate(&rows, predicate, csr, edge_store, frontier_bitmap)?;
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    if !query.return_columns.is_empty() {
        rows = predicates::project_columns(&rows, &query.return_columns);
    }

    if query.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            // Build a sorted-key representation so that two BindingRows with
            // the same entries but different HashMap iteration orders are
            // treated as identical. `format!("{row:?}")` on a HashMap is
            // non-deterministic in key order, which would miss duplicates.
            let mut pairs: Vec<(&String, &String)> = row.iter().collect();
            pairs.sort_unstable_by_key(|(k, _)| *k);
            let key = format!("{pairs:?}");
            seen.insert(key)
        });
    }

    Ok(rows)
}

/// Resume a MATCH pattern on THIS shard's CSR starting at `resume_triple_idx`.
///
/// # Why this does NOT optimize
///
/// [`super::execute`] reorders the query's triples by per-shard selectivity
/// (using THIS CSR's edge counts) before running. A continuation MUST NOT
/// re-optimize: `resume_triple_idx` is an index into the **originating
/// shard's already-optimized triple order**. Re-optimizing here against a
/// different CSR's edge counts could yield a different order, so
/// `resume_triple_idx` would point at the wrong triple. The caller therefore
/// passes the originating shard's already-optimized `query` AS GIVEN, and this
/// function runs it verbatim — the optimizer is never invoked on the resume
/// path.
///
/// # How it resumes
///
/// `seed_row` carries all bindings accumulated by the originating shard up to
/// (and including) the source node being resumed from — i.e. the bindings for
/// triples `[0, resume_triple_idx)` are already present. This function seeds
/// the row set as `vec![seed_row]` and runs [`run_chain_from`] starting at
/// `resume_triple_idx`, skipping the already-satisfied prefix triples. The
/// query tail (WHERE / LIMIT / RETURN / DISTINCT) is then applied via
/// [`finalize_rows`], identically to the from-scratch path.
///
/// # `resume_triple_idx` semantics
///
/// `resume_triple_idx` is the index of the triple **within its pattern
/// chain** — the exact value the originating shard recorded in
/// `UnresolvedExpansion::triple_idx` (produced by `execute_chain`'s
/// `enumerate`). This is a within-chain index, not a global flattening across
/// clauses; the single-clause MATCH case (the dominant case) has exactly one
/// chain, so the within-chain index and the pattern index coincide.
///
/// # Multi-clause limitation
///
/// Resuming mid-pattern is only well-defined for a single MATCH clause with a
/// single pattern chain (`resume_triple_idx` indexes that chain). A query with
/// multiple clauses (e.g. `OPTIONAL MATCH`) or multiple comma-separated
/// patterns in the resumed clause makes mid-pattern resume ambiguous — there
/// is no single chain that `resume_triple_idx` unambiguously indexes. Rather
/// than silently mis-handle it (which would produce wrong results), this
/// function returns a typed `BadRequest` error for that case. The frontier is
/// only ever emitted from the single-chain expansion path today, so this
/// guard is defensive against a future multi-clause caller.
pub fn execute_continuation<'a>(
    query: &MatchQuery,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
    is_remote_node: Option<&'a dyn Fn(&str) -> bool>,
    resume_triple_idx: usize,
    seed_row: BindingRow,
) -> Result<MatchOutcome, crate::Error> {
    // Mid-pattern resume is only unambiguous for a single clause holding a
    // single pattern chain. Reject anything else with a typed error rather
    // than guessing which chain `resume_triple_idx` refers to.
    let chain = match query.clauses.as_slice() {
        [clause] if clause.patterns.len() == 1 => &clause.patterns[0],
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "cross-shard MATCH continuation is only supported for a single \
                         MATCH clause with a single pattern chain; multi-clause / \
                         multi-pattern continuation is not yet supported"
                    .to_string(),
            });
        }
    };

    if resume_triple_idx > chain.triples.len() {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "cross-shard MATCH continuation resume_triple_idx {resume_triple_idx} \
                 exceeds chain length {}",
                chain.triples.len()
            ),
        });
    }

    let mut state = ExecutionState::new(is_remote_node);

    // Resume the chain from the originating shard's stopping point. The seed
    // row already carries the bindings for triples [0, resume_triple_idx).
    let rows = run_chain_from(
        chain,
        resume_triple_idx,
        vec![seed_row],
        csr,
        &mut state,
        frontier_bitmap,
    )?;

    let rows = finalize_rows(query, rows, csr, edge_store, frontier_bitmap)?;

    Ok(MatchOutcome {
        rows,
        truncated: state.truncated,
        unresolved_frontier: state.frontier,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::make_csr;
    use super::super::{BindingRow, execute};
    use super::execute_continuation;

    /// Resume produces the correct tail. Graph `(x)-[:E]->(y)-[:E]->(z)` with
    /// `root -E-> mid -E-> leaf`. Resume at triple_idx 1 with the seed bindings
    /// `{x:root, y:mid}` (i.e. triple 0 already satisfied on the originating
    /// shard). `mid` HAS a local out-edge `mid->leaf`, so the tail resolves to
    /// `z = leaf` and the row carries through the seed bindings.
    #[test]
    fn continuation_resumes_tail_with_seed_bindings() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid"), ("mid", "E", "leaf")]);
        let query =
            super::super::super::compiler::parse("MATCH (x)-[:E]->(y)-[:E]->(z) RETURN x, y, z")
                .unwrap();

        let mut seed = BindingRow::new();
        seed.insert("x".to_string(), "root".to_string());
        seed.insert("y".to_string(), "mid".to_string());

        let outcome = execute_continuation(&query, &csr, &store, None, None, 1, seed).unwrap();

        assert_eq!(outcome.rows.len(), 1, "expected exactly one tail row");
        assert_eq!(
            outcome.rows[0]["x"], "root",
            "seed binding x carried through"
        );
        assert_eq!(
            outcome.rows[0]["y"], "mid",
            "seed binding y carried through"
        );
        assert_eq!(outcome.rows[0]["z"], "leaf", "tail resolved z=leaf");
        assert!(outcome.unresolved_frontier.is_empty());
    }

    /// Resume with no matching tail edge yields empty rows. `mid` has NO local
    /// out-edge, so resuming triple 1 from `{x:root, y:mid}` produces nothing.
    #[test]
    fn continuation_no_matching_tail_edge_is_empty() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid")]);
        let query =
            super::super::super::compiler::parse("MATCH (x)-[:E]->(y)-[:E]->(z) RETURN x, y, z")
                .unwrap();

        let mut seed = BindingRow::new();
        seed.insert("x".to_string(), "root".to_string());
        seed.insert("y".to_string(), "mid".to_string());

        let outcome = execute_continuation(&query, &csr, &store, None, None, 1, seed).unwrap();

        assert!(
            outcome.rows.is_empty(),
            "mid has no local out-edge; tail must be empty, got {:?}",
            outcome.rows
        );
    }

    /// `execute()` (the from-scratch path) is unchanged: a full query still
    /// returns the same rows. Sanity that the chain-core refactor preserved
    /// from-scratch behaviour.
    #[test]
    fn full_execute_unchanged_after_refactor() {
        let (csr, store, _dir) = make_csr(&[("root", "E", "mid"), ("mid", "E", "leaf")]);
        let query = super::super::super::compiler::parse(
            "MATCH (x)-[:E]->(y)-[:E]->(z) WHERE x = 'root' RETURN x, y, z",
        )
        .unwrap();
        let rows = execute(&query, &csr, &store, None, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["x"], "root");
        assert_eq!(rows[0]["y"], "mid");
        assert_eq!(rows[0]["z"], "leaf");
    }
}
