//! Clause- and chain-level MATCH execution.

use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::pattern::ast::{MatchClause, PatternChain};
use crate::engine::graph::pattern::executor::continuation;
use crate::engine::graph::pattern::executor::types::{BindingRow, ExecutionState};

/// Execute a single MATCH clause.
pub(in crate::engine::graph::pattern::executor) fn execute_clause(
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
