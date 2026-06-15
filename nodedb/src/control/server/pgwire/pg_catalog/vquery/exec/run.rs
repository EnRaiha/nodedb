// SPDX-License-Identifier: BUSL-1.1

//! Top-level virtual-query executor: filter → sort → aggregate-or-project → limit.

use super::super::expr::types::{EvalError, Expr};
use super::super::expr::{EvalCtx, eval, truthy};
use super::super::select::{ParseError, VSelect};
use super::super::table::VTable;
use super::super::value::VValue;
use super::meta::ResultSet;
use super::project::{project_aggregate, project_rowwise};

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("eval: {0}")]
    Eval(#[from] EvalError),
    #[error("{0}")]
    Parse(#[from] ParseError),
    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Execute `select` against an already-materialized (and, for joins, combined)
/// input table.
pub fn execute(select: &VSelect, input: VTable, ctx: &EvalCtx) -> Result<ResultSet, ExecError> {
    // 1. WHERE.
    let mut filtered: Vec<Vec<VValue>> = Vec::with_capacity(input.rows.len());
    for row in &input.rows {
        let keep = match &select.filter {
            Some(predicate) => truthy(&eval(predicate, row, &input, ctx)?),
            None => true,
        };
        if keep {
            filtered.push(row.clone());
        }
    }

    // 2. ORDER BY — evaluated against the input schema, so sort the filtered
    //    input rows before projection (which may reorder/rename columns).
    //    Aggregate queries collapse to a single row, so sorting is skipped.
    if !select.order_by.is_empty() && !select.has_aggregate {
        sort_rows(&mut filtered, &select.order_by, &input, ctx)?;
    }

    // 3. Projection (aggregate vs. row-wise).
    let (out_cols, mut out_rows) = if select.has_aggregate {
        project_aggregate(select, &filtered, &input, ctx)?
    } else {
        project_rowwise(select, &filtered, &input, ctx)?
    };

    // 4. OFFSET / LIMIT.
    if select.offset > 0 {
        let skip = select.offset.min(out_rows.len());
        out_rows.drain(..skip);
    }
    if let Some(limit) = select.limit
        && out_rows.len() > limit
    {
        out_rows.truncate(limit);
    }

    Ok(ResultSet {
        columns: out_cols,
        rows: out_rows,
    })
}

fn sort_rows(
    rows: &mut [Vec<VValue>],
    keys: &[(Expr, bool)],
    table: &VTable,
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    // Pre-compute each row's sort-key tuple against the input schema.
    let mut keyed: Vec<(usize, Vec<VValue>)> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let mut key = Vec::with_capacity(keys.len());
        for (expr, _) in keys {
            key.push(eval(expr, row, table, ctx)?);
        }
        keyed.push((i, key));
    }
    keyed.sort_by(|a, b| {
        for (i, (_, asc)) in keys.iter().enumerate() {
            let ord = match a.1[i].sql_cmp(&b.1[i]) {
                Some(o) => o,
                None => match (a.1[i].is_null(), b.1[i].is_null()) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                },
            };
            if ord != std::cmp::Ordering::Equal {
                return if *asc { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    });

    let original: Vec<Vec<VValue>> = rows.to_vec();
    for (new_pos, (orig_idx, _)) in keyed.into_iter().enumerate() {
        rows[new_pos] = original[orig_idx].clone();
    }
    Ok(())
}
