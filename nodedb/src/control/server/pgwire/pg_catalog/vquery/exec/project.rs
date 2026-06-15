// SPDX-License-Identifier: BUSL-1.1

//! Projection: row-wise column output and single-group aggregation.

use super::super::expr::types::{AggFn, Expr};
use super::super::expr::{EvalCtx, eval};
use super::super::select::{VProj, VSelect};
use super::super::table::VTable;
use super::super::value::{VType, VValue};
use super::meta::{OutColumn, aggregate_name, infer_type, projection_name};
use super::run::ExecError;

type Projected = (Vec<OutColumn>, Vec<Vec<VValue>>);

pub fn project_rowwise(
    select: &VSelect,
    rows: &[Vec<VValue>],
    table: &VTable,
    ctx: &EvalCtx,
) -> Result<Projected, ExecError> {
    // Build output schema, recording for each projection item which source
    // column indices it expands to (for `*` / `t.*`).
    let mut out_cols: Vec<OutColumn> = Vec::new();
    for item in &select.projection {
        match item {
            VProj::Star => {
                for col in &table.columns {
                    out_cols.push(OutColumn {
                        name: col.name.to_string(),
                        ty: col.ty,
                    });
                }
            }
            VProj::QualifiedStar(alias) => {
                for idx in star_indices(table, alias) {
                    let col = &table.columns[idx];
                    out_cols.push(OutColumn {
                        name: col.name.to_string(),
                        ty: col.ty,
                    });
                }
            }
            VProj::Expr { expr, alias } => out_cols.push(OutColumn {
                name: alias.clone().unwrap_or_else(|| projection_name(expr)),
                ty: infer_type(expr, table),
            }),
        }
    }

    let mut out_rows: Vec<Vec<VValue>> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out_row: Vec<VValue> = Vec::with_capacity(out_cols.len());
        for item in &select.projection {
            match item {
                VProj::Star => out_row.extend_from_slice(row),
                VProj::QualifiedStar(alias) => {
                    for idx in star_indices(table, alias) {
                        out_row.push(row[idx].clone());
                    }
                }
                VProj::Expr { expr, .. } => out_row.push(eval(expr, row, table, ctx)?),
            }
        }
        out_rows.push(out_row);
    }
    Ok((out_cols, out_rows))
}

fn star_indices(table: &VTable, alias: &str) -> Vec<usize> {
    table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            c.qualifier
                .as_deref()
                .map(|q| q.eq_ignore_ascii_case(alias))
                .unwrap_or(false)
        })
        .map(|(i, _)| i)
        .collect()
}

pub fn project_aggregate(
    select: &VSelect,
    rows: &[Vec<VValue>],
    table: &VTable,
    ctx: &EvalCtx,
) -> Result<Projected, ExecError> {
    let mut out_cols: Vec<OutColumn> = Vec::with_capacity(select.projection.len());
    let mut out_row: Vec<VValue> = Vec::with_capacity(select.projection.len());

    for item in &select.projection {
        let VProj::Expr { expr, alias } = item else {
            return Err(ExecError::Unsupported(
                "cannot mix * with aggregate projection on virtual tables".into(),
            ));
        };
        let Expr::Aggregate(agg, arg) = expr else {
            return Err(ExecError::Unsupported(
                "non-aggregate expressions in an aggregate projection are not supported \
                 (use GROUP BY)"
                    .into(),
            ));
        };

        let (value, ty) = compute_aggregate(*agg, arg, rows, table, ctx)?;
        out_cols.push(OutColumn {
            name: alias.clone().unwrap_or_else(|| aggregate_name(*agg)),
            ty,
        });
        out_row.push(value);
    }

    Ok((out_cols, vec![out_row]))
}

fn compute_aggregate(
    agg: AggFn,
    arg: &Expr,
    rows: &[Vec<VValue>],
    table: &VTable,
    ctx: &EvalCtx,
) -> Result<(VValue, VType), ExecError> {
    match agg {
        AggFn::Count => {
            let n = match arg {
                Expr::Star => rows.len() as i64,
                _ => {
                    let mut c: i64 = 0;
                    for row in rows {
                        if !eval(arg, row, table, ctx)?.is_null() {
                            c += 1;
                        }
                    }
                    c
                }
            };
            Ok((VValue::Int8(n), VType::Int8))
        }
        AggFn::Sum => {
            let mut acc: i64 = 0;
            let mut saw_any = false;
            for row in rows {
                if let Some(i) = eval(arg, row, table, ctx)?.as_i64() {
                    acc = acc.wrapping_add(i);
                    saw_any = true;
                }
            }
            Ok((
                if saw_any {
                    VValue::Int8(acc)
                } else {
                    VValue::Null
                },
                VType::Int8,
            ))
        }
        AggFn::Min | AggFn::Max => {
            let mut best: Option<VValue> = None;
            for row in rows {
                let v = eval(arg, row, table, ctx)?;
                if v.is_null() {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let take_new = matches!(
                            (agg, cur.sql_cmp(&v)),
                            (AggFn::Min, Some(std::cmp::Ordering::Greater))
                                | (AggFn::Max, Some(std::cmp::Ordering::Less))
                        );
                        if take_new { v } else { cur }
                    }
                });
            }
            Ok((best.unwrap_or(VValue::Null), infer_type(arg, table)))
        }
        AggFn::Avg => {
            let mut sum: i64 = 0;
            let mut n: i64 = 0;
            for row in rows {
                if let Some(i) = eval(arg, row, table, ctx)?.as_i64() {
                    sum = sum.wrapping_add(i);
                    n += 1;
                }
            }
            Ok((
                if n == 0 {
                    VValue::Null
                } else {
                    VValue::Int8(sum / n)
                },
                VType::Int8,
            ))
        }
    }
}
