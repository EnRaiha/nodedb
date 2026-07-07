// SPDX-License-Identifier: Apache-2.0

//! Extract correlation predicates from a LATERAL subquery's WHERE clause.
//!
//! A correlated predicate is one where the left side references the outer
//! table and the right side references the inner table (or vice versa).
//! The outer table is identified by its alias or name.

use sqlparser::ast::{self, Expr, SetExpr};

use crate::parser::normalize::normalize_ident;

/// A single equi-correlation pair extracted from the subquery WHERE.
///
/// `outer_col` is the column name on the outer (driving) side;
/// `inner_col` is the column name on the inner (lateral) side.
#[derive(Debug, Clone)]
pub struct CorrelationEq {
    pub outer_col: String,
    pub inner_col: String,
}

/// Result of analysing a lateral subquery's WHERE clause.
#[derive(Debug, Default)]
pub struct CorrelationAnalysis {
    /// Equi-join pairs `(outer_col, inner_col)` extracted from `inner.col = outer.col`.
    pub equi_keys: Vec<CorrelationEq>,
    /// Non-equi correlated predicates as `(inner_col, outer_col)`. Populated
    /// as a routing signal (its presence forces the `LateralLoop` shape); the
    /// predicates themselves are also retained verbatim in `remaining`.
    pub non_equi: Vec<(String, String)>,
    /// WHERE expression with the *equi* correlated predicates stripped (those
    /// become join keys / correlation keys). Non-equi correlated predicates are
    /// retained here: they lower to `*Column` scan filters whose outer operand
    /// is bound per outer row by the Data Plane executor. `None` when nothing
    /// remains.
    pub remaining: Option<Expr>,
}

/// Analyse the WHERE clause of a LATERAL subquery.
///
/// `outer_alias` is the alias or name of the driving table (e.g. `"u"` for
/// `FROM users u`). Any compound identifier `outer_alias.col` or `col = outer_alias.col`
/// is treated as a correlation reference to the outer side.
pub fn analyse_lateral_where(subquery: &ast::Query, outer_alias: &str) -> CorrelationAnalysis {
    let select = match subquery.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return CorrelationAnalysis::default(),
    };
    let Some(where_expr) = &select.selection else {
        return CorrelationAnalysis::default();
    };

    let mut analysis = CorrelationAnalysis::default();
    analysis.remaining = extract_correlation_recursive(
        where_expr,
        outer_alias,
        &mut analysis.equi_keys,
        &mut analysis.non_equi,
    );
    analysis
}

/// Walk the WHERE expression, extracting correlated predicates.
///
/// Returns `None` when the expression was fully consumed; `Some(expr)` when
/// a non-correlated residual remains.
fn extract_correlation_recursive(
    expr: &Expr,
    outer_alias: &str,
    equi_keys: &mut Vec<CorrelationEq>,
    non_equi: &mut Vec<(String, String)>,
) -> Option<Expr> {
    match expr {
        // AND: recurse both sides.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            let l = extract_correlation_recursive(left, outer_alias, equi_keys, non_equi);
            let r = extract_correlation_recursive(right, outer_alias, equi_keys, non_equi);
            match (l, r) {
                (None, None) => None,
                (Some(e), None) | (None, Some(e)) => Some(e),
                (Some(l), Some(r)) => Some(Expr::BinaryOp {
                    left: Box::new(l),
                    op: ast::BinaryOperator::And,
                    right: Box::new(r),
                }),
            }
        }

        // Equi-predicate: check if one side is an outer reference.
        Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::Eq,
            right,
        } => {
            let lp = compound_parts(left);
            let rp = compound_parts(right);
            match (lp, rp) {
                (Some((lt, lc)), Some((rt, rc))) => {
                    let lc_str = lc.as_str();
                    let rc_str = rc.as_str();
                    let lt_lower = lt.to_lowercase();
                    let rt_lower = rt.to_lowercase();
                    if lt_lower == outer_alias {
                        // left = outer, right = inner
                        equi_keys.push(CorrelationEq {
                            outer_col: lc_str.to_string(),
                            inner_col: rc_str.to_string(),
                        });
                        None
                    } else if rt_lower == outer_alias {
                        // right = outer, left = inner
                        equi_keys.push(CorrelationEq {
                            outer_col: rc_str.to_string(),
                            inner_col: lc_str.to_string(),
                        });
                        None
                    } else {
                        // No outer reference — leave as-is.
                        Some(expr.clone())
                    }
                }
                _ => {
                    // Try non-equi correlation detection. A correlated
                    // non-equi predicate is recorded as a routing signal but
                    // kept in the residual WHERE so it lowers to a runtime-bound
                    // `*Column` scan filter.
                    if is_correlated_expr(expr, outer_alias) {
                        extract_non_equi_correlation(expr, outer_alias, non_equi);
                    }
                    Some(expr.clone())
                }
            }
        }

        // Non-equi predicates referencing the outer table. Recorded as a
        // routing signal and retained in the residual WHERE so the executor
        // can bind the outer operand at runtime.
        Expr::BinaryOp { .. } => {
            if is_correlated_expr(expr, outer_alias) {
                extract_non_equi_correlation(expr, outer_alias, non_equi);
            }
            Some(expr.clone())
        }

        Expr::Nested(inner) => {
            extract_correlation_recursive(inner, outer_alias, equi_keys, non_equi)
        }

        _ => Some(expr.clone()),
    }
}

/// True when the expression references the outer table by alias.
fn is_correlated_expr(expr: &Expr, outer_alias: &str) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            normalize_ident(&parts[0]).eq_ignore_ascii_case(outer_alias)
        }
        Expr::BinaryOp { left, right, .. } => {
            is_correlated_expr(left, outer_alias) || is_correlated_expr(right, outer_alias)
        }
        _ => false,
    }
}

/// Extract a non-equi correlation from a binary predicate into `non_equi`.
///
/// Records `(inner_side, outer_side)` for the predicate.
fn extract_non_equi_correlation(
    expr: &Expr,
    outer_alias: &str,
    non_equi: &mut Vec<(String, String)>,
) {
    let Expr::BinaryOp { left, right, .. } = expr else {
        return;
    };
    let lp = compound_parts(left);
    let rp = compound_parts(right);
    if let (Some((lt, lc)), Some((rt, rc))) = (lp, rp) {
        if rt.eq_ignore_ascii_case(outer_alias) {
            non_equi.push((lc, rc));
        } else if lt.eq_ignore_ascii_case(outer_alias) {
            non_equi.push((rc, lc));
        }
    }
}

/// Extract `(table_alias, column_name)` from a compound identifier.
fn compound_parts(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((normalize_ident(&parts[0]), normalize_ident(&parts[1])))
        }
        _ => None,
    }
}
