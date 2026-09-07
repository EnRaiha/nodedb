// SPDX-License-Identifier: Apache-2.0

//! Scalar subqueries: `col > (SELECT AGG(...) FROM tbl ...)`.

use sqlparser::ast::{self, Expr, SetExpr};

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::normalize_ident;
use crate::types::*;

use super::extract::SubqueryJoin;

fn canonical_aggregate_key(function: &str, field: &str) -> String {
    format!("{function}({field})")
}

/// Result of planning a scalar subquery.
pub(super) struct ScalarSubqueryResult {
    pub(super) join: SubqueryJoin,
    pub(super) replacement_expr: Expr,
}

/// Plan a scalar subquery (e.g., `(SELECT AVG(amount) FROM orders)`).
///
/// Rewrites `col > (SELECT AVG(amount) FROM orders)` as:
///   cross-join with the aggregate result (1 row), then filter `col > result_col`.
///
/// The cross-join produces a cartesian product, but since the aggregate returns
/// exactly 1 row, every outer row gets paired with that single result row.
pub(super) fn try_plan_scalar_subquery(
    subquery: &ast::Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: crate::TemporalScope,
) -> Result<Option<ScalarSubqueryResult>> {
    let inner_plan = crate::planner::select::plan_query(subquery, catalog, functions, temporal)?;

    // Extract the result column name from the subquery's SELECT list.
    let result_col = match extract_scalar_column(subquery) {
        Some(col) => col,
        None => return Ok(None),
    };

    let replacement = Expr::Identifier(ast::Ident::new(&result_col));

    Ok(Some(ScalarSubqueryResult {
        join: SubqueryJoin {
            // A cross join pairs every row with every row, so it has no key.
            on: Vec::new(),
            inner_plan,
            join_type: JoinType::Cross,
        },
        replacement_expr: replacement,
    }))
}

/// Extract the projected column name from a scalar subquery.
///
/// Handles aliased aggregates like `SELECT AVG(amount) AS avg_amount`.
/// For unaliased aggregates, returns the canonical aggregate key emitted by
/// the aggregate executor (e.g. `avg(amount)`, `count(*)`).
fn extract_scalar_column(query: &ast::Query) -> Option<String> {
    let select = match &*query.body {
        SetExpr::Select(s) => s,
        _ => return None,
    };
    if select.projection.len() != 1 {
        return None;
    }
    match &select.projection[0] {
        ast::SelectItem::ExprWithAlias { alias, .. } => Some(normalize_ident(alias)),
        ast::SelectItem::UnnamedExpr(expr) => match expr {
            Expr::Identifier(ident) => Some(normalize_ident(ident)),
            Expr::CompoundIdentifier(parts) if parts.len() >= 3 => {
                // Schema-qualified: return None to propagate "unsupported" through convert_expr.
                None
            }
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some(normalize_ident(&parts[1])),
            Expr::Function(func) => {
                let func_name = func
                    .name
                    .0
                    .iter()
                    .map(|p| match p {
                        ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
                    .join(".")
                    .to_lowercase();
                let arg = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .first()
                        .and_then(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::Identifier(ident),
                            )) => Some(normalize_ident(ident)),
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::CompoundIdentifier(parts),
                            )) if parts.len() >= 3 => None,
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                Expr::CompoundIdentifier(parts),
                            )) if parts.len() == 2 => Some(normalize_ident(&parts[1])),
                            ast::FunctionArg::Unnamed(
                                ast::FunctionArgExpr::Wildcard
                                | ast::FunctionArgExpr::QualifiedWildcard(_),
                            ) => Some("all".to_string()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "*".to_string()),
                    _ => "*".to_string(),
                };
                Some(canonical_aggregate_key(&func_name, &arg))
            }
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::extract_scalar_column;
    use crate::parser::statement::parse_sql;
    use sqlparser::ast::Statement;

    #[test]
    fn unaliased_scalar_aggregate_uses_canonical_aggregate_key() {
        let statements = parse_sql("SELECT AVG(amount) FROM orders").unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query");
        };
        assert_eq!(extract_scalar_column(query), Some("avg(amount)".into()));
    }
}
