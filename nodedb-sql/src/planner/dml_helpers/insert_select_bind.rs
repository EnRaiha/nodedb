// SPDX-License-Identifier: Apache-2.0

//! Target-column binding for `INSERT ... SELECT`.

use sqlparser::ast;

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Prefix of the output names given to unaliased SELECT-list items of an
/// `INSERT ... SELECT` source. A user alias cannot start with it, so it
/// never shadows a name an ORDER BY or HAVING clause refers to.
const OUTPUT_NAME_PREFIX: &str = "__nodedb_insert_select_";

/// The target column each SELECT-list item writes, in projection order.
///
/// `target_columns` is the explicit `INSERT INTO t (a, b)` list, empty when
/// the statement gives none. An empty target list with a `SELECT *` source
/// copies the row unchanged and gives no names. Every name must be a
/// column of a closed-schema target.
pub(crate) fn insert_select_targets(
    target_columns: &[String],
    select: &ast::Select,
    target: &CollectionInfo,
) -> Result<Vec<String>> {
    let source_is_star = select.projection.iter().all(|item| {
        matches!(
            item,
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..)
        )
    });

    if source_is_star {
        if target_columns.is_empty() {
            return Ok(Vec::new());
        }
        return Err(SqlError::Unsupported {
            detail: "INSERT ... SELECT * cannot bind to an explicit target column list; name the \
                     source columns explicitly"
                .into(),
        });
    }

    let names = target_names(target_columns, select)?;

    if !target.open_schema {
        for name in &names {
            if !target.columns.iter().any(|c| &c.name == name) {
                return Err(SqlError::UnknownColumn {
                    table: target.name.clone(),
                    column: name.clone(),
                });
            }
        }
    }
    Ok(names)
}

/// Bind each SELECT-list item to its target column as an expression over a
/// stored source row. `names` comes from [`insert_select_targets`]. Empty
/// names give an empty map, which copies each row unchanged.
pub(crate) fn bind_insert_select_columns(
    catalog: &dyn SqlCatalog,
    functions: &crate::functions::registry::FunctionRegistry,
    temporal: crate::TemporalScope,
    names: &[String],
    select: &ast::Select,
) -> Result<Vec<(String, SqlExpr)>> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    // The source scope gates every column the SELECT list names: one the
    // source does not carry raises `UnknownColumn` here, at plan time.
    let source_scope = TableScope::resolve_from(catalog, functions, temporal, &select.from)?;
    let scope = ColumnScope::Relations(&source_scope);

    let mut bound = Vec::with_capacity(names.len());
    for (name, item) in names.iter().zip(select.projection.iter()) {
        bound.push((name.clone(), convert_expr(projection_expr(item)?, &scope)?));
    }
    Ok(bound)
}

/// Give every SELECT-list item of `query` an output name the copy can read
/// back, and pair each target column with the output name it takes.
///
/// An aliased item keeps its alias, so the clauses that refer to it resolve
/// as written. An unaliased item gets a name under [`OUTPUT_NAME_PREFIX`],
/// which no clause can refer to. Two items with one alias are refused: the
/// output row cannot hold both. `names` empty returns the query unchanged.
pub(crate) fn name_insert_select_outputs(
    query: &ast::Query,
    names: &[String],
) -> Result<(ast::Query, Vec<(String, String)>)> {
    if names.is_empty() {
        return Ok((query.clone(), Vec::new()));
    }
    let mut query = query.clone();
    let ast::SetExpr::Select(select) = query.body.as_mut() else {
        return Err(SqlError::Unsupported {
            detail: "INSERT ... SELECT with named target columns needs a plain SELECT source"
                .into(),
        });
    };
    let mut outputs = Vec::with_capacity(names.len());
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    for (index, (item, target)) in select.projection.iter_mut().zip(names).enumerate() {
        let output = match item {
            ast::SelectItem::ExprWithAlias { alias, .. } => normalize_ident(alias),
            ast::SelectItem::UnnamedExpr(expr) => {
                let output = format!("{OUTPUT_NAME_PREFIX}{index}");
                *item = ast::SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias: ast::Ident::with_quote('"', output.as_str()),
                };
                output
            }
            ast::SelectItem::ExprWithAliases { .. }
            | ast::SelectItem::Wildcard(_)
            | ast::SelectItem::QualifiedWildcard(..) => return Err(unnamed_projection_error()),
        };
        if !seen.insert(output.clone()) {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "INSERT ... SELECT list repeats the output name '{output}'; give each \
                     item its own alias"
                ),
            });
        }
        outputs.push((target.clone(), output));
    }
    Ok((query, outputs))
}

/// The target column each projection item writes, in projection order.
fn target_names(target_columns: &[String], select: &ast::Select) -> Result<Vec<String>> {
    if !target_columns.is_empty() {
        if target_columns.len() != select.projection.len() {
            return Err(SqlError::Arity {
                detail: format!(
                    "INSERT has {} target columns but the SELECT list has {} expressions",
                    target_columns.len(),
                    select.projection.len()
                ),
            });
        }
        return Ok(target_columns.to_vec());
    }
    select.projection.iter().map(output_name).collect()
}

/// The output name of a projection item: its alias, else the column it names.
fn output_name(item: &ast::SelectItem) -> Result<String> {
    match item {
        ast::SelectItem::ExprWithAlias { alias, .. } => Ok(normalize_ident(alias)),
        ast::SelectItem::UnnamedExpr(expr) => match expr {
            ast::Expr::Identifier(ident) => Ok(normalize_ident(ident)),
            ast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                Ok(normalize_ident(&parts[1]))
            }
            _ => Err(unnamed_projection_error()),
        },
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => Err(unnamed_projection_error()),
    }
}

fn unnamed_projection_error() -> SqlError {
    SqlError::Unsupported {
        detail: "INSERT ... SELECT needs a target column for every projected expression; add an \
                 alias or an explicit target column list"
            .into(),
    }
}

/// The expression a projection item evaluates per source row.
fn projection_expr(item: &ast::SelectItem) -> Result<&ast::Expr> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
            Ok(expr)
        }
        ast::SelectItem::ExprWithAliases { .. }
        | ast::SelectItem::Wildcard(_)
        | ast::SelectItem::QualifiedWildcard(..) => Err(SqlError::Unsupported {
            detail: "INSERT ... SELECT cannot mix a wildcard with named projections".into(),
        }),
    }
}
