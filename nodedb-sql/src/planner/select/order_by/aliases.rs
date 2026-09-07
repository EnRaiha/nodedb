// SPDX-License-Identifier: Apache-2.0

//! ORDER BY ↔ SELECT-projection alias resolution.
//!
//! `ORDER BY <ident>` may reference a column alias declared in SELECT. The
//! search-trigger detection code only understands raw function-call AST
//! nodes, so the identifier must be substituted for its underlying
//! expression before the trigger check runs.
//!
//! The dual case — `ORDER BY rrf_score(...)` paired with
//! `SELECT rrf_score(...) AS score` — must propagate the SELECT alias as
//! the response's score column name even though the trigger detector saw
//! the literal call directly.

use sqlparser::ast;

use crate::parser::normalize::normalize_ident;

/// The output names a SELECT list introduces via explicit `AS`.
pub(crate) fn select_output_aliases(items: &[ast::SelectItem]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| match item {
            ast::SelectItem::ExprWithAlias { alias, .. } => Some(normalize_ident(alias)),
            ast::SelectItem::UnnamedExpr(_)
            | ast::SelectItem::ExprWithAliases { .. }
            | ast::SelectItem::QualifiedWildcard(..)
            | ast::SelectItem::Wildcard(_) => None,
        })
        .collect()
}

/// Resolve a possibly-aliased ORDER BY expression against the SELECT list.
///
/// Returns the expression to inspect for search-trigger detection plus the
/// caller's chosen alias for the score column (if any).
pub(super) fn resolve_order_by_target<'a>(
    expr: &'a ast::Expr,
    select_items: &'a [ast::SelectItem],
) -> (&'a ast::Expr, Option<String>) {
    if let ast::Expr::Identifier(ident) = expr {
        let needle = normalize_ident(ident);
        for item in select_items {
            if let ast::SelectItem::ExprWithAlias {
                expr: aliased_expr,
                alias,
            } = item
                && normalize_ident(alias) == needle
            {
                return (aliased_expr, Some(needle));
            }
        }
        // Identifier did not match any projection alias — fall through with
        // no alias. The caller will treat this as a non-search ORDER BY.
        return (expr, None);
    }

    // Literal function call form: surface the SELECT alias if the same
    // function name is also being projected.
    if let Some(name) = function_call_name(expr) {
        for item in select_items {
            if let ast::SelectItem::ExprWithAlias {
                expr: aliased_expr,
                alias,
            } = item
                && let Some(other) = function_call_name(aliased_expr)
                && other.eq_ignore_ascii_case(&name)
            {
                return (expr, Some(normalize_ident(alias)));
            }
        }
    }

    (expr, None)
}

/// Extract the dotted name from a function-call AST node, if any.
pub(super) fn function_call_name(expr: &ast::Expr) -> Option<String> {
    let ast::Expr::Function(func) = expr else {
        return None;
    };
    let parts: Vec<String> = func
        .name
        .0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(ident) => normalize_ident(ident),
            _ => String::new(),
        })
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}
