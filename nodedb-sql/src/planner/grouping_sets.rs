// SPDX-License-Identifier: Apache-2.0

//! ROLLUP / CUBE / GROUPING SETS expansion and canonical key extraction.
//!
//! The public entry point is `expand_group_by`, which inspects the raw AST
//! GROUP BY clause and returns:
//!
//! - `canonical_keys`: the full deduplicated column list (indices 0..N).
//! - `grouping_sets`: one entry per set; each entry is the subset of
//!   canonical-key indices that are *active* (non-NULL) in that set.
//!
//! `GROUPING(col)` at query time resolves `col` to its canonical-key index and
//! checks whether that bit is absent from the row's active set.

use sqlparser::ast::{self, GroupByExpr};

use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_ident;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::SqlExpr;

/// Result of expanding a GROUP BY clause that contains ROLLUP/CUBE/GROUPING SETS.
#[derive(Debug, Clone)]
pub struct GroupingSetsExpansion {
    /// All distinct group-key expressions in canonical order.
    pub canonical_keys: Vec<SqlExpr>,
    /// One entry per logical grouping set; each entry is the indices into
    /// `canonical_keys` that are *present* (non-NULL) for rows in that set.
    pub grouping_sets: Vec<Vec<usize>>,
}

/// Expand the GROUP BY clause if it contains ROLLUP/CUBE/GROUPING SETS.
///
/// Returns `None` when the GROUP BY is a plain expression list with no
/// extensions — callers fall back to the existing single-set path.
pub fn expand_group_by(
    group_by: &GroupByExpr,
    scope: &TableScope,
) -> Result<Option<GroupingSetsExpansion>> {
    let scope = ColumnScope::Relations(scope);
    let exprs = match group_by {
        GroupByExpr::All(_) => return Ok(None),
        GroupByExpr::Expressions(exprs, _) => exprs,
    };

    // Check whether any expression is ROLLUP / CUBE / GROUPING SETS.
    let has_extension = exprs.iter().any(is_grouping_extension);
    if !has_extension {
        return Ok(None);
    }

    // Split into plain columns and the single extension expression.
    // SQL standard: only one extension per GROUP BY; mixed is allowed but
    // forms a cross-product with the plain columns.
    let mut plain_ast: Vec<&ast::Expr> = Vec::new();
    let mut extension_sets: Option<Vec<Vec<&ast::Expr>>> = None;

    for expr in exprs {
        match expr {
            ast::Expr::Rollup(groups) => {
                if extension_sets.is_some() {
                    return Err(SqlError::Unsupported {
                        detail: "only one ROLLUP/CUBE/GROUPING SETS per GROUP BY is supported"
                            .into(),
                    });
                }
                extension_sets = Some(expand_rollup(groups));
            }
            ast::Expr::Cube(groups) => {
                if extension_sets.is_some() {
                    return Err(SqlError::Unsupported {
                        detail: "only one ROLLUP/CUBE/GROUPING SETS per GROUP BY is supported"
                            .into(),
                    });
                }
                extension_sets = Some(expand_cube(groups));
            }
            ast::Expr::GroupingSets(sets) => {
                if extension_sets.is_some() {
                    return Err(SqlError::Unsupported {
                        detail: "only one ROLLUP/CUBE/GROUPING SETS per GROUP BY is supported"
                            .into(),
                    });
                }
                // GroupingSets: each inner Vec<Expr> is one set.
                extension_sets = Some(sets.iter().map(|s| s.iter().collect()).collect());
            }
            other => {
                plain_ast.push(other);
            }
        }
    }

    let ext_sets = extension_sets.unwrap_or_default();

    // Build canonical key list: plain columns first, then extension columns
    // (deduped by display name so identical columns share an index).
    let mut canonical_names: Vec<String> = Vec::new();
    let mut canonical_exprs: Vec<SqlExpr> = Vec::new();

    let mut intern = |e: &ast::Expr| -> Result<usize> {
        let display = format!("{e}");
        if let Some(pos) = canonical_names.iter().position(|n| n == &display) {
            return Ok(pos);
        }
        let idx = canonical_names.len();
        canonical_names.push(display);
        canonical_exprs.push(convert_expr(e, &scope)?);
        Ok(idx)
    };

    // Plain columns get canonical indices first.
    let plain_indices: Vec<usize> = plain_ast.iter().map(|e| intern(e)).collect::<Result<_>>()?;

    // Extension sets: each set is a list of ast::Expr refs → indices.
    let ext_sets_indexed: Vec<Vec<usize>> = ext_sets
        .into_iter()
        .map(|set| set.into_iter().map(&mut intern).collect::<Result<_>>())
        .collect::<Result<_>>()?;

    // Cross-product: plain_indices × ext_sets_indexed.
    // For each extension set, prepend the plain indices.
    let grouping_sets: Vec<Vec<usize>> = if ext_sets_indexed.is_empty() {
        // Only plain columns — this shouldn't happen (caught by has_extension),
        // but handle gracefully.
        vec![plain_indices]
    } else {
        ext_sets_indexed
            .into_iter()
            .map(|ext_set| {
                // plain_indices always present; ext_set columns also present.
                let mut combined = plain_indices.clone();
                for idx in &ext_set {
                    if !combined.contains(idx) {
                        combined.push(*idx);
                    }
                }
                combined
            })
            .collect()
    };

    Ok(Some(GroupingSetsExpansion {
        canonical_keys: canonical_exprs,
        grouping_sets,
    }))
}

/// Returns true if the expression is a ROLLUP/CUBE/GROUPING SETS node.
fn is_grouping_extension(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Rollup(_) | ast::Expr::Cube(_) | ast::Expr::GroupingSets(_)
    )
}

/// Expand `ROLLUP(a, b, c)` → `[[a,b,c], [a,b], [a], []]`.
///
/// The input is `Vec<Vec<Expr>>` where each inner vec is one composite element.
/// We flatten composite elements to individual expressions for simplicity — the
/// outer product is: suffix-strip from all-present down to empty.
fn expand_rollup(groups: &[Vec<ast::Expr>]) -> Vec<Vec<&ast::Expr>> {
    // Flatten composite groups (e.g. `(a, b)` as one element) into atoms.
    let atoms: Vec<&ast::Expr> = groups.iter().flat_map(|g| g.iter()).collect();
    let n = atoms.len();
    // Prefixes: atoms[0..n], atoms[0..n-1], ..., atoms[0..0] (empty).
    (0..=n).rev().map(|len| atoms[..len].to_vec()).collect()
}

/// Expand `CUBE(a, b)` → all 2^N subsets.
fn expand_cube(groups: &[Vec<ast::Expr>]) -> Vec<Vec<&ast::Expr>> {
    let atoms: Vec<&ast::Expr> = groups.iter().flat_map(|g| g.iter()).collect();
    let n = atoms.len();
    let count = 1usize << n;
    let mut sets: Vec<Vec<&ast::Expr>> = Vec::with_capacity(count);
    // Enumerate all bitmasks from (all-present) down to 0 (empty).
    for mask in (0..count).rev() {
        let set: Vec<&ast::Expr> = (0..n)
            .filter(|i| (mask >> i) & 1 == 1)
            .map(|i| atoms[i])
            .collect();
        sets.push(set);
    }
    sets
}

/// Resolve the canonical index for a `GROUPING(col)` argument.
///
/// Matches by display string against `canonical_names`.
pub fn resolve_grouping_col(col_expr: &ast::Expr, canonical_keys: &[SqlExpr]) -> Result<usize> {
    let display = format!("{col_expr}");
    // Find by rebuilding the display of each canonical key via its SqlExpr.
    // Since canonical_keys are built from the same AST exprs, we can compare
    // their SqlExpr display strings.
    for (i, key) in canonical_keys.iter().enumerate() {
        if format!("{key:?}").contains(&display) {
            return Ok(i);
        }
    }
    // Fallback: try normalized ident match.
    if let ast::Expr::Identifier(ident) = col_expr {
        let name = normalize_ident(ident);
        for (i, key) in canonical_keys.iter().enumerate() {
            if let SqlExpr::Column { name: col_name, .. } = key
                && col_name.eq_ignore_ascii_case(&name)
            {
                return Ok(i);
            }
        }
    }
    Err(SqlError::Unsupported {
        detail: format!(
            "GROUPING({col_expr}) references a column not found in the canonical key list"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolver::columns::test_support::open_scope;

    fn parse_group_by(sql: &str) -> GroupByExpr {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            ast::Statement::Query(q) => match *q.body {
                ast::SetExpr::Select(s) => s.group_by,
                _ => panic!("expected SELECT"),
            },
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn rollup_two_cols() {
        let gb = parse_group_by(
            "SELECT region, country, SUM(sales) FROM orders GROUP BY ROLLUP (region, country)",
        );
        let result = expand_group_by(&gb, &open_scope("orders"))
            .unwrap()
            .unwrap();
        // ROLLUP(region, country) → [[0,1], [0], []]
        assert_eq!(result.canonical_keys.len(), 2);
        assert_eq!(result.grouping_sets.len(), 3);
        assert_eq!(result.grouping_sets[0], vec![0, 1]); // (region, country)
        assert_eq!(result.grouping_sets[1], vec![0]); // (region)
        assert_eq!(result.grouping_sets[2], Vec::<usize>::new()); // ()
    }

    #[test]
    fn cube_two_cols() {
        let gb = parse_group_by(
            "SELECT region, country, SUM(sales) FROM orders GROUP BY CUBE (region, country)",
        );
        let result = expand_group_by(&gb, &open_scope("orders"))
            .unwrap()
            .unwrap();
        // CUBE(region, country) → [[0,1], [0], [1], []]
        assert_eq!(result.canonical_keys.len(), 2);
        assert_eq!(result.grouping_sets.len(), 4);
        // All-present first.
        assert!(result.grouping_sets[0].contains(&0));
        assert!(result.grouping_sets[0].contains(&1));
        // Empty set last.
        assert_eq!(*result.grouping_sets.last().unwrap(), Vec::<usize>::new());
    }

    #[test]
    fn grouping_sets_explicit() {
        let gb = parse_group_by(
            "SELECT region, country, SUM(sales) FROM orders \
             GROUP BY GROUPING SETS ((region, country), (region), ())",
        );
        let result = expand_group_by(&gb, &open_scope("orders"))
            .unwrap()
            .unwrap();
        assert_eq!(result.canonical_keys.len(), 2);
        assert_eq!(result.grouping_sets.len(), 3);
        assert_eq!(result.grouping_sets[0], vec![0, 1]);
        assert_eq!(result.grouping_sets[1], vec![0]);
        assert_eq!(result.grouping_sets[2], Vec::<usize>::new());
    }

    #[test]
    fn plain_group_by_returns_none() {
        let gb = parse_group_by("SELECT region, COUNT(*) FROM orders GROUP BY region");
        let result = expand_group_by(&gb, &open_scope("orders")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn mixed_plain_and_rollup() {
        let gb = parse_group_by("SELECT a, b, c, SUM(x) FROM t GROUP BY a, ROLLUP (b, c)");
        let result = expand_group_by(&gb, &open_scope("orders"))
            .unwrap()
            .unwrap();
        // Canonical: a(0), b(1), c(2).
        // Extension sets (from ROLLUP(b,c)): [[b,c], [b], []].
        // Cross-product with plain [a]:
        //   set 0: [a, b, c] = [0,1,2]
        //   set 1: [a, b]    = [0,1]
        //   set 2: [a]       = [0]
        assert_eq!(result.canonical_keys.len(), 3);
        assert_eq!(result.grouping_sets.len(), 3);
        assert!(result.grouping_sets[0].contains(&0)); // a always present
        assert!(result.grouping_sets[1].contains(&0));
        assert!(result.grouping_sets[2].contains(&0));
    }

    #[test]
    fn rollup_three_cols() {
        let gb = parse_group_by("SELECT a, b, c, SUM(x) FROM t GROUP BY ROLLUP (a, b, c)");
        let result = expand_group_by(&gb, &open_scope("orders"))
            .unwrap()
            .unwrap();
        assert_eq!(result.grouping_sets.len(), 4); // (a,b,c),(a,b),(a),()
    }
}
