// SPDX-License-Identifier: Apache-2.0

//! Postgres-semantic value coercion for planner use-sites.
//!
//! The planner matches `sqlparser::ast::Value` in numeric contexts
//! (LIMIT, OFFSET, fusion weights, …). When a parameter was sent over
//! the pgwire Parse message with `Type::UNKNOWN` — the default for
//! drivers that don't pre-fetch OIDs, e.g. `postgres-js` with
//! `fetch_types: false` — our bind layer emits it as
//! `Value::SingleQuotedString` (we have no type information to do
//! otherwise at bind time, and a guess-and-coerce approach would
//! silently corrupt string parameters bound into string columns).
//!
//! Postgres' model: UNKNOWN literals stay uncoerced until the planner
//! has context, and the planner then resolves them by the surrounding
//! operator / column type. These helpers are the single chokepoint
//! implementing that resolution for numeric contexts. Any future
//! numeric use-site must route through here — a raw
//! `match Value::Number` ignores UNKNOWN-coerced literals and
//! re-introduces the silent match-failure bug class.

use sqlparser::ast;

use crate::error::SqlError;

/// Resolve a `Value` into a `usize` if numeric-shaped.
///
/// Accepts:
/// - `Value::Number(n, _)` — the typed-parameter and explicit-literal path.
/// - `Value::SingleQuotedString(s)` where `s` parses as `usize` — the
///   UNKNOWN-param bind path (pgwire drivers that send `Type::UNKNOWN`).
///
/// `Value::DoubleQuotedString` is NOT accepted: with the PostgreSQL dialect
/// double-quoted tokens parse as `Expr::Identifier`, never as
/// `Expr::Value(Value::DoubleQuotedString)`, so that variant is unreachable
/// in practice and routing it here would silently accept non-numeric text.
///
/// # Bounds
///
/// Valid outputs are `[0, usize::MAX]` (64-bit on typical targets).
/// Inputs that don't fit — negative numbers, fractional values,
/// values exceeding `usize::MAX`, non-numeric text — return `None`.
/// The caller decides the semantic: a LIMIT site treats `None` as
/// "no limit applied" (pre-existing behavior); stricter sites should
/// surface a planner error.
///
/// Does not perform saturating or wrapping coercion — values that
/// overflow `usize` are rejected, not silently truncated.
pub fn as_usize_literal(value: &ast::Value) -> Option<usize> {
    match value {
        ast::Value::Number(n, _) => n.parse::<usize>().ok(),
        ast::Value::SingleQuotedString(s) => s.parse::<usize>().ok(),
        _ => None,
    }
}

/// Resolve an `Expr::Value` into a `usize` if numeric-shaped. Thin
/// wrapper that unpacks the `Expr` → `Value` layer so callers reading
/// LIMIT/OFFSET clauses don't each re-write the unpack.
pub fn expr_as_usize_literal(expr: &ast::Expr) -> Option<usize> {
    if let ast::Expr::Value(v) = expr {
        as_usize_literal(&v.value)
    } else {
        None
    }
}

/// Fallible LIMIT/OFFSET resolution.
///
/// Rejects negative literals (`LIMIT -1`, `OFFSET -2`) with
/// [`SqlError::InvalidLimitValue`] instead of collapsing to `None`
/// (unbounded). Everything else behaves exactly like
/// [`expr_as_usize_literal`].
pub fn expr_as_nonnegative_usize(
    expr: &ast::Expr,
    clause: &'static str,
) -> Result<Option<usize>, SqlError> {
    if matches!(
        expr,
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            ..
        }
    ) {
        return Err(SqlError::InvalidLimitValue {
            detail: format!("{clause} must not be negative"),
        });
    }
    Ok(expr_as_usize_literal(expr))
}

/// Resolve a `Value` into an `f64` if numeric-shaped.
///
/// Same UNKNOWN-coercion behavior as `as_usize_literal` but for
/// floating-point contexts (fusion weights, scoring thresholds,
/// confidence intervals).
///
/// # Bounds
///
/// `f64::from_str` accepts `NaN`, `inf`, subnormals, and values that
/// overflow to `±inf` via the IEEE-754 rules. Callers that need to
/// reject those (e.g. fusion weights outside `[0, 1]`) must validate
/// the returned value themselves — this helper is purely a literal
/// extractor, not a domain validator.
pub fn as_f64_literal(value: &ast::Value) -> Option<f64> {
    match value {
        ast::Value::Number(n, _) => n.parse::<f64>().ok(),
        ast::Value::SingleQuotedString(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usize_from_number() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("42".into(), false)),
            Some(42)
        );
    }

    /// Untyped pgwire param: `Type::UNKNOWN` → `ParamValue::Text` →
    /// `Value::SingleQuotedString`. LIMIT still has to work.
    #[test]
    fn usize_from_unknown_param_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("42".into())),
            Some(42)
        );
    }

    #[test]
    fn usize_rejects_non_numeric_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("abc".into())),
            None
        );
    }

    #[test]
    fn usize_rejects_negative() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("-1".into())),
            None
        );
    }

    #[test]
    fn f64_from_unknown_param_text() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("1.5".into())),
            Some(1.5)
        );
    }

    // ── bounds / overflow ──────────────────────────────────────────

    /// Values larger than `usize::MAX` are rejected, not wrapped or
    /// truncated. Silent truncation would reproduce the pattern of
    /// "untyped param drops silently" — the exact bug class this
    /// module exists to close.
    #[test]
    fn usize_rejects_overflow_number() {
        let huge = format!("{}0", usize::MAX);
        assert_eq!(as_usize_literal(&ast::Value::Number(huge, false)), None);
    }

    #[test]
    fn usize_rejects_overflow_text() {
        let huge = format!("{}0", usize::MAX);
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString(huge)),
            None
        );
    }

    #[test]
    fn usize_rejects_fractional_number() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("1.5".into(), false)),
            None
        );
    }

    #[test]
    fn usize_rejects_fractional_text() {
        assert_eq!(
            as_usize_literal(&ast::Value::SingleQuotedString("1.5".into())),
            None
        );
    }

    #[test]
    fn usize_rejects_scientific_notation() {
        // `1e3` is not a usize literal — Postgres treats it as a float.
        assert_eq!(
            as_usize_literal(&ast::Value::Number("1e3".into(), false)),
            None
        );
    }

    #[test]
    fn usize_accepts_zero() {
        assert_eq!(
            as_usize_literal(&ast::Value::Number("0".into(), false)),
            Some(0)
        );
    }

    #[test]
    fn usize_accepts_max() {
        let max_str = usize::MAX.to_string();
        assert_eq!(
            as_usize_literal(&ast::Value::Number(max_str, false)),
            Some(usize::MAX)
        );
    }

    #[test]
    fn f64_accepts_negative() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("-1.5".into())),
            Some(-1.5)
        );
    }

    #[test]
    fn f64_overflow_produces_infinity() {
        // IEEE-754 semantics — documented contract. Callers that
        // can't tolerate `inf` must validate.
        let out = as_f64_literal(&ast::Value::Number("1e400".into(), false));
        assert!(matches!(out, Some(f) if f.is_infinite()));
    }

    #[test]
    fn f64_rejects_non_numeric_text() {
        assert_eq!(
            as_f64_literal(&ast::Value::SingleQuotedString("foo".into())),
            None
        );
    }

    #[test]
    fn s1_negative_limit_rejected_not_unbounded() {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            "SELECT * FROM t LIMIT -1",
        )
        .unwrap();
        let ast::Statement::Query(q) = &stmts[0] else {
            panic!("expected query statement");
        };
        let Some(ast::LimitClause::LimitOffset { limit: Some(expr), .. }) =
            q.limit_clause.as_ref()
        else {
            panic!("expected LIMIT clause");
        };
        assert!(
            matches!(
                expr_as_nonnegative_usize(expr, "LIMIT"),
                Err(SqlError::InvalidLimitValue { .. })
            ),
            "S1: negative LIMIT must error"
        );
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::GenericDialect {},
            "SELECT * FROM t LIMIT 10",
        )
        .unwrap();
        let ast::Statement::Query(q) = &stmts[0] else {
            panic!("expected query statement");
        };
        let Some(ast::LimitClause::LimitOffset { limit: Some(expr), .. }) =
            q.limit_clause.as_ref()
        else {
            panic!("expected LIMIT clause");
        };
        assert_eq!(expr_as_nonnegative_usize(expr, "LIMIT").unwrap(), Some(10));
    }
}