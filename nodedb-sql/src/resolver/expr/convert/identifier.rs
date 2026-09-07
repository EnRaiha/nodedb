// SPDX-License-Identifier: Apache-2.0

//! Column references and bare-identifier conversion.

use sqlparser::ast::Ident;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{SCHEMA_QUALIFIED_MSG, normalize_ident};
use crate::resolver::ColumnScope;
use crate::types::*;

/// SQL-standard niladic functions: written without parentheses. Parsers
/// emit them as bare identifiers; we promote them to function calls so
/// they fold to a value at plan time instead of resolving to a column.
fn is_zero_arg_keyword_function(name: &str) -> bool {
    matches!(
        name,
        "current_timestamp"
            | "current_date"
            | "current_time"
            | "localtime"
            | "localtimestamp"
            | "current_user"
            | "current_role"
            | "current_schema"
            | "session_user"
            | "user"
            | "version"
    )
}

pub(super) fn convert_identifier(ident: &Ident, scope: &ColumnScope<'_>) -> Result<SqlExpr> {
    let name = normalize_ident(ident);
    // SQL-standard zero-arg keyword functions parse as bare
    // identifiers (no parentheses): `SELECT current_timestamp`,
    // `SELECT current_user`, etc. Promote them to function calls
    // so const folding evaluates them like the parenthesised form.
    if is_zero_arg_keyword_function(&name) {
        return Ok(SqlExpr::Function {
            name,
            args: vec![],
            distinct: false,
        });
    }
    scope.check_column(None, &name)?;
    Ok(SqlExpr::Column { table: None, name })
}

pub(super) fn convert_compound_identifier(
    parts: &[Ident],
    scope: &ColumnScope<'_>,
) -> Result<SqlExpr> {
    if parts.len() >= 3 {
        let qualified: String = parts
            .iter()
            .map(normalize_ident)
            .collect::<Vec<_>>()
            .join(".");
        return Err(SqlError::Unsupported {
            detail: format!(
                "schema-qualified column reference '{qualified}': {SCHEMA_QUALIFIED_MSG}"
            ),
        });
    }
    let table = normalize_ident(&parts[0]);
    let name = normalize_ident(&parts[1]);
    scope.check_column(Some(&table), &name)?;
    Ok(SqlExpr::Column {
        table: Some(table),
        name,
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Expr;

    use crate::error::SqlError;
    use crate::resolver::ColumnScope;
    use crate::resolver::expr::convert::convert_expr;
    use crate::resolver::expr::convert::entry::tests::first_select_expr;
    use crate::types::*;

    #[test]
    fn compound_identifier_two_parts_is_column() {
        let expr = first_select_expr("SELECT t.col FROM t");
        let result = convert_expr(&expr, &ColumnScope::Unchecked).expect("should succeed");
        match result {
            SqlExpr::Column {
                table: Some(t),
                name,
            } => {
                assert_eq!(t, "t");
                assert_eq!(name, "col");
            }
            other => panic!("expected Column with table, got {other:?}"),
        }
    }

    #[test]
    fn compound_identifier_three_parts_rejected() {
        // schema.table.col — should be rejected.
        use sqlparser::ast::Ident;
        let parts = vec![Ident::new("schema"), Ident::new("table"), Ident::new("col")];
        let expr = Expr::CompoundIdentifier(parts);
        let err = convert_expr(&expr, &ColumnScope::Unchecked).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("schema.table.col") || msg.contains("schema-qualified"),
            "error should mention the qualified name: {msg}"
        );
    }

    #[test]
    fn compound_identifier_four_parts_rejected() {
        use sqlparser::ast::Ident;
        let parts = vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ];
        let expr = Expr::CompoundIdentifier(parts);
        let err = convert_expr(&expr, &ColumnScope::Unchecked).unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected Unsupported, got {err:?}"
        );
    }

    /// `"userId"` with the PostgreSQL dialect is an identifier (quoted,
    /// case-preserved), not a string literal.
    #[test]
    fn double_quoted_is_identifier_not_literal() {
        let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
        match expr {
            Expr::Identifier(ident) => {
                assert_eq!(ident.value, "userId");
                assert_eq!(ident.quote_style, Some('"'));
            }
            other => panic!("expected Identifier, got {other:?}"),
        }
    }

    /// A double-quoted identifier in the SELECT list resolves as `SqlExpr::Column`
    /// with the exact case preserved (not lowercased, because it was quoted).
    #[test]
    fn double_quoted_select_col_case_preserved() {
        let expr = first_select_expr(r#"SELECT "userId" FROM users"#);
        let sql_expr =
            convert_expr(&expr, &ColumnScope::Unchecked).expect("convert_expr should succeed");
        match sql_expr {
            SqlExpr::Column { name, table } => {
                assert_eq!(
                    name, "userId",
                    "case must be preserved for quoted identifier"
                );
                assert_eq!(table, None, "no table qualifier expected");
            }
            other => panic!("expected Column, got {other:?}"),
        }
    }
}
