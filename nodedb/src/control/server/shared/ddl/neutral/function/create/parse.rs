// SPDX-License-Identifier: BUSL-1.1

//! Parse a `CREATE [OR REPLACE] FUNCTION` statement into a
//! typed `ParsedCreateFunction`.

use crate::control::security::catalog::{FunctionParam, FunctionVolatility};
use crate::control::server::shared::ddl::result::DdlError;

use super::super::parse::parse_function_header;

/// Parsed components of a `CREATE FUNCTION` statement.
pub struct ParsedCreateFunction {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<FunctionParam>,
    pub return_type: String,
    pub volatility: FunctionVolatility,
    pub body_sql: String,
}

/// Parse a CREATE [OR REPLACE] FUNCTION statement.
///
/// Grammar:
/// ```text
/// CREATE [OR REPLACE] FUNCTION <name>(<param_name> <type> [, ...])
///   RETURNS <type>
///   [IMMUTABLE | STABLE | VOLATILE]
///   AS <sql_expression> ;
/// ```
pub fn parse_create_function(sql: &str) -> Result<ParsedCreateFunction, DdlError> {
    // Use shared header parser — SQL functions terminate return type at AS/volatility.
    let header = parse_function_header(sql, &[" AS ", " IMMUTABLE ", " STABLE ", " VOLATILE "])?;

    let (volatility, body_part) = extract_volatility_and_body(&header.rest)?;

    let body_sql = body_part.trim().trim_end_matches(';').trim().to_string();
    if body_sql.is_empty() {
        return Err(DdlError::new("42601", "function body is empty"));
    }

    Ok(ParsedCreateFunction {
        or_replace: header.or_replace,
        name: header.name,
        parameters: header.parameters,
        return_type: header.return_type,
        volatility,
        body_sql,
    })
}

/// Extract optional volatility keyword and the body after AS.
fn extract_volatility_and_body(s: &str) -> Result<(FunctionVolatility, &str), DdlError> {
    let mut rest = s;
    let mut volatility = FunctionVolatility::Immutable; // default

    for kw in ["IMMUTABLE", "STABLE", "VOLATILE"] {
        if s.get(..kw.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(kw))
        {
            volatility = FunctionVolatility::parse(kw).unwrap_or_default();
            rest = s.get(kw.len()..).unwrap_or_default().trim();
            break;
        }
    }

    let has_as = rest
        .get(.."AS".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"));
    let has_as_separator = rest
        .as_bytes()
        .get("AS".len())
        .is_some_and(|byte| byte.is_ascii_whitespace());
    if !has_as || !has_as_separator {
        if has_as {
            return Err(DdlError::new("42601", "expected function body after AS"));
        }
        return Err(DdlError::new("42601", "expected AS <body>"));
    }
    let body = rest.get("AS".len()..).unwrap_or_default().trim();

    Ok((volatility, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_expression_function() {
        let sql =
            "CREATE FUNCTION normalize_email(email TEXT) RETURNS TEXT AS SELECT LOWER(TRIM(email))";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.name, "normalize_email");
        assert!(!parsed.or_replace);
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.parameters[0].name, "email");
        assert_eq!(parsed.parameters[0].data_type, "TEXT");
        assert_eq!(parsed.return_type, "TEXT");
        assert_eq!(parsed.body_sql, "SELECT LOWER(TRIM(email))");
        assert_eq!(parsed.volatility, FunctionVolatility::Immutable);
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1";
        let parsed = parse_create_function(sql).unwrap();
        assert!(parsed.or_replace);
        assert_eq!(parsed.name, "f");
    }

    #[test]
    fn parse_multi_param() {
        let sql = "CREATE FUNCTION add(a FLOAT, b FLOAT) RETURNS FLOAT AS SELECT a + b";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.parameters.len(), 2);
        assert_eq!(parsed.parameters[0].name, "a");
        assert_eq!(parsed.parameters[1].name, "b");
        assert_eq!(parsed.return_type, "FLOAT");
    }

    #[test]
    fn parse_no_params() {
        let sql = "CREATE FUNCTION pi() RETURNS FLOAT AS SELECT 3.14159";
        let parsed = parse_create_function(sql).unwrap();
        assert!(parsed.parameters.is_empty());
        assert_eq!(parsed.body_sql, "SELECT 3.14159");
    }

    #[test]
    fn parse_explicit_volatility() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT VOLATILE AS SELECT x";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.volatility, FunctionVolatility::Volatile);
    }

    #[test]
    fn parse_stable_volatility() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT STABLE AS SELECT x";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.volatility, FunctionVolatility::Stable);
    }

    #[test]
    fn parse_with_semicolon() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS SELECT x + 1;";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.body_sql, "SELECT x + 1");
    }

    #[test]
    fn parse_error_no_returns() {
        let sql = "CREATE FUNCTION f(x INT) AS SELECT x";
        assert!(parse_create_function(sql).is_err());
    }

    #[test]
    fn parse_error_bad_type() {
        let sql = "CREATE FUNCTION f(x FOOBAR) RETURNS INT AS SELECT x";
        assert!(parse_create_function(sql).is_err());
    }

    #[test]
    fn parse_error_empty_body() {
        let sql = "CREATE FUNCTION f(x INT) RETURNS INT AS";
        assert!(parse_create_function(sql).is_err());
    }

    #[test]
    fn parse_procedural_body() {
        let sql = "CREATE FUNCTION classify(score INT) RETURNS TEXT AS \
                    BEGIN \
                      IF score > 90 THEN RETURN 'excellent'; \
                      ELSIF score > 70 THEN RETURN 'good'; \
                      ELSE RETURN 'needs improvement'; \
                      END IF; \
                    END";
        let parsed = parse_create_function(sql).unwrap();
        assert_eq!(parsed.name, "classify");
        assert!(parsed.body_sql.starts_with("BEGIN"));

        use crate::control::planner::procedural::ast::BodyKind;
        assert!(matches!(
            BodyKind::detect(&parsed.body_sql),
            BodyKind::Procedural
        ));
        let block = crate::control::planner::procedural::parse_block(&parsed.body_sql);
        assert!(block.is_ok(), "procedural parse failed: {:?}", block.err());
    }

    #[test]
    fn parse_dml_in_procedural_body() {
        let sql = "CREATE FUNCTION bad_func(x INT) RETURNS INT AS \
                    BEGIN INSERT INTO t (id) VALUES (x); RETURN x; END";
        let parsed = parse_create_function(sql).unwrap();

        use crate::control::planner::procedural::ast::BodyKind;
        assert!(matches!(
            BodyKind::detect(&parsed.body_sql),
            BodyKind::Procedural
        ));
        let block = crate::control::planner::procedural::parse_block(&parsed.body_sql).unwrap();

        let result = crate::control::planner::procedural::validate_function_block(&block);
        assert!(result.is_err(), "should reject DML: {:?}", result);
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("side-effecting"),
            "error should reject side-effecting SQL, got: {err_msg}"
        );
    }
}
