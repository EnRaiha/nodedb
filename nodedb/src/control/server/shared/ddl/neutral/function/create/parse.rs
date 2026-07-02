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
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "function body is empty".to_string(),
        });
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
    let upper = s.to_uppercase();
    let mut rest = s;
    let mut volatility = FunctionVolatility::Immutable; // default

    for kw in ["IMMUTABLE", "STABLE", "VOLATILE"] {
        if upper.starts_with(kw) {
            volatility = FunctionVolatility::parse(kw).unwrap_or_default();
            rest = s[kw.len()..].trim();
            break;
        }
    }

    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("AS ") && !rest_upper.starts_with("AS\n") {
        if rest_upper.starts_with("AS") {
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "expected function body after AS".to_string(),
            });
        }
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected AS <body>".to_string(),
        });
    }
    let body = rest["AS".len()..].trim();

    Ok((volatility, body))
}
