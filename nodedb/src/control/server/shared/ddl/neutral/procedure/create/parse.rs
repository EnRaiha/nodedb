// SPDX-License-Identifier: BUSL-1.1

//! `CREATE [OR REPLACE] PROCEDURE` surface-grammar parser.
//!
//! Hand-rolled instead of routing through `nodedb-sql` because the
//! procedure body is a procedural-SQL block (BEGIN ... END) that the
//! main SQL parser doesn't yet tokenise as a single statement; the
//! parser here lifts the body verbatim and lets the procedural-SQL
//! parser handle it downstream.
//!
//! Ported from the pgwire `ddl::procedure::create::parse` module; only the
//! error type changed from pgwire `PgWireError` to the protocol-neutral
//! [`DdlError`].

use crate::control::security::catalog::procedure_types::{ParamDirection, ProcedureParam};

use super::super::super::super::result::DdlError;
use super::super::parens::find_matching_paren;

/// Output of [`parse_create_procedure`] — every field needed to
/// assemble a `StoredProcedure`.
pub struct ParsedCreateProcedure {
    pub or_replace: bool,
    pub name: String,
    pub parameters: Vec<ProcedureParam>,
    pub body_sql: String,
    pub max_iterations: u64,
    pub timeout_secs: u64,
}

/// Parse `CREATE [OR REPLACE] PROCEDURE <name>(<params>)
/// [WITH (...)] AS BEGIN ... END;`.
pub fn parse_create_procedure(sql: &str) -> Result<ParsedCreateProcedure, DdlError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();

    let (or_replace, rest) = if trimmed
        .get(.."CREATE OR REPLACE PROCEDURE ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE OR REPLACE PROCEDURE "))
    {
        (
            true,
            trimmed
                .get("CREATE OR REPLACE PROCEDURE ".len()..)
                .unwrap_or_default(),
        )
    } else if trimmed
        .get(.."CREATE PROCEDURE ".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CREATE PROCEDURE "))
    {
        (
            false,
            trimmed.get("CREATE PROCEDURE ".len()..).unwrap_or_default(),
        )
    } else {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected CREATE PROCEDURE".to_string(),
        });
    };

    // Find param list in parens.
    let paren_open = rest.find('(').ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "expected '(' after procedure name".to_string(),
    })?;
    let name = rest
        .get(..paren_open)
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    if name.is_empty() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "procedure name required".to_string(),
        });
    }

    let paren_close = find_matching_paren(rest, paren_open).ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "unmatched '(' in parameter list".to_string(),
    })?;
    let params_str = rest.get(paren_open + 1..paren_close).unwrap_or_default();
    let parameters = parse_procedure_params(params_str)?;

    let after_params = rest.get(paren_close + 1..).unwrap_or_default().trim();

    // Optional WITH (...) clause.
    let (max_iterations, timeout_secs, after_with) = parse_with_clause(after_params)?;

    // Expect AS then BEGIN...END body.
    let has_as = after_with
        .get(.."AS".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("AS"));
    let has_as_separator = after_with
        .as_bytes()
        .get("AS".len())
        .is_some_and(|byte| byte.is_ascii_whitespace());
    let body_start = if has_as && has_as_separator {
        after_with.get("AS".len()..).unwrap_or_default().trim()
    } else {
        after_with
    };

    let body_sql = body_start.trim().trim_end_matches(';').trim().to_string();
    if body_sql.is_empty()
        || !body_sql
            .get(.."BEGIN".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("BEGIN"))
    {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "procedure body must start with BEGIN".to_string(),
        });
    }

    Ok(ParsedCreateProcedure {
        or_replace,
        name,
        parameters,
        body_sql,
        max_iterations,
        timeout_secs,
    })
}

/// Parse a comma-separated parameter list with optional `IN`/`OUT`/`INOUT`
/// direction prefixes.
fn parse_procedure_params(params_str: &str) -> Result<Vec<ProcedureParam>, DdlError> {
    let trimmed = params_str.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    for part in trimmed.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        if tokens.is_empty() {
            continue;
        }

        // Optional direction: IN/OUT/INOUT.
        let (direction, name_idx) = match tokens[0].to_uppercase().as_str() {
            "IN" if tokens.len() >= 3 => (ParamDirection::In, 1),
            "OUT" if tokens.len() >= 3 => (ParamDirection::Out, 1),
            "INOUT" if tokens.len() >= 3 => (ParamDirection::InOut, 1),
            _ => (ParamDirection::In, 0), // default IN
        };

        if name_idx + 1 >= tokens.len() {
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: format!("parameter must have name and type: '{}'", part.trim()),
            });
        }

        let name = tokens[name_idx].to_lowercase();
        let data_type = tokens[name_idx + 1..].join(" ").to_uppercase();

        params.push(ProcedureParam {
            name,
            data_type,
            direction,
        });
    }
    Ok(params)
}

/// Parse the optional `WITH (MAX_ITERATIONS = N, TIMEOUT = N)` clause.
/// Returns `(max_iterations, timeout_secs, rest)` where `rest` is the
/// unconsumed tail of the input.
fn parse_with_clause(s: &str) -> Result<(u64, u64, &str), DdlError> {
    if !s
        .get(.."WITH".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WITH"))
    {
        return Ok((1_000_000, 60, s));
    }

    let after_with = s.get("WITH".len()..).unwrap_or_default().trim_start();
    if !after_with.starts_with('(') {
        return Ok((1_000_000, 60, s));
    }

    let close = after_with.find(')').ok_or_else(|| DdlError {
        sqlstate: "42601".to_string(),
        message: "unmatched '(' in WITH clause".to_string(),
    })?;
    let inner = after_with.get(1..close).unwrap_or_default();
    let rest = after_with.get(close + 1..).unwrap_or_default().trim();

    let mut max_iter = 1_000_000u64;
    let mut timeout = 60u64;

    for part in inner.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "MAX_ITERATIONS" => {
                max_iter = kv[1].parse().map_err(|_| DdlError {
                    sqlstate: "42601".to_string(),
                    message: "invalid MAX_ITERATIONS value".to_string(),
                })?;
            }
            "TIMEOUT" => {
                timeout = kv[1].parse().map_err(|_| DdlError {
                    sqlstate: "42601".to_string(),
                    message: "invalid TIMEOUT value".to_string(),
                })?;
            }
            _ => {}
        }
    }

    Ok((max_iter, timeout, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let sql =
            "CREATE PROCEDURE archive(cutoff INT) AS BEGIN DELETE FROM old WHERE age > cutoff; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.name, "archive");
        assert_eq!(parsed.parameters.len(), 1);
        assert_eq!(parsed.parameters[0].name, "cutoff");
        assert_eq!(parsed.parameters[0].data_type, "INT");
        assert!(parsed.body_sql.starts_with("BEGIN"));
    }

    #[test]
    fn parse_or_replace() {
        let sql = "CREATE OR REPLACE PROCEDURE p() AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert!(parsed.or_replace);
    }

    #[test]
    fn parse_with_clause() {
        let sql =
            "CREATE PROCEDURE p() WITH (MAX_ITERATIONS = 500, TIMEOUT = 30) AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.max_iterations, 500);
        assert_eq!(parsed.timeout_secs, 30);
    }

    #[test]
    fn parse_out_param() {
        let sql = "CREATE PROCEDURE p(IN x INT, OUT result TEXT) AS BEGIN RETURN; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert_eq!(parsed.parameters[0].direction, ParamDirection::In);
        assert_eq!(parsed.parameters[1].direction, ParamDirection::Out);
        assert_eq!(parsed.parameters[1].name, "result");
    }

    #[test]
    fn parse_no_params() {
        let sql = "CREATE PROCEDURE cleanup() AS BEGIN DELETE FROM temp; END";
        let parsed = parse_create_procedure(sql).unwrap();
        assert!(parsed.parameters.is_empty());
    }
}
