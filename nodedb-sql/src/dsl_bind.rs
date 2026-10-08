// SPDX-License-Identifier: Apache-2.0

//! Lexer-aware parameter binding for DSL statements.
//!
//! # The architectural problem
//!
//! NodeDB's DSL statements — `UPSERT INTO`, `SEARCH`, `GRAPH`, `MATCH`,
//! `OPTIONAL MATCH`, `CRDT MERGE`, `CREATE VECTOR INDEX`,
//! `CREATE FULLTEXT INDEX`, `CREATE SEARCH INDEX`, `CREATE SPARSE INDEX`
//! — are dispatched from raw SQL text because they aren't part of
//! sqlparser's grammar. The planned-SQL path binds parameters on the
//! parsed AST, so nothing touches the DSL text: `$N` survives into the
//! dispatcher and reaches the engine as a literal string.
//!
//! The observed symptom was `cannot parse '$2' as INT` from the binary-
//! tuple encoder. That's a symptom of a class: any DSL reached through
//! the pgwire extended-query path silently skips parameter binding.
//!
//! # The fix
//!
//! A single chokepoint — `bind_dsl` — that every DSL-bound prepared
//! statement must go through before execution. It tokenizes the DSL
//! SQL with sqlparser's own tokenizer, rewrites `Token::Placeholder`
//! occurrences to concrete literal tokens, and re-serializes. This
//! respects string boundaries, quoted identifiers, and comments for
//! free — sqlparser's lexer already classified them.
//!
//! The `BoundDslSql` newtype exists so the DSL execute path takes
//! `BoundDslSql`, not `&str`. The compiler refuses to execute a DSL
//! statement whose parameters haven't been bound. That is the
//! architectural enforcement — the mechanism that made this class of
//! bug possible (a `&str` flowing straight to the dispatcher) is
//! structurally gone.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::{Result, SqlError};
use crate::params::ParamValue;

/// SQL text whose prepared-statement `$N` placeholders have been
/// substituted with concrete literals.
///
/// The DSL dispatcher's prepared-statement entry points take
/// `BoundDslSql`, not `&str`. This makes it impossible for a caller
/// to forget parameter binding on a DSL path — the types enforce it.
#[derive(Debug, Clone)]
pub struct BoundDslSql(String);

impl BoundDslSql {
    /// Construct from SQL known not to need binding (simple-query path).
    ///
    /// The simple-query protocol does not carry prepared parameters, so
    /// passing the raw text is correct at that layer. This constructor
    /// is the only way to bypass `bind_dsl` and is named loudly so that
    /// any future use is obvious in review.
    pub fn from_simple_query(sql: String) -> Self {
        Self(sql)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// Substitute `$N` placeholders in DSL SQL text with concrete literals.
///
/// Uses sqlparser's own tokenizer so that string literals, quoted
/// identifiers, comments, and dollar-quoted strings are never
/// accidentally rewritten — the tokenizer has already classified them.
pub fn bind_dsl(sql: &str, params: &[ParamValue]) -> Result<BoundDslSql> {
    if params.is_empty() {
        return Ok(BoundDslSql(sql.to_owned()));
    }
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|e| SqlError::Parse {
            detail: format!("tokenize DSL for parameter binding: {e}"),
        })?;

    let mut out = String::with_capacity(sql.len());
    for tok in &tokens {
        if let Token::Placeholder(p) = tok
            && p.starts_with('$')
        {
            // Every `$N` must resolve to a provided parameter. A
            // silent pass-through here would let an out-of-range
            // placeholder reach the engine as a raw `$N` literal —
            // exactly the bug class this module exists to close.
            let replacement =
                placeholder_literal_token(p, params).ok_or_else(|| SqlError::Parse {
                    detail: format!(
                        "DSL parameter bind: placeholder {p} has no corresponding \
                         parameter ({len} provided)",
                        len = params.len()
                    ),
                })?;
            out.push_str(&replacement.to_string());
            continue;
        }
        out.push_str(&tok.to_string());
    }
    Ok(BoundDslSql(out))
}

fn placeholder_literal_token(placeholder: &str, params: &[ParamValue]) -> Option<Token> {
    let idx_str = placeholder.strip_prefix('$')?;
    let idx: usize = idx_str.parse().ok()?;
    let param = params.get(idx.checked_sub(1)?)?;
    Some(match param {
        ParamValue::Null => Token::make_keyword("NULL"),
        ParamValue::Bool(true) => Token::make_keyword("TRUE"),
        ParamValue::Bool(false) => Token::make_keyword("FALSE"),
        ParamValue::Int64(n) => Token::Number(n.to_string(), false),
        ParamValue::Float64(f) => Token::Number(crate::params::float_literal_text(*f), false),
        ParamValue::Decimal(d) => Token::Number(d.to_string(), false),
        ParamValue::Text(s) => Token::SingleQuotedString(s.clone()),
        ParamValue::Timestamp(dt) | ParamValue::Timestamptz(dt) => {
            Token::SingleQuotedString(dt.to_iso8601())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_int_and_text_params() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("alice".into()), ParamValue::Int64(42)],
        )
        .unwrap();
        assert!(
            bound.as_str().contains("'alice'"),
            "text param not substituted: {}",
            bound.as_str()
        );
        assert!(
            bound.as_str().contains("42"),
            "int param not substituted: {}",
            bound.as_str()
        );
        assert!(
            !bound.as_str().contains('$'),
            "placeholder survived: {}",
            bound.as_str()
        );
    }

    #[test]
    fn search_top_k_param() {
        let bound = bind_dsl(
            "SEARCH v USING VECTOR(ARRAY[1.0, 0.0, 0.0], $1)",
            &[ParamValue::Int64(5)],
        )
        .unwrap();
        assert!(bound.as_str().contains(", 5)"), "got: {}", bound.as_str());
    }

    /// String literals that happen to contain `$1` must not be touched
    /// — the tokenizer has already classified the `$1` inside quotes
    /// as part of `Token::SingleQuotedString`, not as a placeholder.
    #[test]
    fn placeholder_inside_string_literal_untouched() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, note) VALUES ($1, 'your $1 change')",
            &[ParamValue::Text("abc".into())],
        )
        .unwrap();
        assert!(
            bound.as_str().contains("'your $1 change'"),
            "string literal was rewritten: {}",
            bound.as_str()
        );
        assert!(
            bound.as_str().contains("'abc'"),
            "real placeholder not bound: {}",
            bound.as_str()
        );
    }

    #[test]
    fn null_param() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("x".into()), ParamValue::Null],
        )
        .unwrap();
        let s = bound.as_str();
        assert!(s.to_uppercase().contains("NULL"), "got: {s}");
    }

    #[test]
    fn out_of_range_placeholder_errors() {
        let err = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("only-one".into())],
        )
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("$2") && msg.to_lowercase().contains("placeholder"),
            "error must name the unresolved placeholder: {msg}"
        );
    }

    #[test]
    fn zero_placeholder_errors() {
        // `$0` is not a valid pgwire parameter reference — must error,
        // not silently pass through to the engine.
        let err = bind_dsl(
            "UPSERT INTO t (id) VALUES ($0)",
            &[ParamValue::Text("x".into())],
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("$0"));
    }

    #[test]
    fn empty_params_is_noop() {
        let sql = "UPSERT INTO t (id) VALUES ('a')";
        let bound = bind_dsl(sql, &[]).unwrap();
        assert_eq!(bound.as_str(), sql);
    }
}
