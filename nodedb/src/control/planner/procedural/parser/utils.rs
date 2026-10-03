// SPDX-License-Identifier: BUSL-1.1

//! Token utility functions for the procedural SQL parser.

use super::super::error::ProceduralError;
use super::super::tokenizer::{Token, TokenStream};

/// Check if a token matches a pattern token (ignoring content for parameterized variants).
pub(super) fn token_matches(token: &Token, pattern: &Token) -> bool {
    std::mem::discriminant(token) == std::mem::discriminant(pattern)
}

pub(super) fn skip_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
) -> Result<(), ProceduralError> {
    expect_token(tokens, pos, expected)
}

pub(super) fn expect_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
) -> Result<(), ProceduralError> {
    if *pos < tokens.len() && token_matches(&tokens[*pos], expected) {
        *pos += 1;
        Ok(())
    } else {
        Err(ProceduralError::parse(format!(
            "expected {expected:?} at position {pos}, got {:?}",
            tokens.get(*pos)
        )))
    }
}

pub(super) fn expect_ident(tokens: &[Token], pos: &mut usize) -> Result<String, ProceduralError> {
    match tokens.get(*pos) {
        Some(Token::Ident(s)) => {
            let name = s.clone();
            *pos += 1;
            Ok(name)
        }
        other => Err(ProceduralError::parse(format!(
            "expected identifier at position {pos}, got {other:?}"
        ))),
    }
}

pub(super) fn skip_if(tokens: &[Token], pos: &mut usize, token: &Token) {
    if *pos < tokens.len() && token_matches(&tokens[*pos], token) {
        *pos += 1;
    }
}

/// Collect tokens as a SQL expression until one of the terminator tokens is found.
pub(super) fn collect_sql_until(
    tokens: &TokenStream<'_>,
    pos: &mut usize,
    terminators: &[Token],
) -> Result<super::super::ast::SqlExpr, ProceduralError> {
    let sql = collect_raw_sql_until(tokens, pos, terminators);
    if sql.is_empty() {
        return Err(ProceduralError::parse(format!(
            "expected SQL expression before {:?} at position {pos}",
            terminators
        )));
    }
    Ok(super::super::ast::SqlExpr::new(sql))
}

/// Collect tokens until a terminator is found, and return the source text
/// they cover, spelled as written.
///
/// The text ends at the last token that is not a comment. A trailing line
/// comment will otherwise swallow the `;` a caller appends to the text.
pub(super) fn collect_raw_sql_until(
    tokens: &TokenStream<'_>,
    pos: &mut usize,
    terminators: &[Token],
) -> String {
    let first = *pos;
    let mut last_code: Option<usize> = None;
    while *pos < tokens.len() {
        let token = &tokens[*pos];
        if terminators.iter().any(|t| token_matches(token, t)) {
            break;
        }
        if !token.is_comment() {
            last_code = Some(*pos);
        }
        *pos += 1;
    }
    last_code
        .and_then(|last| tokens.source_of(first, last))
        .map(|sql| sql.trim().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::super::super::tokenizer::tokenize;
    use super::*;

    #[test]
    fn collected_sql_keeps_its_source_spelling() {
        let tokens = tokenize(
            "INSERT INTO t (id, v) VALUES ('it''s', 1.5e3::float) WHERE a>=1 AND b||c <> d;",
        )
        .expect("tokenize");
        let mut pos = 0;
        assert_eq!(
            collect_raw_sql_until(&tokens, &mut pos, &[Token::Semicolon]),
            "INSERT INTO t (id, v) VALUES ('it''s', 1.5e3::float) WHERE a>=1 AND b||c <> d"
        );
        assert_eq!(tokens.get(pos), Some(&Token::Semicolon));
    }

    #[test]
    fn collected_sql_ends_at_its_last_code_token() {
        let tokens =
            tokenize("DELETE FROM t /* keep */ WHERE id = 1 -- trailing\n;").expect("tokenize");
        let mut pos = 0;
        assert_eq!(
            collect_raw_sql_until(&tokens, &mut pos, &[Token::Semicolon]),
            "DELETE FROM t /* keep */ WHERE id = 1"
        );
    }
}
