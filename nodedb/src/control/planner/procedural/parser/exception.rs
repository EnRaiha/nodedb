// SPDX-License-Identifier: BUSL-1.1

//! EXCEPTION handler parsing for procedural SQL blocks.

use super::super::ast::*;
use super::super::error::ProceduralError;
use super::super::tokenizer::{Token, TokenStream};
use super::statements::parse_statement;

/// Parse `EXCEPTION WHEN <condition> THEN <statements> [WHEN ...] ...`
///
/// Called when the parser encounters Token::Exception inside a BEGIN block.
/// Parses one or more WHEN handlers until END is reached.
pub(super) fn parse_exception_handlers(
    tokens: &TokenStream<'_>,
    pos: &mut usize,
) -> Result<Vec<ExceptionHandler>, ProceduralError> {
    *pos += 1; // skip EXCEPTION

    let mut handlers = Vec::new();

    while *pos < tokens.len() {
        if !matches!(tokens.get(*pos), Some(Token::Ident(w)) if w.to_uppercase() == "WHEN") {
            break;
        }
        *pos += 1; // skip WHEN

        let condition = parse_exception_condition(tokens, pos)?;

        if !matches!(tokens.get(*pos), Some(Token::Then)) {
            return Err(ProceduralError::parse(
                "expected THEN after exception condition",
            ));
        }
        *pos += 1;

        let body = parse_exception_body(tokens, pos)?;
        handlers.push(ExceptionHandler { condition, body });
    }

    if handlers.is_empty() {
        return Err(ProceduralError::parse(
            "EXCEPTION block requires at least one WHEN handler",
        ));
    }

    Ok(handlers)
}

fn parse_exception_condition(
    tokens: &[Token],
    pos: &mut usize,
) -> Result<ExceptionCondition, ProceduralError> {
    match tokens.get(*pos) {
        Some(Token::Ident(w)) => {
            let upper = w.to_uppercase();
            *pos += 1;
            match upper.as_str() {
                "OTHERS" => Ok(ExceptionCondition::Others),
                "SQLSTATE" => match tokens.get(*pos) {
                    Some(Token::StringLit(code)) => {
                        let code = code.clone();
                        *pos += 1;
                        Ok(ExceptionCondition::SqlState(code))
                    }
                    _ => Err(ProceduralError::parse(
                        "expected SQLSTATE code string after SQLSTATE",
                    )),
                },
                _ => Ok(ExceptionCondition::Named(upper)),
            }
        }
        _ => Err(ProceduralError::parse(
            "expected exception condition (OTHERS, SQLSTATE, or named condition)",
        )),
    }
}

fn parse_exception_body(
    tokens: &TokenStream<'_>,
    pos: &mut usize,
) -> Result<Vec<Statement>, ProceduralError> {
    let mut stmts = Vec::new();
    while *pos < tokens.len() {
        // A comment between statements belongs to none of them.
        if tokens[*pos].is_comment() {
            *pos += 1;
            continue;
        }
        match tokens.get(*pos) {
            Some(Token::End) => break,
            Some(Token::Ident(w)) if w.to_uppercase() == "WHEN" => break,
            _ => {}
        }
        stmts.push(parse_statement(tokens, pos)?);
    }
    Ok(stmts)
}
