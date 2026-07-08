// SPDX-License-Identifier: Apache-2.0

//! `ALTER ARRAY` statement parsing.

use crate::error::Result;
use crate::parser::array_stmt::ast::AlterArrayAst;
use crate::parser::array_stmt::lexer::Tok;

use super::core::Parser;

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn parse_alter(&mut self) -> Result<AlterArrayAst> {
        let name = self.expect_ident()?;
        self.expect_kw("SET")?;
        self.expect(&Tok::LParen)?;
        let mut set = Vec::new();
        loop {
            let key = self.expect_ident()?;
            self.expect(&Tok::Eq)?;
            let value = match self.peek() {
                Some(Tok::Null) => {
                    self.i += 1;
                    None
                }
                Some(Tok::Int(_)) => {
                    let n = self.expect_int()?;
                    Some(n)
                }
                other => {
                    return Err(self.err(format!(
                        "SET {key}: expected integer or NULL, got {other:?}"
                    )));
                }
            };
            let key_lower = key.to_ascii_lowercase();
            match key_lower.as_str() {
                "audit_retain_ms" => {
                    if let Some(n) = value
                        && n < 0
                    {
                        return Err(
                            self.err(format!("SET audit_retain_ms = {n}: must be >= 0 or NULL"))
                        );
                    }
                }
                "minimum_audit_retain_ms" => match value {
                    Some(n) if n < 0 => {
                        return Err(
                            self.err(format!("SET minimum_audit_retain_ms = {n}: must be >= 0"))
                        );
                    }
                    None => {
                        return Err(self.err(
                            "SET minimum_audit_retain_ms = NULL: floor cannot be NULL".to_string(),
                        ));
                    }
                    _ => {}
                },
                other => {
                    return Err(self.err(format!(
                        "SET: unknown key `{other}`; expected `audit_retain_ms` \
                         or `minimum_audit_retain_ms`"
                    )));
                }
            }
            set.push((key_lower, value));
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after ALTER ARRAY: {:?}",
                self.peek()
            )));
        }
        if set.is_empty() {
            return Err(self.err("ALTER ARRAY SET (): at least one key is required"));
        }
        Ok(AlterArrayAst { name, set })
    }
}
