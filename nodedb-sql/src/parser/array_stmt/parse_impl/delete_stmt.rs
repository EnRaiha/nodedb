// SPDX-License-Identifier: Apache-2.0

//! `DELETE FROM ARRAY` statement parsing.

use crate::error::Result;
use crate::parser::array_stmt::ast::DeleteArrayAst;
use crate::parser::array_stmt::lexer::Tok;

use super::core::Parser;

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn parse_delete(&mut self) -> Result<DeleteArrayAst> {
        let name = self.expect_ident()?;
        self.expect_kw("WHERE")?;
        self.expect_kw("COORDS")?;
        self.expect_kw("IN")?;
        self.expect(&Tok::LParen)?;
        let mut coords = Vec::new();
        loop {
            self.expect(&Tok::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_coord_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;
            coords.push(row);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after DELETE FROM ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(DeleteArrayAst { name, coords })
    }
}
