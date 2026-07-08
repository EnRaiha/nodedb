// SPDX-License-Identifier: Apache-2.0

//! `INSERT INTO ARRAY` statement parsing.
//!
//! `parse_coord_literal` is also shared with `DELETE FROM ARRAY` parsing
//! (`delete_stmt.rs`), which reuses the same coordinate-literal grammar.

use crate::error::Result;
use crate::parser::array_stmt::ast::InsertArrayAst;
use crate::parser::array_stmt::lexer::Tok;
use crate::types_array::{ArrayAttrLiteral, ArrayCoordLiteral, ArrayInsertRow};

use super::core::Parser;

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn parse_insert(&mut self) -> Result<InsertArrayAst> {
        let name = self.expect_ident()?;
        let mut rows = Vec::new();
        loop {
            self.expect_kw("COORDS")?;
            self.expect(&Tok::LParen)?;
            let mut coords = Vec::new();
            loop {
                coords.push(self.parse_coord_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;

            self.expect_kw("VALUES")?;
            self.expect(&Tok::LParen)?;
            let mut attrs = Vec::new();
            loop {
                attrs.push(self.parse_attr_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;

            rows.push(ArrayInsertRow { coords, attrs });
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after INSERT INTO ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(InsertArrayAst { name, rows })
    }

    pub(super) fn parse_coord_literal(&mut self) -> Result<ArrayCoordLiteral> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(ArrayCoordLiteral::Int64(*n)),
            Some(Tok::Float(f)) => Ok(ArrayCoordLiteral::Float64(*f)),
            Some(Tok::Str(s)) => Ok(ArrayCoordLiteral::String(s.clone())),
            other => Err(self.err(format!("expected coord literal, got {other:?}"))),
        }
    }

    fn parse_attr_literal(&mut self) -> Result<ArrayAttrLiteral> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Null) => Ok(ArrayAttrLiteral::Null),
            Some(Tok::Int(n)) => Ok(ArrayAttrLiteral::Int64(*n)),
            Some(Tok::Float(f)) => Ok(ArrayAttrLiteral::Float64(*f)),
            Some(Tok::Str(s)) => Ok(ArrayAttrLiteral::String(s.clone())),
            other => Err(self.err(format!("expected attr literal, got {other:?}"))),
        }
    }
}
