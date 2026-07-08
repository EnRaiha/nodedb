// SPDX-License-Identifier: Apache-2.0

//! `DROP ARRAY` statement parsing.

use crate::error::Result;
use crate::parser::array_stmt::ast::DropArrayAst;

use super::core::Parser;

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn parse_drop(&mut self) -> Result<DropArrayAst> {
        let if_exists = if self.match_kw("IF") {
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after DROP ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(DropArrayAst { name, if_exists })
    }
}
