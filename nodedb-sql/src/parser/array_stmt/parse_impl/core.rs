// SPDX-License-Identifier: Apache-2.0

//! `Parser` struct definition and the low-level token-stream primitives
//! (peek/bump/expect) that every statement-specific parse method builds on.

use crate::error::{Result, SqlError};
use crate::parser::array_stmt::lexer::{Tok, Token};

pub(in crate::parser::array_stmt::parse) struct Parser<'a> {
    pub(super) toks: &'a [Token],
    pub(super) i: usize,
}

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn new(toks: &'a [Token]) -> Self {
        Self { toks, i: 0 }
    }

    pub(super) fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i).map(|t| &t.tok)
    }

    pub(super) fn bump(&mut self) -> Option<&'a Token> {
        let t = self.toks.get(self.i)?;
        self.i += 1;
        Some(t)
    }

    pub(super) fn at_end(&self) -> bool {
        self.i >= self.toks.len()
    }

    pub(super) fn err(&self, msg: impl Into<String>) -> SqlError {
        SqlError::Parse { detail: msg.into() }
    }

    pub(in crate::parser::array_stmt::parse) fn expect_kw(&mut self, kw: &str) -> Result<()> {
        match self.peek() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => {
                self.i += 1;
                Ok(())
            }
            other => Err(self.err(format!("expected keyword `{kw}`, got {other:?}"))),
        }
    }

    pub(super) fn match_kw(&mut self, kw: &str) -> bool {
        match self.peek() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    pub(super) fn expect_ident(&mut self) -> Result<String> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Ident(s)) => Ok(s.clone()),
            other => Err(self.err(format!("expected identifier, got {other:?}"))),
        }
    }

    pub(super) fn expect(&mut self, want: &Tok) -> Result<()> {
        if self.peek() == Some(want) {
            self.i += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected {want:?}, got {:?}", self.peek())))
        }
    }

    pub(super) fn match_token(&mut self, want: &Tok) -> bool {
        if self.peek() == Some(want) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    pub(super) fn expect_int(&mut self) -> Result<i64> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(*n),
            other => Err(self.err(format!("expected integer, got {other:?}"))),
        }
    }

    pub(super) fn expect_float_or_int_as_f64(&mut self) -> Result<f64> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(*n as f64),
            Some(Tok::Float(f)) => Ok(*f),
            other => Err(self.err(format!("expected number, got {other:?}"))),
        }
    }
}
