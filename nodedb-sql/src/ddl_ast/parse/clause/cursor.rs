// SPDX-License-Identifier: Apache-2.0

//! Left-to-right clause reader for the DDL parsers.
//!
//! A parser reads its statement in source order. A keyword matches only at
//! the cursor position, never anywhere in the text, and a value is read from
//! the tokens right after its keyword. `finish` refuses any token the parser
//! did not consume, so a misspelled or misplaced clause is a parse error and
//! never falls back to a default.

use super::token::{Tok, Token};
use crate::error::SqlError;
use crate::reserved::{check_identifier, check_identifier_or_wildcard};

pub(in crate::ddl_ast::parse) struct ClauseCursor<'t, 'a> {
    src: &'a str,
    toks: &'t [Token<'a>],
    pos: usize,
    statement: &'static str,
    seen: Vec<&'static str>,
}

/// A parse error with `detail`.
pub(in crate::ddl_ast::parse) fn parse_err(detail: impl Into<String>) -> SqlError {
    SqlError::Parse {
        detail: detail.into(),
    }
}

impl<'t, 'a> ClauseCursor<'t, 'a> {
    /// A cursor over `toks`, the tokens of `src`. `statement` names the
    /// statement in error messages, such as `CREATE ALERT`.
    pub(in crate::ddl_ast::parse) fn new(
        src: &'a str,
        toks: &'t [Token<'a>],
        statement: &'static str,
    ) -> Self {
        Self {
            src,
            toks,
            pos: 0,
            statement,
            seen: Vec::new(),
        }
    }

    fn peek(&self) -> Option<&'t Token<'a>> {
        let toks: &'t [Token<'a>] = self.toks;
        toks.get(self.pos)
    }

    fn tok_at(&self, offset: usize) -> Option<&'t Tok<'a>> {
        let toks: &'t [Token<'a>] = self.toks;
        toks.get(self.pos + offset).map(|t| &t.tok)
    }

    /// A short description of the next token for error messages.
    pub(in crate::ddl_ast::parse) fn describe_next(&self) -> String {
        match self.peek().map(|t| &t.tok) {
            None => "end of statement".into(),
            Some(Tok::Word(w)) => format!("'{w}'"),
            Some(Tok::Str(s)) => format!("string '{s}'"),
            Some(Tok::Ident(s)) => format!("identifier \"{s}\""),
            Some(Tok::Punct(c)) => format!("'{c}'"),
        }
    }

    /// A parse error that names this statement.
    pub(in crate::ddl_ast::parse) fn error(&self, detail: impl std::fmt::Display) -> SqlError {
        parse_err(format!("{}: {detail}", self.statement))
    }

    /// True at the end of the statement, or before one trailing `;`.
    pub(in crate::ddl_ast::parse) fn at_end(&self) -> bool {
        match self.toks.len().saturating_sub(self.pos) {
            0 => true,
            1 => matches!(self.tok_at(0), Some(Tok::Punct(';'))),
            _ => false,
        }
    }

    /// True when the next tokens are the bare words `kws`, in order.
    pub(in crate::ddl_ast::parse) fn peek_keywords(&self, kws: &[&str]) -> bool {
        kws.iter().enumerate().all(
            |(i, kw)| matches!(self.tok_at(i), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw)),
        )
    }

    /// True when the next token is the bare word `kw`.
    pub(in crate::ddl_ast::parse) fn peek_keyword(&self, kw: &str) -> bool {
        self.peek_keywords(&[kw])
    }

    /// Consume the bare word `kw` when it is next.
    pub(in crate::ddl_ast::parse) fn eat_keyword(&mut self, kw: &str) -> bool {
        self.eat_keywords(&[kw])
    }

    /// Consume the bare words `kws` when all are next, in order. Consumes
    /// nothing otherwise.
    pub(in crate::ddl_ast::parse) fn eat_keywords(&mut self, kws: &[&str]) -> bool {
        let hit = self.peek_keywords(kws);
        if hit {
            self.pos += kws.len();
        }
        hit
    }

    pub(in crate::ddl_ast::parse) fn expect_keyword(&mut self, kw: &str) -> Result<(), SqlError> {
        self.expect_keywords(&[kw])
    }

    pub(in crate::ddl_ast::parse) fn expect_keywords(
        &mut self,
        kws: &[&str],
    ) -> Result<(), SqlError> {
        if self.eat_keywords(kws) {
            return Ok(());
        }
        Err(self.error(format!(
            "expected {} — found {}",
            kws.join(" "),
            self.describe_next()
        )))
    }

    /// True when the next token is the punctuation `c`.
    pub(in crate::ddl_ast::parse) fn peek_punct(&self, c: char) -> bool {
        matches!(self.tok_at(0), Some(Tok::Punct(p)) if *p == c)
    }

    pub(in crate::ddl_ast::parse) fn eat_punct(&mut self, c: char) -> bool {
        let hit = self.peek_punct(c);
        if hit {
            self.pos += 1;
        }
        hit
    }

    pub(in crate::ddl_ast::parse) fn expect_punct(&mut self, c: char) -> Result<(), SqlError> {
        if self.eat_punct(c) {
            return Ok(());
        }
        Err(self.error(format!("expected '{c}' — found {}", self.describe_next())))
    }

    /// The next bare word, upper-cased, without consuming it.
    pub(in crate::ddl_ast::parse) fn peek_word_upper(&self) -> Option<String> {
        match self.tok_at(0) {
            Some(Tok::Word(w)) => Some(w.to_ascii_uppercase()),
            _ => None,
        }
    }

    /// Consume the next bare word.
    pub(in crate::ddl_ast::parse) fn word(&mut self, what: &str) -> Result<&'a str, SqlError> {
        match self.tok_at(0) {
            Some(Tok::Word(w)) => {
                let w: &'a str = w;
                self.pos += 1;
                Ok(w)
            }
            _ => Err(self.error(format!(
                "{what} needs a word — found {}",
                self.describe_next()
            ))),
        }
    }

    /// The source text of the next word, quoted identifier, or string, as
    /// written (quotes kept). For names a later stage decodes once.
    pub(in crate::ddl_ast::parse) fn raw(&mut self, what: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(
                tok @ Token {
                    tok: Tok::Word(_) | Tok::Ident(_) | Tok::Str(_),
                    ..
                },
            ) => {
                let src: &'a str = self.src;
                let text = &src[tok.span.clone()];
                self.pos += 1;
                Ok(text)
            }
            _ => Err(self.error(format!(
                "{what} needs a value — found {}",
                self.describe_next()
            ))),
        }
    }

    /// The source text of the next run of tokens written with no whitespace
    /// between them, such as `topic:orders` or `db.coll`. The run stops
    /// before `,`, `;`, `(` and `)`. For names a later stage decodes once.
    pub(in crate::ddl_ast::parse) fn raw_run(&mut self, what: &str) -> Result<&'a str, SqlError> {
        let stops = |tok: &Tok<'_>| matches!(tok, Tok::Punct(',' | ';' | '(' | ')'));
        let Some(first) = self.peek().filter(|t| !stops(&t.tok)) else {
            return Err(self.error(format!(
                "{what} needs a value — found {}",
                self.describe_next()
            )));
        };
        let start = first.span.start;
        let mut end = first.span.end;
        self.pos += 1;
        while let Some(next) = self.peek() {
            if next.span.start != end || stops(&next.tok) {
                break;
            }
            end = next.span.end;
            self.pos += 1;
        }
        let src: &'a str = self.src;
        Ok(&src[start..end])
    }

    /// An identifier: a bare word lower-cases and is checked against the
    /// reserved words; a quoted identifier keeps its case.
    pub(in crate::ddl_ast::parse) fn ident(&mut self, what: &str) -> Result<String, SqlError> {
        match self.peek() {
            Some(
                tok @ Token {
                    tok: Tok::Word(_) | Tok::Ident(_),
                    ..
                },
            ) => {
                let out = check_identifier(&self.src[tok.span.clone()])?;
                self.pos += 1;
                Ok(out)
            }
            _ => Err(self.error(format!(
                "{what} needs an identifier — found {}",
                self.describe_next()
            ))),
        }
    }

    /// A name with no reserved-word check: a bare word lower-cases, a quoted
    /// identifier keeps its case.
    pub(in crate::ddl_ast::parse) fn name(&mut self, what: &str) -> Result<String, SqlError> {
        let out = match self.tok_at(0) {
            Some(Tok::Word(w)) => w.to_lowercase(),
            Some(Tok::Ident(s)) => s.to_string(),
            _ => {
                return Err(self.error(format!(
                    "{what} needs a name — found {}",
                    self.describe_next()
                )));
            }
        };
        self.pos += 1;
        Ok(out)
    }

    /// An identifier or the `*` wildcard.
    pub(in crate::ddl_ast::parse) fn ident_or_wildcard(
        &mut self,
        what: &str,
    ) -> Result<String, SqlError> {
        if self.eat_punct('*') {
            return check_identifier_or_wildcard("*");
        }
        self.ident(what)
    }

    /// A single-quoted string, decoded.
    pub(in crate::ddl_ast::parse) fn string(&mut self, what: &str) -> Result<String, SqlError> {
        match self.tok_at(0) {
            Some(Tok::Str(s)) => {
                let s = s.to_string();
                self.pos += 1;
                Ok(s)
            }
            _ => Err(self.error(format!(
                "{what} needs a quoted string — found {}",
                self.describe_next()
            ))),
        }
    }

    /// A non-negative integer.
    pub(in crate::ddl_ast::parse) fn u64(&mut self, what: &str) -> Result<u64, SqlError> {
        let Some(Tok::Word(w)) = self.tok_at(0) else {
            return Err(self.error(format!(
                "{what} needs a non-negative integer — found {}",
                self.describe_next()
            )));
        };
        let value = w.parse().map_err(|_| {
            self.error(format!(
                "{what} must be a non-negative integer — found '{w}'"
            ))
        })?;
        self.pos += 1;
        Ok(value)
    }

    /// A signed integer: an optional `-` then digits.
    pub(in crate::ddl_ast::parse) fn i64(&mut self, what: &str) -> Result<i64, SqlError> {
        let negative = self.eat_punct('-');
        let Some(Tok::Word(w)) = self.tok_at(0) else {
            return Err(self.error(format!(
                "{what} needs an integer — found {}",
                self.describe_next()
            )));
        };
        let text = if negative {
            format!("-{w}")
        } else {
            (*w).to_string()
        };
        let value = text
            .parse()
            .map_err(|_| self.error(format!("{what} must be an integer — found '{text}'")))?;
        self.pos += 1;
        Ok(value)
    }

    /// `TRUE` or `FALSE`, or a quoted `'true'` / `'false'`.
    pub(in crate::ddl_ast::parse) fn bool_literal(&mut self, what: &str) -> Result<bool, SqlError> {
        let value = match self.tok_at(0) {
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("TRUE") => true,
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case("FALSE") => false,
            Some(Tok::Str(s)) if s.eq_ignore_ascii_case("true") => true,
            Some(Tok::Str(s)) if s.eq_ignore_ascii_case("false") => false,
            _ => {
                return Err(self.error(format!(
                    "{what} must be TRUE or FALSE — found {}",
                    self.describe_next()
                )));
            }
        };
        self.pos += 1;
        Ok(value)
    }

    /// One of the bare words `options`, returned as written in `options`.
    pub(in crate::ddl_ast::parse) fn one_of(
        &mut self,
        what: &str,
        options: &[&'static str],
    ) -> Result<&'static str, SqlError> {
        for option in options {
            let words: Vec<&str> = option.split(' ').collect();
            if self.eat_keywords(&words) {
                return Ok(option);
            }
        }
        Err(self.error(format!(
            "{what} must be one of {} — found {}",
            options.join(", "),
            self.describe_next()
        )))
    }

    /// `( item , item ... )` with at least one item.
    pub(in crate::ddl_ast::parse) fn paren_list<T>(
        &mut self,
        what: &str,
        mut item: impl FnMut(&mut Self) -> Result<T, SqlError>,
    ) -> Result<Vec<T>, SqlError> {
        self.expect_punct('(')?;
        if self.peek_punct(')') {
            return Err(self.error(format!("{what} must not be empty")));
        }
        let mut out = Vec::new();
        loop {
            out.push(item(self)?);
            if self.eat_punct(',') {
                continue;
            }
            self.expect_punct(')')?;
            return Ok(out);
        }
    }

    /// The text inside a balanced `( ... )`, outer parentheses excluded.
    pub(in crate::ddl_ast::parse) fn raw_paren(&mut self, what: &str) -> Result<&'a str, SqlError> {
        let Some(open) = self.peek().filter(|t| t.tok == Tok::Punct('(')) else {
            return Err(self.error(format!("{what} needs '(' — found {}", self.describe_next())));
        };
        let inner_start = open.span.end;
        let src: &'a str = self.src;
        let mut depth = 0i32;
        while let Some(tok) = self.peek() {
            match tok.tok {
                Tok::Punct('(') => depth += 1,
                Tok::Punct(')') => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = &src[inner_start..tok.span.start];
                        self.pos += 1;
                        return Ok(inner.trim());
                    }
                }
                _ => {}
            }
            self.pos += 1;
        }
        Err(self.error(format!("{what}: unclosed '('")))
    }

    /// Source text up to the first stop sequence at parenthesis depth zero,
    /// or to the end of the statement. A stop inside a literal never matches.
    /// Empty text is an error.
    pub(in crate::ddl_ast::parse) fn raw_until(
        &mut self,
        what: &str,
        stops: &[&[&str]],
    ) -> Result<&'a str, SqlError> {
        let start = self.pos;
        let mut depth = 0i32;
        while self.pos < self.toks.len() {
            if depth == 0 && (self.at_end() || stops.iter().any(|s| self.peek_keywords(s))) {
                break;
            }
            match self.tok_at(0) {
                Some(Tok::Punct('(')) => depth += 1,
                Some(Tok::Punct(')')) => depth -= 1,
                _ => {}
            }
            self.pos += 1;
        }
        if self.pos == start {
            return Err(self.error(format!(
                "{what} needs a value — found {}",
                self.describe_next()
            )));
        }
        if depth != 0 {
            return Err(self.error(format!("{what}: unbalanced parentheses")));
        }
        let src: &'a str = self.src;
        let text = &src[self.toks[start].span.start..self.toks[self.pos - 1].span.end];
        Ok(text.trim())
    }

    /// The source text from the next token to the cursor's last token, with
    /// one trailing `;` left out. Consumes every token.
    pub(in crate::ddl_ast::parse) fn rest(&mut self) -> &'a str {
        let toks: &'t [Token<'a>] = self.toks;
        let Some(first) = toks.get(self.pos) else {
            self.pos = toks.len();
            return "";
        };
        let mut last_idx = toks.len() - 1;
        if last_idx > self.pos && toks[last_idx].tok == Tok::Punct(';') {
            last_idx -= 1;
        }
        let src: &'a str = self.src;
        let text = if toks[last_idx].tok == Tok::Punct(';') {
            ""
        } else {
            &src[first.span.start..toks[last_idx].span.end]
        };
        self.pos = toks.len();
        text.trim()
    }

    /// The tokens not yet consumed.
    pub(in crate::ddl_ast::parse) fn remaining_tokens(&self) -> &'t [Token<'a>] {
        let toks: &'t [Token<'a>] = self.toks;
        toks.get(self.pos..).unwrap_or(&[])
    }

    /// Consume the next `n` tokens, as found by `remaining_tokens`.
    pub(in crate::ddl_ast::parse) fn skip(&mut self, n: usize) {
        self.pos = (self.pos + n).min(self.toks.len());
    }

    /// Record that clause `name` was read. A second read is an error.
    pub(in crate::ddl_ast::parse) fn clause(&mut self, name: &'static str) -> Result<(), SqlError> {
        if self.seen.contains(&name) {
            return Err(self.error(format!("{name} given twice")));
        }
        self.seen.push(name);
        Ok(())
    }

    /// The error for a token at a clause position that names no clause.
    pub(in crate::ddl_ast::parse) fn unknown_clause(&self) -> SqlError {
        self.error(format!("unexpected {}", self.describe_next()))
    }

    /// Succeed only when every token was consumed (one trailing `;` allowed).
    pub(in crate::ddl_ast::parse) fn finish(&self) -> Result<(), SqlError> {
        if self.at_end() {
            Ok(())
        } else {
            Err(self.unknown_clause())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::token::tokenize;
    use super::*;

    #[test]
    fn keyword_matches_only_at_the_cursor() {
        let sql = "NAME 'ON x' ON t";
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "TEST");
        assert!(!c.eat_keyword("ON"));
        c.expect_keyword("NAME").expect("NAME");
        assert_eq!(c.string("name").expect("string"), "ON x");
        c.expect_keyword("ON").expect("ON");
        assert_eq!(c.ident("collection").expect("ident"), "t");
        c.finish().expect("all consumed");
    }

    #[test]
    fn finish_refuses_leftover_tokens() {
        let sql = "A B;";
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "TEST");
        c.expect_keyword("A").expect("A");
        assert!(c.finish().is_err());
        c.expect_keyword("B").expect("B");
        c.finish().expect("trailing ; allowed");
    }

    #[test]
    fn raw_until_stops_at_depth_zero_outside_literals() {
        let sql = "WHERE f(a, 'GROUP BY') > 1 GROUP BY x";
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "TEST");
        c.expect_keyword("WHERE").expect("WHERE");
        let text = c.raw_until("WHERE", &[&["GROUP", "BY"]]).expect("text");
        assert_eq!(text, "f(a, 'GROUP BY') > 1");
        assert!(c.eat_keywords(&["GROUP", "BY"]));
    }

    #[test]
    fn duplicate_clause_is_refused() {
        let sql = "";
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "TEST");
        c.clause("WINDOW").expect("first");
        assert!(c.clause("WINDOW").is_err());
    }
}
