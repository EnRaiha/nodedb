// SPDX-License-Identifier: Apache-2.0

//! Tokenizer for the DDL clause cursor.
//!
//! Built on the shared SQL lexer, so a keyword inside a string literal, a
//! quoted identifier, or a comment is never a token of its own. Every token
//! keeps its byte span, so a parser can recover the original text of an
//! expression it does not interpret.

use std::borrow::Cow;
use std::ops::Range;

use crate::parser::preprocess::lex::{self, SqlSegment};

/// One DDL token.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::ddl_ast::parse) enum Tok<'a> {
    /// A bare word: keyword, unquoted identifier, number, or duration.
    Word(&'a str),
    /// A `'...'` or `E'...'` string with its quotes stripped and escapes decoded.
    Str(Cow<'a, str>),
    /// A `"..."` identifier with its quotes stripped and `""` decoded.
    Ident(Cow<'a, str>),
    /// One punctuation character such as `(`, `)`, `,`, `;`, `=`, `.`, `*`.
    Punct(char),
}

/// A token with its byte span in the statement.
#[derive(Debug, Clone)]
pub(in crate::ddl_ast::parse) struct Token<'a> {
    pub tok: Tok<'a>,
    pub span: Range<usize>,
}

/// Split one DDL statement into tokens. Comments produce no token.
pub(in crate::ddl_ast::parse) fn tokenize(sql: &str) -> Vec<Token<'_>> {
    let mut out = Vec::new();
    let mut base = 0usize;
    for segment in lex::segments(sql) {
        let raw = match &segment {
            SqlSegment::Text(t)
            | SqlSegment::SingleQuotedString(t)
            | SqlSegment::QuotedIdent(t)
            | SqlSegment::LineComment(t)
            | SqlSegment::BlockComment(t) => *t,
        };
        let span = base..base + raw.len();
        match segment {
            SqlSegment::Text(text) => push_text(text, base, &mut out),
            // An unterminated literal becomes its bare opening quote, so any
            // parser that expects a string or a name refuses it.
            SqlSegment::SingleQuotedString(_) if !is_terminated(raw, '\'') => out.push(Token {
                tok: Tok::Punct('\''),
                span,
            }),
            SqlSegment::QuotedIdent(_) if !is_terminated(raw, '"') => out.push(Token {
                tok: Tok::Punct('"'),
                span,
            }),
            SqlSegment::SingleQuotedString(_) => out.push(Token {
                tok: Tok::Str(unquote_string(raw)),
                span,
            }),
            SqlSegment::QuotedIdent(_) => out.push(Token {
                tok: Tok::Ident(unquote(raw, '"')),
                span,
            }),
            SqlSegment::LineComment(_) | SqlSegment::BlockComment(_) => {}
        }
        base += raw.len();
    }
    out
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || b >= 0x80
}

fn push_text<'a>(text: &'a str, base: usize, out: &mut Vec<Token<'a>>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if is_word_byte(b) {
            let start = i;
            while i < bytes.len() {
                let c = bytes[i];
                // A decimal point inside a number stays in the word: `1.5`.
                let decimal_point = c == b'.'
                    && text[start..i].bytes().all(|d| d.is_ascii_digit())
                    && bytes.get(i + 1).is_some_and(u8::is_ascii_digit);
                if !(is_word_byte(c) || decimal_point) {
                    break;
                }
                i += 1;
            }
            out.push(Token {
                tok: Tok::Word(&text[start..i]),
                span: base + start..base + i,
            });
            continue;
        }
        // Text segments split at ASCII bytes, so a punctuation byte is a
        // complete character.
        out.push(Token {
            tok: Tok::Punct(char::from(b)),
            span: base + i..base + i + 1,
        });
        i += 1;
    }
}

/// Whether the quoted run `raw` (opening quote included, optional `E`
/// prefix for strings) ends with its own closing quote.
fn is_terminated(raw: &str, q: char) -> bool {
    let escape = q == '\'' && raw.starts_with(['E', 'e']);
    let body = if escape {
        raw.get(1..).unwrap_or(raw)
    } else {
        raw
    };
    let Some(inner) = body.strip_prefix(q) else {
        return false;
    };
    let mut chars = inner.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if escape && c == '\\' {
            chars.next();
            continue;
        }
        if c == q {
            if chars.peek().is_some_and(|(_, next)| *next == q) {
                chars.next();
                continue;
            }
            return i + c.len_utf8() == inner.len();
        }
    }
    false
}

/// Strip the outer `q` quotes and decode doubled `qq` escapes.
fn unquote(raw: &str, q: char) -> Cow<'_, str> {
    let inner = raw
        .strip_prefix(q)
        .map(|rest| rest.strip_suffix(q).unwrap_or(rest))
        .unwrap_or(raw);
    let doubled: String = [q, q].iter().collect();
    if inner.contains(&doubled) {
        Cow::Owned(inner.replace(&doubled, &q.to_string()))
    } else {
        Cow::Borrowed(inner)
    }
}

/// Decode `'...'` (doubled `''`) or `E'...'` (also backslash escapes).
fn unquote_string(raw: &str) -> Cow<'_, str> {
    let Some(rest) = raw.strip_prefix(['E', 'e']) else {
        return unquote(raw, '\'');
    };
    let inner = rest
        .strip_prefix('\'')
        .map(|r| r.strip_suffix('\'').unwrap_or(r))
        .unwrap_or(rest);
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some(other) => out.push(other),
                None => {}
            },
            '\'' => {
                // `''` inside the string is one quote.
                chars.next();
                out.push('\'');
            }
            other => out.push(other),
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(sql: &str) -> Vec<Tok<'_>> {
        tokenize(sql).into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn keyword_inside_a_string_is_not_a_word() {
        assert_eq!(
            kinds("SET x = 'TRIGGER now'"),
            vec![
                Tok::Word("SET"),
                Tok::Word("x"),
                Tok::Punct('='),
                Tok::Str(Cow::Borrowed("TRIGGER now")),
            ]
        );
    }

    #[test]
    fn comments_produce_no_token_and_spans_point_at_source() {
        let sql = "A /* B */ \"Q\"\"x\" -- C\n1.5";
        let toks = tokenize(sql);
        assert_eq!(toks.len(), 3);
        assert_eq!(toks[1].tok, Tok::Ident(Cow::Owned("Q\"x".into())));
        assert_eq!(&sql[toks[1].span.clone()], "\"Q\"\"x\"");
        assert_eq!(toks[2].tok, Tok::Word("1.5"));
    }

    #[test]
    fn escape_string_decodes_backslashes() {
        assert_eq!(
            kinds("E'a\\nb''c'"),
            vec![Tok::Str(Cow::Owned("a\nb'c".into()))]
        );
    }
}
