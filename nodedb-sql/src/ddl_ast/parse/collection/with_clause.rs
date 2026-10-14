// SPDX-License-Identifier: Apache-2.0

//! Read the `WITH ...` options clause and the `BALANCED ON (...)` clause of
//! CREATE COLLECTION.
//!
//! Both spellings of the options clause are accepted: the parenthesised
//! `WITH (engine='kv', ttl=...)` and the bare `WITH storage = 'kv', ttl = ...`.
//! They mean the same thing. The reader works on tokens, so a keyword, comma,
//! or parenthesis inside a literal never ends a value.

use super::column_list::matching_close;
use super::flags::flag_index;
use crate::ddl_ast::parse::clause::{ClauseCursor, Tok};
use crate::error::SqlError;

/// Read the `key = value` pairs after `WITH`, in either spelling. Keys are
/// lower-cased. A value is:
/// - a quoted string or quoted identifier: its decoded text;
/// - a `[ ... ]` list: the text between the brackets, as written;
/// - anything else: the source text of its tokens, as written, such as
///   `INTERVAL '1 hour'` or `last_active + INTERVAL '1 hour'`.
///
/// A pair without `=` or without a value is an error.
pub(super) fn read_with_options(
    c: &mut ClauseCursor<'_, '_>,
    src: &str,
) -> Result<Vec<(String, String)>, SqlError> {
    if c.peek_punct('(') {
        return c.paren_list("WITH options", |c| read_pair(c, src, false));
    }
    let mut pairs = vec![read_pair(c, src, true)?];
    while c.eat_punct(',') {
        pairs.push(read_pair(c, src, true)?);
    }
    Ok(pairs)
}

fn read_pair(
    c: &mut ClauseCursor<'_, '_>,
    src: &str,
    bare: bool,
) -> Result<(String, String), SqlError> {
    let key = c.name("WITH option name")?.to_lowercase();
    c.expect_punct('=')?;
    let value = read_value(c, src, &key, bare)?;
    Ok((key, value))
}

/// A word that ends a bare options clause: the clauses parsed after it and
/// the free-standing flags.
fn ends_bare_clause(word: &str) -> bool {
    ["ENGINE", "BALANCED", "WITH"]
        .iter()
        .any(|kw| word.eq_ignore_ascii_case(kw))
        || flag_index(word).is_some()
}

fn read_value(
    c: &mut ClauseCursor<'_, '_>,
    src: &str,
    key: &str,
    bare: bool,
) -> Result<String, SqlError> {
    let toks = c.remaining_tokens();
    if c.peek_punct('[') {
        let close = matching_close(toks, 0, '[', ']')
            .ok_or_else(|| c.error(format!("WITH option {key}: unclosed '['")))?;
        let inner = if close > 1 {
            src[toks[1].span.start..toks[close - 1].span.end].trim()
        } else {
            ""
        };
        c.skip(close + 1);
        return Ok(inner.to_string());
    }

    let mut depth = 0i32;
    let mut n = 0usize;
    while let Some(tok) = toks.get(n) {
        match &tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') if depth == 0 => break,
            Tok::Punct(')') => depth -= 1,
            Tok::Punct(',' | ';') if depth == 0 => break,
            Tok::Word(w) if bare && depth == 0 && n > 0 && ends_bare_clause(w) => break,
            _ => {}
        }
        n += 1;
    }
    if n == 0 {
        return Err(c.error(format!(
            "WITH option {key} needs a value — found {}",
            c.describe_next()
        )));
    }
    if depth != 0 {
        return Err(c.error(format!("WITH option {key}: unbalanced parentheses")));
    }
    let value = match &toks[0].tok {
        Tok::Str(s) | Tok::Ident(s) if n == 1 => s.to_string(),
        _ => src[toks[0].span.start..toks[n - 1].span.end].to_string(),
    };
    c.skip(n);
    Ok(value)
}

/// Read `ON ( ... )` after `BALANCED` and return the text inside the
/// parentheses. The handler parses it with `parse_balanced_clause_from_raw`.
pub(super) fn read_balanced(c: &mut ClauseCursor<'_, '_>) -> Result<String, SqlError> {
    c.expect_keyword("ON")?;
    Ok(c.raw_paren("BALANCED ON")?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::clause::tokenize;

    fn options(sql: &str) -> Result<Vec<(String, String)>, SqlError> {
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "CREATE COLLECTION");
        c.expect_keyword("WITH")?;
        let pairs = read_with_options(&mut c, sql)?;
        c.finish()?;
        Ok(pairs)
    }

    fn pair(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn values_after_unicode_text_keep_their_offsets() {
        assert_eq!(
            options("WITH (note = 'ﬀﬀ', ttl = 60)").expect("parses"),
            vec![pair("note", "ﬀﬀ"), pair("ttl", "60")]
        );
    }

    #[test]
    fn interval_value_keeps_its_literal() {
        // The old reader cut the value at the first quote and kept `INTERVAL`.
        assert_eq!(
            options("WITH (ttl = INTERVAL '15 minutes', capacity = 10)").expect("parses"),
            vec![pair("ttl", "INTERVAL '15 minutes'"), pair("capacity", "10")]
        );
        assert_eq!(
            options("WITH ttl = last_active + INTERVAL '1 hour'").expect("parses"),
            vec![pair("ttl", "last_active + INTERVAL '1 hour'")]
        );
    }

    #[test]
    fn literal_comma_and_quote_stay_in_the_value() {
        assert_eq!(
            options("WITH (note = 'a, it''s)', engine = 'kv')").expect("parses"),
            vec![pair("note", "a, it's)"), pair("engine", "kv")]
        );
    }

    #[test]
    fn list_value_is_its_bracket_interior() {
        assert_eq!(
            options("WITH (payload_indexes = ['a', 'b'])").expect("parses"),
            vec![pair("payload_indexes", "'a', 'b'")]
        );
    }

    #[test]
    fn pair_without_equals_or_value_is_an_error() {
        assert!(options("WITH (engine 'kv')").is_err());
        assert!(options("WITH (engine = )").is_err());
        assert!(options("WITH (payload_indexes = ['a'").is_err());
    }

    #[test]
    fn balanced_clause_after_unicode_text_keeps_its_offsets() {
        let sql = "BALANCED ON (group_key = 'ﬀ', debit = tenant_id)";
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "CREATE COLLECTION");
        c.expect_keyword("BALANCED").expect("BALANCED");
        assert_eq!(
            read_balanced(&mut c).expect("parses"),
            "group_key = 'ﬀ', debit = tenant_id"
        );
    }
}
