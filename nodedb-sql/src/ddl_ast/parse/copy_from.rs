// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <collection> FROM '<path>' [WITH (FORMAT ..., DELIMITER ..., HEADER ...)]`.
//!
//! The server-side file-path form parses here. The STDIN streaming shape
//! (`COPY <name> FROM STDIN`) parses in `copy_from_stdin`. The restore shape
//! `COPY tenant_restore(<id>) FROM STDIN` returns `None`, so dispatch falls
//! through to the backup/restore handler. `COPY ... TO` and
//! `COPY (SELECT ...) TO` belong to the `copy_to` family.
//!
//! The direction is the keyword right after the copied relation, never a
//! `FROM` or `TO` inside a path, a name, or a query.

use super::clause::{ClauseCursor, Tok, Token, tokenize};
use crate::ddl_ast::statement::{CopyFormat, MiscStmt, NodedbStatement};
use crate::error::SqlError;

/// Which way a `COPY` statement moves rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyDirection {
    /// `COPY <relation> FROM '<path>'`.
    FromFile,
    /// `COPY <relation> FROM STDIN`.
    FromStdin,
    /// `COPY <relation> TO ...`.
    To,
}

/// The direction of a `COPY` statement, read from the keyword that follows
/// the copied relation: a name (`users`, `"Users"`, `tenant_restore(1)`) or a
/// parenthesized query. `None` when the statement is not a `COPY` or names
/// no direction there.
pub(super) fn copy_direction(sql: &str) -> Option<CopyDirection> {
    let toks = tokenize(sql);
    let end = relation_end(&toks)?;
    let mut c = ClauseCursor::new(sql, &toks[end..], "COPY");
    if c.eat_keyword("TO") {
        return Some(CopyDirection::To);
    }
    if c.eat_keyword("FROM") {
        return Some(if c.peek_keyword("STDIN") {
            CopyDirection::FromStdin
        } else {
            CopyDirection::FromFile
        });
    }
    None
}

/// Index of the first token after the copied relation, or `None` when the
/// statement does not start with `COPY`.
fn relation_end(toks: &[Token<'_>]) -> Option<usize> {
    if !matches!(toks.first().map(|t| &t.tok), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("COPY"))
    {
        return None;
    }
    let mut i = 1;
    let mut depth = 0i32;
    while let Some(tok) = toks.get(i) {
        match &tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => {
                depth -= 1;
                if depth == 0 && i > 1 && toks[1].tok == Tok::Punct('(') {
                    // A parenthesized query ends at its matching `)`.
                    return Some(i + 1);
                }
            }
            Tok::Word(w)
                if depth == 0
                    && i > 1
                    && (w.eq_ignore_ascii_case("FROM") || w.eq_ignore_ascii_case("TO")) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Try to parse a COPY statement.
///
/// Returns `None` unless the statement is `COPY <collection> FROM '<path>'`
/// or `COPY <name> FROM STDIN`. Returns `Some(Err)` for a malformed
/// statement of either shape.
pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !upper.starts_with("COPY ") {
        return None;
    }
    match copy_direction(trimmed)? {
        CopyDirection::FromFile => Some(parse_copy_from(trimmed)),
        CopyDirection::FromStdin => super::copy_from_stdin::try_parse(trimmed),
        CopyDirection::To => None,
    }
}

fn parse_copy_from(sql: &str) -> Result<NodedbStatement, SqlError> {
    // Grammar: COPY <name> FROM '<path>' [WITH (...)]
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "COPY");
    c.expect_keyword("COPY")?;
    let collection = copy_collection_name(&mut c)?;
    c.expect_keyword("FROM")?;
    let path = c
        .string("FROM")
        .map_err(|_| c.error("expected single-quoted path after FROM"))?;
    let (format, delimiter, header) = parse_copy_options(&mut c)?;
    let format = match format {
        Some(f) => Some(f),
        None => detect_format_from_path(&path)?,
    };
    Ok(NodedbStatement::Misc(MiscStmt::CopyFromFile {
        collection,
        path,
        format,
        delimiter,
        header,
    }))
}

/// The copied collection: one bare or quoted name, quotes removed and
/// lower-cased.
pub(super) fn copy_collection_name(c: &mut ClauseCursor<'_, '_>) -> Result<String, SqlError> {
    let raw = c.raw_run("collection name")?;
    let unquoted = raw
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(raw);
    Ok(unquoted.to_lowercase())
}

/// Read an optional `WITH (key [=] value, ...)` and require the end of the
/// statement. Returns `(format, delimiter, header)`; `header` defaults to
/// true.
pub(super) fn parse_copy_options(
    c: &mut ClauseCursor<'_, '_>,
) -> Result<(Option<CopyFormat>, Option<char>, bool), SqlError> {
    let mut format = None;
    let mut delimiter = None;
    let mut header = true;
    if c.eat_keyword("WITH") {
        let options = c.paren_list("WITH options", |c| {
            let key = c.word("option name")?.to_ascii_uppercase();
            c.eat_punct('=');
            let value = match c.string(&key) {
                Ok(value) => value,
                Err(_) => c.word(&key)?.to_string(),
            };
            Ok((key, value))
        })?;
        for (key, value) in options {
            match key.as_str() {
                "FORMAT" => format = Some(parse_format_value(&value)?),
                "DELIMITER" => delimiter = Some(parse_delimiter_value(&value)?),
                "HEADER" => {
                    header = parse_bool_value(&value).ok_or_else(|| {
                        c.error(format!("HEADER must be true or false — found '{value}'"))
                    })?;
                }
                other => return Err(c.error(format!("WITH: unknown option '{other}'"))),
            }
        }
    }
    if !c.at_end() {
        return Err(c.error(format!("unexpected trailing content {}", c.describe_next())));
    }
    Ok((format, delimiter, header))
}

fn parse_format_value(s: &str) -> Result<CopyFormat, SqlError> {
    match s.trim().to_lowercase().as_str() {
        "ndjson" | "jsonl" => Ok(CopyFormat::Ndjson),
        "json" => Ok(CopyFormat::JsonArray),
        "csv" => Ok(CopyFormat::Csv),
        other => Err(SqlError::Parse {
            detail: format!("COPY: unknown FORMAT '{other}'; expected ndjson, json, or csv"),
        }),
    }
}

pub(super) fn parse_delimiter_value(s: &str) -> Result<char, SqlError> {
    let mut chars = s.chars();
    let ch = chars.next().ok_or_else(|| SqlError::Parse {
        detail: "COPY: DELIMITER must be a single character".to_string(),
    })?;
    if chars.next().is_some() {
        return Err(SqlError::Parse {
            detail: "COPY: DELIMITER must be a single character".to_string(),
        });
    }
    Ok(ch)
}

pub(super) fn parse_bool_value(s: &str) -> Option<bool> {
    match s.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Detect the copy format from the file path extension.
///
/// Returns `Err` if the extension is unrecognised or absent (suggests an
/// explicit WITH clause).
pub(super) fn detect_format_from_path(path: &str) -> Result<Option<CopyFormat>, SqlError> {
    let lower = path.to_lowercase();
    if lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        return Ok(Some(CopyFormat::Ndjson));
    }
    if lower.ends_with(".json") {
        return Ok(Some(CopyFormat::JsonArray));
    }
    if lower.ends_with(".csv") {
        return Ok(Some(CopyFormat::Csv));
    }
    Err(SqlError::Parse {
        detail: format!(
            "COPY: cannot infer format from path '{path}'; \
             add WITH (FORMAT ndjson|json|csv)"
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn unicode_collection_before_from_preserves_original_offsets() {
        let stmt = ok("COPY usersßﬀİ FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection, path, ..
            }) => {
                assert_eq!(collection, "usersßﬀi\u{307}");
                assert_eq!(path, "/tmp/users.ndjson");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn basic_ndjson_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                collection,
                path,
                format,
                delimiter,
                header,
            }) => {
                assert_eq!(collection, "users");
                assert_eq!(path, "/tmp/users.ndjson");
                assert_eq!(format, Some(CopyFormat::Ndjson));
                assert_eq!(delimiter, None);
                assert!(header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_array_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.json'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_by_extension() {
        let stmt = ok("COPY users FROM '/tmp/users.csv'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn explicit_format_overrides_extension() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT json)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_with_delimiter() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, DELIMITER ';')");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile {
                format, delimiter, ..
            }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
                assert_eq!(delimiter, Some(';'));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn header_false() {
        let stmt = ok("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, HEADER false)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyFromFile { header, .. }) => {
                assert!(!header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn non_boolean_header_is_refused() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (HEADER maybe)");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn trailing_content_after_with_is_refused() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv) EXTRA");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn to_inside_the_path_does_not_change_the_direction() {
        let stmt = ok("COPY users FROM '/tmp/copy to mars.csv'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyFromFile { .. })
        ));
    }

    #[test]
    fn unknown_extension_is_err() {
        let e = err("COPY users FROM '/tmp/data.parquet'");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("cannot infer format"));
    }

    #[test]
    fn copy_to_handled_by_copy_to_parser() {
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY users TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(try_parse(&upper, &parts, sql).is_none());
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_query_form_handled_by_copy_to_parser() {
        use super::super::copy_to::try_parse as copy_to_try_parse;
        let sql = "COPY (SELECT * FROM users) TO '/tmp/out.csv'";
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        assert!(try_parse(&upper, &parts, sql).is_none());
        assert!(copy_to_try_parse(&upper, sql).is_some());
    }

    #[test]
    fn copy_from_stdin_returns_none() {
        // Should return None so backup/restore handler catches it.
        assert!(parse("COPY tenant_restore(1) FROM STDIN").is_none());
    }

    #[test]
    fn unknown_with_option_is_err() {
        let e = err("COPY users FROM '/tmp/data.csv' WITH (FORMAT csv, BOGUS opt)");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("BOGUS"));
    }
}
