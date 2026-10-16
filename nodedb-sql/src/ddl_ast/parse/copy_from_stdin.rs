// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <name> FROM STDIN [WITH (FORMAT csv|text, DELIMITER '<c>',
//! HEADER [bool])]`.
//!
//! Only a one-token relation parses here. `COPY tenant_restore(1) FROM
//! STDIN` is the restore shape, which the backup detector owns, so it
//! returns `None`.

use super::clause::{ClauseCursor, Tok, tokenize};
use super::copy_from::{copy_collection_name, parse_bool_value, parse_delimiter_value};
use crate::ddl_ast::statement::{CopyStdinFormat, MiscStmt, NodedbStatement};
use crate::error::SqlError;

/// Parse a `COPY ... FROM STDIN` statement. `None` when the copied relation
/// is more than one name token, or the options are not this grammar's (a
/// `FORMAT json`, say): such a statement keeps the refusal the bulk-import
/// router gives it.
pub(super) fn try_parse(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
    let toks = tokenize(sql);
    // `COPY`, the name, then `FROM`.
    let one_name = matches!(
        toks.get(1).map(|t| &t.tok),
        Some(Tok::Word(_) | Tok::Ident(_))
    ) && matches!(toks.get(2).map(|t| &t.tok), Some(Tok::Word(w)) if w.eq_ignore_ascii_case("FROM"));
    if !one_name {
        return None;
    }
    let mut c = ClauseCursor::new(sql, &toks, "COPY");
    parse_stdin(&mut c).ok().map(Ok)
}

fn parse_stdin(c: &mut ClauseCursor<'_, '_>) -> Result<NodedbStatement, SqlError> {
    c.expect_keyword("COPY")?;
    let collection = copy_collection_name(c)?;
    c.expect_keyword("FROM")?;
    c.expect_keyword("STDIN")?;
    let mut format = CopyStdinFormat::Text;
    let mut delimiter = None;
    // PostgreSQL reads no header line unless HEADER is given.
    let mut header = false;
    if c.eat_keyword("WITH") {
        let options = c.paren_list("WITH options", |c| {
            let key = c.word("option name")?.to_ascii_uppercase();
            c.eat_punct('=');
            // A bare `HEADER` turns the header line on.
            if key == "HEADER" && (c.peek_punct(',') || c.peek_punct(')')) {
                return Ok((key, "true".to_string()));
            }
            let value = match c.string(&key) {
                Ok(value) => value,
                Err(_) => c.word(&key)?.to_string(),
            };
            Ok((key, value))
        })?;
        for (key, value) in options {
            match key.as_str() {
                "FORMAT" => format = parse_format(&value)?,
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
    Ok(NodedbStatement::Misc(MiscStmt::CopyFromStdin {
        collection,
        format,
        delimiter,
        header,
    }))
}

fn parse_format(value: &str) -> Result<CopyStdinFormat, SqlError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "text" => Ok(CopyStdinFormat::Text),
        "csv" => Ok(CopyStdinFormat::Csv),
        other => Err(SqlError::Parse {
            detail: format!("COPY FROM STDIN: unknown FORMAT '{other}'; expected csv or text"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn stdin(sql: &str) -> (String, CopyStdinFormat, Option<char>, bool) {
        match parse(sql).expect("expected Some").expect("expected Ok") {
            NodedbStatement::Misc(MiscStmt::CopyFromStdin {
                collection,
                format,
                delimiter,
                header,
            }) => (collection, format, delimiter, header),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn bare_stdin_defaults_to_text_without_header() {
        let (name, format, delimiter, header) = stdin("COPY scene FROM STDIN");
        assert_eq!(name, "scene");
        assert_eq!(format, CopyStdinFormat::Text);
        assert_eq!(delimiter, None);
        assert!(!header);
    }

    #[test]
    fn csv_with_bare_header_and_delimiter() {
        let (_, format, delimiter, header) =
            stdin("COPY scene FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER)");
        assert_eq!(format, CopyStdinFormat::Csv);
        assert_eq!(delimiter, Some(';'));
        assert!(header);
    }

    #[test]
    fn header_takes_an_explicit_boolean() {
        let (_, _, _, header) = stdin("COPY scene FROM STDIN WITH (FORMAT csv, HEADER false)");
        assert!(!header);
    }

    #[test]
    fn quoted_name_keeps_no_quotes() {
        let (name, ..) = stdin("COPY \"Scene\" FROM STDIN");
        assert_eq!(name, "scene");
    }

    #[test]
    fn another_grammars_options_are_not_parsed() {
        assert!(parse("COPY scene FROM STDIN WITH (FORMAT json)").is_none());
        assert!(parse("COPY scene FROM STDIN WITH (BOGUS 1)").is_none());
    }

    #[test]
    fn restore_shape_is_not_parsed() {
        assert!(parse("COPY tenant_restore(1) FROM STDIN").is_none());
    }
}
