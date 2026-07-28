// SPDX-License-Identifier: BUSL-1.1

//! One-call front door for the index-DDL surfaces: tokenize, consume the
//! leading command keywords, parse the header, then parse the option tail
//! against a closed set. Handlers get a fully validated shape or an error —
//! there is no path on which an unread token survives.

use super::super::super::super::result::DdlError;
use super::super::support::ddl_err;
use super::header::{HeaderSpec, IndexHeader, parse_index_header};
use super::keywords::{OptionSpec, ParsedOptions, parse_options};
use super::lex::tokenize;

/// A fully parsed `CREATE <kind> INDEX` statement.
#[derive(Debug)]
pub struct IndexStatement {
    pub header: IndexHeader,
    pub options: ParsedOptions,
}

/// Parse `sql` as `<leading keywords> <header> <options>`.
///
/// `leading` is the command prefix already matched by the router, e.g.
/// `["CREATE", "VECTOR", "INDEX"]`; it is re-checked here so the grammar
/// stays self-contained rather than trusting the dispatcher's prefix test.
pub fn parse_index_statement(
    sql: &str,
    leading: &[&str],
    header_spec: &HeaderSpec,
    option_specs: &[OptionSpec],
    context: &str,
) -> Result<IndexStatement, DdlError> {
    let toks = tokenize(sql).ok_or_else(|| {
        ddl_err(
            "42601",
            format!(
                "{context}: could not parse statement; syntax: {}",
                header_spec.syntax
            ),
        )
    })?;

    if toks.len() < leading.len()
        || !leading
            .iter()
            .zip(&toks)
            .all(|(kw, tok)| tok.is_keyword(kw))
    {
        return Err(ddl_err("42601", format!("syntax: {}", header_spec.syntax)));
    }

    let rest = &toks[leading.len()..];
    let (header, consumed) = parse_index_header(rest, header_spec)?;
    let options = parse_options(&rest[consumed..], option_specs, context)?;

    Ok(IndexStatement { header, options })
}

#[cfg(test)]
mod tests {
    use super::super::header::{ColumnMode, NameMode};
    use super::super::keywords::OptionSpec;
    use super::*;

    const LEADING: &[&str] = &["CREATE", "VECTOR", "INDEX"];
    const CONTEXT: &str = "CREATE VECTOR INDEX";

    const HEADER: HeaderSpec = HeaderSpec {
        name: NameMode::Required,
        columns: ColumnMode::AtMostOne,
        syntax: "CREATE VECTOR INDEX <name> ON <collection> [(<column>)] [DIM <n>]",
    };

    const OPTIONS: &[OptionSpec] = &[OptionSpec::uint("DIM"), OptionSpec::ident("METRIC")];

    fn parse(sql: &str) -> Result<IndexStatement, DdlError> {
        parse_index_statement(sql, LEADING, &HEADER, OPTIONS, CONTEXT)
    }

    #[test]
    fn parses_header_and_options_together() {
        let stmt = parse("CREATE VECTOR INDEX idx ON coll (emb) METRIC cosine DIM 4").unwrap();
        assert_eq!(stmt.header.name, "idx");
        assert_eq!(stmt.header.collection, "coll");
        assert_eq!(stmt.header.column(), "emb");
        assert_eq!(stmt.options.text("METRIC"), Some("cosine"));
        assert_eq!(stmt.options.uint("DIM"), Some(4));
    }

    #[test]
    fn rejects_the_option_spelling_that_was_silently_skipped() {
        let err = parse("CREATE VECTOR INDEX idx ON coll (emb) WITH (dim = 3, metric = 'cosine')")
            .unwrap_err();
        assert!(
            err.message.contains("unrecognized option 'WITH'"),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_a_trailing_word_after_a_complete_statement() {
        assert!(parse("CREATE VECTOR INDEX idx ON coll DIM 4 CONCURRENTLY").is_err());
    }

    #[test]
    fn rejects_a_mismatched_command_prefix() {
        assert!(parse("CREATE SPARSE INDEX idx ON coll").is_err());
    }
}
