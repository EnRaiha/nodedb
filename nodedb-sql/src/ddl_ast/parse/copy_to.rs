// SPDX-License-Identifier: Apache-2.0

//! Parse `COPY <collection> TO '<path>' [WITH (...)]` and
//! `COPY (SELECT ...) TO '<path>' [WITH (...)]`.

use crate::ddl_ast::statement::{CopyFormat, CopyToSource, MiscStmt, NodedbStatement};
use crate::error::SqlError;

use super::clause::{ClauseCursor, tokenize};
use super::copy_from::{CopyDirection, copy_collection_name, copy_direction};
use super::copy_options::{detect_format_from_path, parse_copy_options};

/// Try to parse a COPY TO statement.
///
/// Returns `None` unless the statement is `COPY <relation> TO ...`. Returns
/// `Some(Err)` on parse errors. Returns `Some(Ok(CopyToFile {...}))` on
/// success.
pub(super) fn try_parse(upper: &str, trimmed: &str) -> Option<Result<NodedbStatement, SqlError>> {
    if !upper.starts_with("COPY ") {
        return None;
    }
    if copy_direction(trimmed) != Some(CopyDirection::To) {
        return None;
    }
    Some(parse_copy_to(trimmed))
}

fn parse_copy_to(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "COPY");
    c.expect_keyword("COPY")?;
    let source = if c.peek_punct('(') {
        let query = c.raw_paren("query")?;
        if query.is_empty() {
            return Err(c.error("empty query in query form"));
        }
        CopyToSource::Query(query.to_string())
    } else {
        CopyToSource::Collection(copy_collection_name(&mut c)?)
    };
    c.expect_keyword("TO")?;
    let path = c.string("TO").map_err(|_| {
        c.error(format!(
            "expected single-quoted path after TO — found {}",
            c.describe_next()
        ))
    })?;
    let options = parse_copy_options(&mut c)?;
    let format = match options.format.clone() {
        Some(f) => Some(f),
        None => detect_format_from_path(&path)?,
    };
    if format == Some(CopyFormat::GeoTiff) {
        return Err(SqlError::FeatureNotSupported {
            detail: "COPY TO does not write GeoTIFF; export with FORMAT ndjson, json, or csv"
                .to_string(),
        });
    }
    options.check_for(format.as_ref())?;
    Ok(NodedbStatement::Misc(MiscStmt::CopyToFile {
        source,
        path,
        format,
        delimiter: options.delimiter,
        header: options.header.unwrap_or(true),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;
    use crate::ddl_ast::statement::CopyFormat;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn unicode_collection_before_to_preserves_original_offsets() {
        let stmt = ok("COPY usersßﬀİ TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { source, path, .. }) => {
                assert_eq!(
                    source,
                    CopyToSource::Collection("usersßﬀi\u{307}".to_string())
                );
                assert_eq!(path, "/tmp/out.ndjson");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn basic_ndjson_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile {
                source,
                path,
                format,
                ..
            }) => {
                assert_eq!(source, CopyToSource::Collection("users".to_string()));
                assert_eq!(path, "/tmp/out.ndjson");
                assert_eq!(format, Some(CopyFormat::Ndjson));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn json_array_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.json'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::JsonArray));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_by_extension() {
        let stmt = ok("COPY users TO '/tmp/out.csv'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Csv));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn explicit_format_with_clause() {
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT ndjson)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { format, .. }) => {
                assert_eq!(format, Some(CopyFormat::Ndjson));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn csv_with_delimiter() {
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT csv, DELIMITER ';')");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile {
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
        let stmt = ok("COPY users TO '/tmp/out.csv' WITH (FORMAT csv, HEADER false)");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { header, .. }) => {
                assert!(!header);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn query_form() {
        let stmt = ok("COPY (SELECT * FROM users WHERE active = true) TO '/tmp/out.ndjson'");
        match stmt {
            NodedbStatement::Misc(MiscStmt::CopyToFile { source, path, .. }) => {
                assert_eq!(
                    source,
                    CopyToSource::Query("SELECT * FROM users WHERE active = true".to_string())
                );
                assert_eq!(path, "/tmp/out.ndjson");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn geotiff_export_is_refused() {
        for sql in [
            "COPY users TO '/tmp/out.tif'",
            "COPY users TO '/tmp/out.bin' WITH (FORMAT geotiff)",
        ] {
            assert!(
                matches!(err(sql), SqlError::FeatureNotSupported { .. }),
                "{sql}"
            );
        }
    }

    #[test]
    fn raster_options_are_refused_for_export() {
        let e = err("COPY users TO '/tmp/out.csv' WITH (NODATA 0)");
        assert!(matches!(e, SqlError::Parse { .. }));
    }

    #[test]
    fn from_inside_the_path_does_not_change_the_direction() {
        let stmt = ok("COPY users TO '/tmp/copy from mars.csv'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyToFile { .. })
        ));
    }

    #[test]
    fn copy_from_not_intercepted() {
        let stmt = ok("COPY users FROM '/tmp/data.ndjson'");
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CopyFromFile { .. })
        ));
    }

    #[test]
    fn unknown_extension_is_err() {
        let e = err("COPY users TO '/tmp/out.parquet'");
        assert!(matches!(e, SqlError::Parse { .. }));
        assert!(e.to_string().contains("cannot infer format"));
    }
}
