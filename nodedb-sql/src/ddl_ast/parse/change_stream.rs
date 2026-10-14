// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER CHANGE STREAM and CREATE/DROP/SHOW CONSUMER GROUP.

use super::clause::{ClauseCursor, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{NodedbStatement, StreamViewStmt};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("SHOW CHANGE STREAM") {
        return Some(Ok(NodedbStatement::StreamView(
            StreamViewStmt::ShowChangeStreams,
        )));
    }
    let known = [
        "CREATE CHANGE STREAM ",
        "DROP CHANGE STREAM ",
        "ALTER CHANGE STREAM ",
        "CREATE CONSUMER GROUP ",
        "DROP CONSUMER GROUP ",
        "SHOW CONSUMER GROUPS",
    ];
    if !known.iter().any(|p| upper.starts_with(p)) {
        return None;
    }
    Some(parse(trimmed).map(NodedbStatement::StreamView))
}

fn parse(sql: &str) -> Result<StreamViewStmt, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "CHANGE STREAM");
    if c.peek_keywords(&["CREATE", "CHANGE", "STREAM"]) {
        return parse_create_change_stream(&mut c);
    }
    if c.eat_keywords(&["DROP", "CHANGE", "STREAM"]) {
        let (if_exists, name) = if_exists_name(&mut c, "stream name")?;
        c.finish()?;
        return Ok(StreamViewStmt::DropChangeStream { name, if_exists });
    }
    if c.eat_keywords(&["ALTER", "CHANGE", "STREAM"]) {
        let name = c.ident("stream name")?;
        let action = c.word("action")?.to_uppercase();
        c.finish()?;
        return Ok(StreamViewStmt::AlterChangeStream { name, action });
    }
    if c.eat_keywords(&["CREATE", "CONSUMER", "GROUP"]) {
        // CREATE CONSUMER GROUP <name> ON <stream>
        //
        // Both names stay raw. A stream name can carry the `topic:` buffer
        // prefix, which the handler resolves along with the identifier.
        let group_name = c.raw_run("group name")?.to_string();
        c.expect_keyword("ON")?;
        let stream_name = c.raw_run("stream name")?.to_string();
        c.finish()?;
        return Ok(StreamViewStmt::CreateConsumerGroup {
            group_name,
            stream_name,
        });
    }
    if c.eat_keywords(&["DROP", "CONSUMER", "GROUP"]) {
        // DROP CONSUMER GROUP [IF EXISTS] <name> ON <stream> [IF EXISTS]
        let leading = c.eat_keywords(&["IF", "EXISTS"]);
        let name = c.raw_run("group name")?.to_string();
        c.expect_keyword("ON")?;
        let stream = c.raw_run("stream name")?.to_string();
        let trailing = c.eat_keywords(&["IF", "EXISTS"]);
        c.finish()?;
        let if_exists = leading || trailing;
        return Ok(StreamViewStmt::DropConsumerGroup {
            name,
            stream,
            if_exists,
        });
    }
    c.expect_keywords(&["SHOW", "CONSUMER", "GROUPS"])?;
    let stream = if c.at_end() {
        None
    } else {
        c.eat_keyword("ON");
        Some(c.raw_run("stream name")?.to_string())
    };
    c.finish()?;
    Ok(StreamViewStmt::ShowConsumerGroups { stream })
}

/// Structural extraction for `CREATE CHANGE STREAM <name> ON <collection|*>
/// [WITH (...)]`.
///
/// Extracts name, collection, and the raw WITH clause inner text. The
/// handler converts the raw WITH pairs to OpFilter, StreamFormat, etc.
fn parse_create_change_stream(c: &mut ClauseCursor<'_, '_>) -> Result<StreamViewStmt, SqlError> {
    c.expect_keywords(&["CREATE", "CHANGE", "STREAM"])?;
    let name = c.ident("stream name")?;
    c.expect_keyword("ON")?;
    // `*` is the every-collection wildcard, not an identifier.
    let collection = c.ident_or_wildcard("collection")?;
    let with_clause_raw = if c.eat_keyword("WITH") {
        c.raw_paren("WITH")?.to_string()
    } else {
        String::new()
    };
    c.finish()?;
    Ok(StreamViewStmt::CreateChangeStream {
        name,
        collection,
        with_clause_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create(sql: &str) -> StreamViewStmt {
        parse(sql).expect("valid CREATE CHANGE STREAM")
    }

    #[test]
    fn quoted_names_keep_case() {
        let StreamViewStmt::CreateChangeStream {
            name, collection, ..
        } = create("CREATE CHANGE STREAM \"MiXeD\" ON \"OrDeRs\"")
        else {
            panic!("expected CreateChangeStream");
        };
        assert_eq!(name, "MiXeD");
        assert_eq!(collection, "OrDeRs");
    }

    #[test]
    fn basic_stream() {
        let StreamViewStmt::CreateChangeStream {
            name,
            collection,
            with_clause_raw,
        } = create("CREATE CHANGE STREAM orders_stream ON orders")
        else {
            panic!("expected CreateChangeStream");
        };
        assert_eq!(name, "orders_stream");
        assert_eq!(collection, "orders");
        assert!(with_clause_raw.is_empty());
    }

    #[test]
    fn wildcard_collection() {
        let StreamViewStmt::CreateChangeStream { collection, .. } =
            create("CREATE CHANGE STREAM all ON *")
        else {
            panic!("expected CreateChangeStream");
        };
        assert_eq!(collection, "*");
    }

    #[test]
    fn with_clause() {
        let StreamViewStmt::CreateChangeStream {
            with_clause_raw, ..
        } = create(
            "CREATE CHANGE STREAM s ON orders WITH (FORMAT = 'msgpack', INCLUDE = 'INSERT')",
        )
        else {
            panic!("expected CreateChangeStream");
        };
        assert_eq!(with_clause_raw, "FORMAT = 'msgpack', INCLUDE = 'INSERT'");
    }

    #[test]
    fn collection_named_with_a_with_prefix_keeps_options() {
        let StreamViewStmt::CreateChangeStream {
            collection,
            with_clause_raw,
            ..
        } = create("CREATE CHANGE STREAM s ON with_history WITH (FORMAT = 'json')")
        else {
            panic!("expected CreateChangeStream");
        };
        assert_eq!(collection, "with_history");
        assert_eq!(with_clause_raw, "FORMAT = 'json'");
    }

    #[test]
    fn trailing_tokens_are_refused() {
        assert!(parse("CREATE CHANGE STREAM s ON orders EXTRA").is_err());
    }

    #[test]
    fn consumer_group_keeps_topic_prefix() {
        let StreamViewStmt::CreateConsumerGroup {
            group_name,
            stream_name,
        } = parse("CREATE CONSUMER GROUP g ON topic:orders").expect("parses")
        else {
            panic!("expected CreateConsumerGroup");
        };
        assert_eq!(group_name, "g");
        assert_eq!(stream_name, "topic:orders");
    }

    #[test]
    fn drop_consumer_group_reads_on_at_its_position() {
        let StreamViewStmt::DropConsumerGroup {
            name,
            stream,
            if_exists,
        } = parse("DROP CONSUMER GROUP IF EXISTS g ON s").expect("parses")
        else {
            panic!("expected DropConsumerGroup");
        };
        assert_eq!(
            (name.as_str(), stream.as_str(), if_exists),
            ("g", "s", true)
        );
    }

    /// `IF EXISTS` is read after the stream name too.
    #[test]
    fn drop_consumer_group_reads_a_trailing_if_exists() {
        let StreamViewStmt::DropConsumerGroup {
            name,
            stream,
            if_exists,
        } = parse("DROP CONSUMER GROUP g ON s IF EXISTS").expect("parses")
        else {
            panic!("expected DropConsumerGroup");
        };
        assert_eq!(
            (name.as_str(), stream.as_str(), if_exists),
            ("g", "s", true)
        );
    }
}
