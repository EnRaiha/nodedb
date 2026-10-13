// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE INDEX / DROP INDEX / SHOW INDEX / REINDEX.

use super::clause::{ClauseCursor, Token, tokenize};
use super::helpers::if_exists_name;
use crate::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    if !(upper.starts_with("CREATE UNIQUE INDEX ")
        || upper.starts_with("CREATE INDEX ")
        || upper.starts_with("DROP INDEX ")
        || upper.starts_with("SHOW INDEX")
        || upper.starts_with("REINDEX "))
    {
        return None;
    }
    let toks = tokenize(trimmed);
    Some(parse(trimmed, &toks).map(NodedbStatement::Collection))
}

fn parse<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<CollectionStmt, SqlError> {
    let mut c = ClauseCursor::new(sql, toks, "INDEX");
    if c.peek_keyword("CREATE") {
        return parse_create_index(sql, toks);
    }
    if c.eat_keywords(&["DROP", "INDEX"]) {
        let (if_exists, name) = if_exists_name(&mut c, "index name")?;
        c.finish()?;
        return Ok(CollectionStmt::DropIndex {
            name,
            collection: None,
            if_exists,
        });
    }
    if c.eat_keyword("SHOW") {
        if !c.eat_keyword("INDEXES") {
            c.expect_keyword("INDEX")?;
        }
        c.eat_keyword("ON");
        let collection = if c.at_end() {
            None
        } else {
            Some(c.raw("collection")?.to_string())
        };
        c.finish()?;
        return Ok(CollectionStmt::ShowIndexes { collection });
    }
    parse_reindex(&mut c)
}

/// `REINDEX [TABLE] [INDEX <name>] [CONCURRENTLY] <collection>`.
fn parse_reindex(c: &mut ClauseCursor<'_, '_>) -> Result<CollectionStmt, SqlError> {
    c.expect_keyword("REINDEX")?;
    c.eat_keyword("TABLE");
    let index_name = if c.eat_keyword("INDEX") {
        if c.peek_keyword("CONCURRENTLY") || c.at_end() {
            return Err(c.error("REINDEX INDEX requires an index name before CONCURRENTLY"));
        }
        Some(c.ident("index name")?)
    } else {
        None
    };
    let concurrent = c.eat_keyword("CONCURRENTLY");
    let collection = c.ident("collection")?;
    c.finish()?;
    Ok(CollectionStmt::Reindex {
        collection,
        index_name,
        concurrent,
    })
}

/// Keywords that belong to the `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`
/// prefix. None of them can be an index name: accepting one as a name turns
/// an unsupported spelling into a different, valid-looking statement that
/// targets an object the user never mentioned.
const INDEX_CLAUSE_KEYWORDS: [&str; 5] = ["IF", "NOT", "EXISTS", "ON", "UNIQUE"];

/// Parse `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON collection (field)
///         [COLLATE NOCASE|CI] [WHERE cond]` into a typed `CreateIndex`
/// variant. `ON collection FIELDS field` is the same as `(field)`.
///
/// `IF NOT EXISTS` is also accepted with the anonymous form
/// (`CREATE INDEX IF NOT EXISTS ON users (email)`): the flag then applies to
/// the name the executor derives from the collection and field, which is
/// deterministic, so a repeated statement is still a no-op.
fn parse_create_index<'a>(sql: &'a str, toks: &[Token<'a>]) -> Result<CollectionStmt, SqlError> {
    let mut c = ClauseCursor::new(sql, toks, "CREATE INDEX");
    c.expect_keyword("CREATE")?;
    let unique = c.eat_keyword("UNIQUE");
    c.expect_keyword("INDEX")?;
    let if_not_exists = c.eat_keywords(&["IF", "NOT", "EXISTS"]);

    let index_name = if c.peek_keyword("ON") {
        None
    } else {
        if INDEX_CLAUSE_KEYWORDS.iter().any(|kw| c.peek_keyword(kw)) {
            return Err(c.error(format!(
                "{} is a keyword of this clause and cannot be an index name; expected \
                 CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON <collection> (<field>)",
                c.describe_next()
            )));
        }
        Some(c.ident("index name")?)
    };
    c.expect_keyword("ON")?;
    let collection = c.ident("collection")?;
    let field = if c.eat_keyword("FIELDS") {
        c.ident("indexed field")?
    } else {
        let mut fields = c.paren_list("indexed field", |c| c.ident("indexed field"))?;
        if fields.len() != 1 {
            return Err(c.error(format!(
                "a secondary index covers one field; found {}",
                fields.len()
            )));
        }
        fields.remove(0)
    };

    let mut case_insensitive = false;
    let mut where_condition = None;
    while !c.at_end() {
        if c.eat_keyword("COLLATE") {
            c.clause("COLLATE")?;
            c.one_of("COLLATE", &["NOCASE", "CI"])?;
            case_insensitive = true;
        } else if c.eat_keyword("WHERE") {
            c.clause("WHERE")?;
            where_condition = Some(c.raw_until("WHERE", &[&["COLLATE"]])?.to_string());
        } else {
            return Err(c.unknown_clause());
        }
    }

    Ok(CollectionStmt::CreateIndex {
        unique,
        index_name,
        collection,
        field,
        case_insensitive,
        where_condition,
        if_not_exists,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape of a parsed `CREATE [UNIQUE] INDEX` statement, flattened for
    /// assertions.
    struct ParsedIndex {
        unique: bool,
        index_name: Option<String>,
        collection: String,
        field: String,
        if_not_exists: bool,
    }

    fn parse(sql: &str) -> Result<NodedbStatement, SqlError> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql).expect("index family should claim this statement")
    }

    fn parse_index(sql: &str) -> ParsedIndex {
        match parse(sql).expect("parse should succeed") {
            NodedbStatement::Collection(CollectionStmt::CreateIndex {
                unique,
                index_name,
                collection,
                field,
                if_not_exists,
                ..
            }) => ParsedIndex {
                unique,
                index_name,
                collection,
                field,
                if_not_exists,
            },
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn create_index_if_not_exists() {
        // The `IF NOT EXISTS` keywords must not be consumed as the
        // index name — that would shift every later token, naming the index
        // `if` and reading the target collection as `exists`.
        let parsed = parse_index("CREATE INDEX IF NOT EXISTS idx ON users (email)");
        assert!(parsed.if_not_exists);
        assert!(!parsed.unique);
        assert_eq!(parsed.index_name.as_deref(), Some("idx"));
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn create_unique_index_if_not_exists() {
        let parsed = parse_index("CREATE UNIQUE INDEX IF NOT EXISTS idx ON users (email)");
        assert!(parsed.if_not_exists);
        assert!(parsed.unique);
        assert_eq!(parsed.index_name.as_deref(), Some("idx"));
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn create_index_without_if_not_exists_unchanged() {
        let parsed = parse_index("CREATE INDEX idx ON users (email)");
        assert!(!parsed.if_not_exists);
        assert!(!parsed.unique);
        assert_eq!(parsed.index_name.as_deref(), Some("idx"));
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");

        let parsed = parse_index("CREATE UNIQUE INDEX idx ON users (email)");
        assert!(!parsed.if_not_exists);
        assert!(parsed.unique);
        assert_eq!(parsed.index_name.as_deref(), Some("idx"));
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn anonymous_index_has_no_name() {
        let parsed = parse_index("CREATE INDEX ON users (email)");
        assert!(parsed.index_name.is_none());
        assert!(!parsed.if_not_exists);
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn if_not_exists_with_anonymous_index() {
        // Both clauses are optional and independent: with no name given the
        // executor derives one from the collection and field, and the flag
        // makes a repeat of the same statement a no-op on that derived name.
        let parsed = parse_index("CREATE INDEX IF NOT EXISTS ON users (email)");
        assert!(parsed.if_not_exists);
        assert!(parsed.index_name.is_none());
        assert_eq!(parsed.collection, "users");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn quoted_index_collection_and_field_keep_their_case() {
        let parsed = parse_index("CREATE INDEX \"IdX\" ON \"MiXeD\" (\"EmAiL\")");
        assert_eq!(parsed.index_name.as_deref(), Some("IdX"));
        assert_eq!(parsed.collection, "MiXeD");
        assert_eq!(parsed.field, "EmAiL");

        let fields_form = parse_index("CREATE INDEX idx ON \"MiXeD\" FIELDS \"EmAiL\"");
        assert_eq!(fields_form.collection, "MiXeD");
        assert_eq!(fields_form.field, "EmAiL");

        let inline = parse_index("CREATE INDEX idx ON \"MiXeD\"(\"EmAiL\")");
        assert_eq!(inline.collection, "MiXeD");
        assert_eq!(inline.field, "EmAiL");
    }

    #[test]
    fn bare_index_collection_and_field_lowercase() {
        let parsed = parse_index("CREATE INDEX IdX ON MiXeD (EmAiL)");
        assert_eq!(parsed.index_name.as_deref(), Some("idx"));
        assert_eq!(parsed.collection, "mixed");
        assert_eq!(parsed.field, "email");
    }

    #[test]
    fn clause_keyword_is_not_an_index_name() {
        // An unsupported spelling must be a syntax error, never a statement
        // aimed at a different object.
        for sql in [
            "CREATE INDEX IF EXISTS idx ON users (email)",
            "CREATE INDEX NOT EXISTS idx ON users (email)",
            "CREATE INDEX EXISTS ON users (email)",
            "CREATE INDEX UNIQUE idx ON users (email)",
        ] {
            let err = parse(sql).expect_err(sql);
            assert!(matches!(err, SqlError::Parse { .. }), "{sql}: {err:?}");
        }
    }

    #[test]
    fn if_not_exists_is_case_insensitive() {
        for sql in [
            "CREATE INDEX if not exists idx ON users (email)",
            "CREATE INDEX If Not Exists idx ON users (email)",
        ] {
            let parsed = parse_index(sql);
            assert!(parsed.if_not_exists, "{sql}");
            assert_eq!(parsed.index_name.as_deref(), Some("idx"), "{sql}");
            assert_eq!(parsed.collection, "users", "{sql}");
            assert_eq!(parsed.field, "email", "{sql}");
        }
    }

    #[test]
    fn misspelled_collate_is_refused() {
        assert!(parse("CREATE INDEX i ON t (email) COLATE NOCASE").is_err());
    }

    #[test]
    fn collate_inside_a_where_literal_does_not_set_case_insensitive() {
        let stmt =
            parse("CREATE INDEX i ON t (email) WHERE note = 'COLLATE NOCASE'").expect("parses");
        let NodedbStatement::Collection(CollectionStmt::CreateIndex {
            case_insensitive,
            where_condition,
            ..
        }) = stmt
        else {
            panic!("expected CreateIndex");
        };
        assert!(!case_insensitive);
        assert_eq!(where_condition.as_deref(), Some("note = 'COLLATE NOCASE'"));
    }

    #[test]
    fn collate_then_where_parse_together() {
        let stmt = parse("CREATE UNIQUE INDEX i ON t (email) COLLATE NOCASE WHERE active = true")
            .expect("parses");
        let NodedbStatement::Collection(CollectionStmt::CreateIndex {
            case_insensitive,
            where_condition,
            ..
        }) = stmt
        else {
            panic!("expected CreateIndex");
        };
        assert!(case_insensitive);
        assert_eq!(where_condition.as_deref(), Some("active = true"));
    }

    #[test]
    fn multi_field_list_is_refused() {
        assert!(parse("CREATE INDEX i ON t (a, b)").is_err());
    }

    #[test]
    fn reindex_forms() {
        let NodedbStatement::Collection(CollectionStmt::Reindex {
            collection,
            index_name,
            concurrent,
        }) = parse("REINDEX INDEX i CONCURRENTLY docs").expect("parses")
        else {
            panic!("expected Reindex");
        };
        assert_eq!(collection, "docs");
        assert_eq!(index_name.as_deref(), Some("i"));
        assert!(concurrent);
        assert!(parse("REINDEX INDEX CONCURRENTLY docs").is_err());
        assert!(parse("REINDEX docs extra").is_err());
    }
}
