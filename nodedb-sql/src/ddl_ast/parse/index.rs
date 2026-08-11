// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE INDEX / DROP INDEX / SHOW INDEX / REINDEX.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use crate::error::SqlError;

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE UNIQUE INDEX ") || upper.starts_with("CREATE UNIQUE IND") {
            return Ok(Some(parse_create_index(true, upper, parts, trimmed)?));
        }
        if upper.starts_with("CREATE INDEX ") {
            return Ok(Some(parse_create_index(false, upper, parts, trimmed)?));
        }
        if upper.starts_with("DROP INDEX ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "INDEX") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::DropIndex {
                    name,
                    collection: None,
                    if_exists,
                },
            )));
        }
        if upper.starts_with("SHOW INDEX") {
            let collection = parts.get(2).map(|s| s.to_string());
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::ShowIndexes { collection },
            )));
        }
        if upper.starts_with("REINDEX ") {
            // Grammar: REINDEX [INDEX <name>] [CONCURRENTLY] <collection>
            //
            // Parsing strategy: walk parts starting at index 1.
            // Optional INDEX <name> occupies positions 1-2; optional CONCURRENTLY
            // is anywhere before the final token; the last token is the collection.
            let mut offset = 1usize;
            let mut index_name: Option<String> = None;
            let mut concurrent = false;

            // Skip optional TABLE keyword for backwards compat (REINDEX TABLE <coll>)
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("TABLE"))
                .unwrap_or(false)
            {
                offset += 1;
            }

            // Consume optional INDEX <name>. The name is required when INDEX is
            // present and must not be the CONCURRENTLY keyword.
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("INDEX"))
                .unwrap_or(false)
            {
                offset += 1;
                let name = parts.get(offset).ok_or_else(|| SqlError::Parse {
                    detail: "REINDEX INDEX requires an index name".to_string(),
                })?;
                if name.eq_ignore_ascii_case("CONCURRENTLY") {
                    return Err(SqlError::Parse {
                        detail: "REINDEX INDEX requires an index name before CONCURRENTLY"
                            .to_string(),
                    });
                }
                index_name = Some(name.to_lowercase());
                offset += 1;
            }

            // Consume optional CONCURRENTLY
            if parts
                .get(offset)
                .map(|p| p.eq_ignore_ascii_case("CONCURRENTLY"))
                .unwrap_or(false)
            {
                concurrent = true;
                offset += 1;
            }

            // Remaining token is the collection name
            let collection = match parts.get(offset) {
                None => return Ok(None),
                Some(s) => s.to_lowercase(),
            };

            return Ok(Some(NodedbStatement::Collection(CollectionStmt::Reindex {
                collection,
                index_name,
                concurrent,
            })));
        }
        Ok(None)
    })()
    .transpose()
}

/// Keywords that belong to the `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`
/// prefix. None of them can be an index name: accepting one as a name turns
/// an unsupported spelling into a different, valid-looking statement that
/// targets an object the user never mentioned.
const INDEX_CLAUSE_KEYWORDS: [&str; 5] = ["IF", "NOT", "EXISTS", "ON", "UNIQUE"];

/// Parse `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON collection (field)
///         [WHERE cond] [COLLATE NOCASE]` into a typed `CreateIndex` variant.
///
/// `IF NOT EXISTS` is also accepted with the anonymous form
/// (`CREATE INDEX IF NOT EXISTS ON users (email)`): the flag then applies to
/// the name the executor derives from the collection and field, which is
/// deterministic, so a repeated statement is still a no-op.
fn parse_create_index(
    unique: bool,
    upper: &str,
    parts: &[&str],
    _trimmed: &str,
) -> Result<NodedbStatement, SqlError> {
    // Skip "CREATE [UNIQUE] INDEX" prefix.
    let mut idx_offset: usize = if unique { 3 } else { 2 };

    // Optional `IF NOT EXISTS`, matched token-wise so any amount of internal
    // whitespace and any casing works (`parts` is whitespace-split).
    let if_not_exists = parts.len() > idx_offset + 2
        && parts[idx_offset].eq_ignore_ascii_case("IF")
        && parts[idx_offset + 1].eq_ignore_ascii_case("NOT")
        && parts[idx_offset + 2].eq_ignore_ascii_case("EXISTS");
    if if_not_exists {
        idx_offset += 3;
    }

    // Detect whether an explicit name is present: if parts[idx_offset] is "ON",
    // the name was omitted; otherwise it is the index name.
    let (index_name, on_offset) = if parts
        .get(idx_offset)
        .map(|p| p.eq_ignore_ascii_case("ON"))
        .unwrap_or(false)
    {
        (None, idx_offset)
    } else {
        match parts.get(idx_offset) {
            None => (None, idx_offset + 1),
            Some(token) => {
                if INDEX_CLAUSE_KEYWORDS
                    .iter()
                    .any(|kw| token.eq_ignore_ascii_case(kw))
                {
                    return Err(SqlError::Parse {
                        detail: format!(
                            "CREATE INDEX: '{token}' is a keyword of this clause and cannot be \
                             an index name; expected \
                             CREATE [UNIQUE] INDEX [IF NOT EXISTS] [name] ON <collection> (<field>)"
                        ),
                    });
                }
                let name = token.to_lowercase();
                (
                    if name.is_empty() { None } else { Some(name) },
                    idx_offset + 1,
                )
            }
        }
    };

    // parts[on_offset] should be "ON"; collection follows.
    let raw_collection_token = parts.get(on_offset + 1).copied().unwrap_or("");

    let (collection, field) = if let Some(paren_pos) = raw_collection_token.find('(') {
        // Inline form: `collection(field)`.
        let coll = raw_collection_token[..paren_pos].to_lowercase();
        let fld = raw_collection_token[paren_pos..]
            .trim_matches(|c| c == '(' || c == ')')
            .to_string();
        (coll, fld)
    } else if parts
        .get(on_offset + 2)
        .map(|p| p.eq_ignore_ascii_case("FIELDS"))
        .unwrap_or(false)
    {
        // FIELDS keyword form: ON collection FIELDS field
        let coll = raw_collection_token.to_lowercase();
        let fld = parts.get(on_offset + 3).copied().unwrap_or("").to_string();
        (coll, fld)
    } else {
        // Standard form: ON collection (field)
        let coll = raw_collection_token.to_lowercase();
        let fld = parts
            .get(on_offset + 2)
            .map(|s| s.trim_matches(|c| c == '(' || c == ')').to_string())
            .unwrap_or_default();
        (coll, fld)
    };

    // WHERE condition.
    let where_condition = if let Some(_pos) = upper.find(" WHERE ") {
        // Recompute position in original-case `_trimmed` ... but we have `upper` only.
        // Use byte offset from upper — safe because the WHERE keyword itself is ASCII.
        // The value after WHERE is the original-case remainder.
        // We need the original text; use `parts` to reconstruct.
        let where_tok_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("WHERE"));
        where_tok_idx.map(|i| parts[i + 1..].join(" ").trim_end_matches(';').to_string())
    } else {
        None
    };

    let case_insensitive = upper.contains("COLLATE NOCASE") || upper.contains("COLLATE CI");

    Ok(NodedbStatement::Collection(CollectionStmt::CreateIndex {
        unique,
        index_name,
        collection,
        field,
        case_insensitive,
        where_condition,
        if_not_exists,
    }))
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
        // Regression: the `IF NOT EXISTS` keywords used to be consumed as the
        // index name, shifting every later token — the index was named `if`
        // and the target collection read as `exists`.
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
}
