// SPDX-License-Identifier: Apache-2.0

//! Top-level `try_parse` for COLLECTION / TABLE DDL.
//!
//! Recognises:
//! - `CREATE COLLECTION` / `CREATE TABLE` (with optional `IF NOT EXISTS`)
//! - `UNDROP COLLECTION` / `UNDROP TABLE`
//! - `DROP COLLECTION` / `DROP TABLE` (with `PURGE`, `CASCADE`, `CASCADE FORCE`)
//! - `ALTER COLLECTION` / `ALTER TABLE`
//! - `DESCRIBE <name>` (excluding `DESCRIBE SEQUENCE`)
//! - `\d` / `SHOW COLLECTIONS`

use super::super::clause::{ClauseCursor, tokenize};
use super::alter_ops::parse_alter_operation;
use super::body::parse_collection_body;
use crate::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use crate::error::SqlError;

pub(in crate::ddl_ast::parse) fn try_parse(
    upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        let create_table = upper.starts_with("CREATE TABLE ");
        if upper.starts_with("CREATE COLLECTION ") || create_table {
            let toks = tokenize(trimmed);
            let mut c = ClauseCursor::new(trimmed, &toks, "CREATE COLLECTION");
            c.expect_keyword("CREATE")?;
            if !c.eat_keyword("COLLECTION") {
                c.expect_keyword("TABLE")?;
            }
            let if_not_exists = c.eat_keywords(&["IF", "NOT", "EXISTS"]);
            let name = c.ident("collection name")?;
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(&mut c, trimmed)?;
            let stmt = if create_table {
                CollectionStmt::CreateTable {
                    name,
                    if_not_exists,
                    engine,
                    columns,
                    options,
                    flags,
                    balanced_raw,
                }
            } else {
                CollectionStmt::CreateCollection {
                    name,
                    if_not_exists,
                    engine,
                    columns,
                    options,
                    flags,
                    balanced_raw,
                }
            };
            return Ok(Some(NodedbStatement::Collection(stmt)));
        }
        if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
            let toks = tokenize(trimmed);
            let mut c = ClauseCursor::new(trimmed, &toks, "UNDROP COLLECTION");
            c.expect_keyword("UNDROP")?;
            if !c.eat_keyword("COLLECTION") {
                c.expect_keyword("TABLE")?;
            }
            let name = c.ident("collection name")?;
            c.finish()?;
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::UndropCollection { name },
            )));
        }
        if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
            let toks = tokenize(trimmed);
            let mut c = ClauseCursor::new(trimmed, &toks, "DROP COLLECTION");
            c.expect_keyword("DROP")?;
            if !c.eat_keyword("COLLECTION") {
                c.expect_keyword("TABLE")?;
            }
            let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
            let name = c.ident("collection name")?;
            let (mut purge, mut cascade, mut force) = (false, false, false);
            while !c.at_end() {
                if c.eat_keyword("PURGE") {
                    c.clause("PURGE")?;
                    purge = true;
                } else if c.eat_keyword("CASCADE") {
                    c.clause("CASCADE")?;
                    cascade = true;
                } else if c.eat_keyword("FORCE") {
                    c.clause("FORCE")?;
                    force = true;
                } else {
                    return Err(c.unknown_clause());
                }
            }
            if force && !cascade {
                return Err(c.error("FORCE applies only with CASCADE"));
            }
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::DropCollection {
                    name,
                    if_exists,
                    purge,
                    cascade,
                    cascade_force: force,
                },
            )));
        }
        if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
            let toks = tokenize(trimmed);
            let mut c = ClauseCursor::new(trimmed, &toks, "ALTER COLLECTION");
            c.expect_keyword("ALTER")?;
            if !c.eat_keyword("COLLECTION") {
                c.expect_keyword("TABLE")?;
            }
            let name = c.ident("collection name")?;
            let operation = match parse_alter_operation(&mut c, &name) {
                None => return Ok(None),
                Some(op) => op?,
            };
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::AlterCollection { name, operation },
            )));
        }
        if upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE") {
            let toks = tokenize(trimmed);
            let mut c = ClauseCursor::new(trimmed, &toks, "DESCRIBE");
            c.expect_keyword("DESCRIBE")?;
            let name = c.raw("collection name")?.to_string();
            c.finish()?;
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::DescribeCollection { name },
            )));
        }
        if upper == "\\D" || upper.starts_with("SHOW COLLECTIONS") {
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::ShowCollections,
            )));
        }
        Ok(None)
    })()
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_sql(sql: &str) -> Option<Result<NodedbStatement, SqlError>> {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
    }

    #[test]
    fn drop_flags_are_read_at_their_position() {
        let Some(Ok(NodedbStatement::Collection(CollectionStmt::DropCollection {
            purge,
            cascade,
            cascade_force,
            ..
        }))) = try_sql("DROP COLLECTION t CASCADE FORCE")
        else {
            panic!("expected DropCollection");
        };
        assert!(!purge && cascade && cascade_force);
    }

    #[test]
    fn drop_with_unknown_trailing_word_is_refused() {
        assert!(matches!(
            try_sql("DROP COLLECTION t PURGEE"),
            Some(Err(SqlError::Parse { .. }))
        ));
    }

    #[test]
    fn if_not_exists_inside_a_default_literal_is_not_the_flag() {
        let Some(Ok(NodedbStatement::Collection(CollectionStmt::CreateCollection {
            if_not_exists,
            ..
        }))) = try_sql("CREATE COLLECTION t (note TEXT DEFAULT 'IF NOT EXISTS')")
        else {
            panic!("expected CreateCollection");
        };
        assert!(!if_not_exists);
    }
}
