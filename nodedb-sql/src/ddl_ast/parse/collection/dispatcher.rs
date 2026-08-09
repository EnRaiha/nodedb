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

use super::super::helpers::{extract_name_after_if_exists, extract_name_after_keyword};
use super::alter_ops::parse_alter_operation;
use super::body::parse_collection_body;
use crate::ddl_ast::statement::{CollectionStmt, NodedbStatement};
use crate::error::SqlError;

pub(in crate::ddl_ast::parse) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE COLLECTION ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "COLLECTION") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::CreateCollection {
                    name,
                    if_not_exists,
                    engine,
                    columns,
                    options,
                    flags,
                    balanced_raw,
                },
            )));
        }
        if upper.starts_with("CREATE TABLE ") {
            let if_not_exists = upper.contains("IF NOT EXISTS");
            let name = match extract_name_after_keyword(parts, "TABLE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            let (engine, columns, options, flags, balanced_raw) =
                parse_collection_body(trimmed, &name)?;
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::CreateTable {
                    name,
                    if_not_exists,
                    engine,
                    columns,
                    options,
                    flags,
                    balanced_raw,
                },
            )));
        }
        if upper.starts_with("UNDROP COLLECTION ") || upper.starts_with("UNDROP TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::UndropCollection { name },
            )));
        }
        if upper.starts_with("DROP COLLECTION ") || upper.starts_with("DROP TABLE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "COLLECTION")
                .or_else(|| extract_name_after_if_exists(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let purge = upper.contains(" PURGE");
            let cascade = upper.contains(" CASCADE");
            let cascade_force =
                upper.contains(" CASCADE FORCE") || upper.contains(" FORCE CASCADE");
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::DropCollection {
                    name,
                    if_exists,
                    purge,
                    cascade: cascade || cascade_force,
                    cascade_force,
                },
            )));
        }
        if upper.starts_with("ALTER COLLECTION ") || upper.starts_with("ALTER TABLE ") {
            let name = match extract_name_after_keyword(parts, "COLLECTION")
                .or_else(|| extract_name_after_keyword(parts, "TABLE"))
            {
                None => return Ok(None),
                Some(r) => r?,
            };
            let operation = match parse_alter_operation(upper, parts, trimmed, &name) {
                None => return Ok(None),
                Some(op) => op?,
            };
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::AlterCollection { name, operation },
            )));
        }
        if upper.starts_with("DESCRIBE ") && !upper.starts_with("DESCRIBE SEQUENCE") {
            let name = match parts.get(1) {
                None => return Ok(None),
                Some(s) => s.to_string(),
            };
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::DescribeCollection { name },
            )));
        }
        if upper == "\\D" || upper == "SHOW COLLECTIONS" || upper.starts_with("SHOW COLLECTIONS") {
            return Ok(Some(NodedbStatement::Collection(
                CollectionStmt::ShowCollections,
            )));
        }
        Ok(None)
    })()
    .transpose()
}
