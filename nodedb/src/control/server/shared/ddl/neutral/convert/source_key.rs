// SPDX-License-Identifier: BUSL-1.1

//! The source collection's primary key, carried onto a converted schema.
//!
//! A conversion keeps every row's client-visible identity, so the converted
//! schema is keyed by the column the source keyed its rows by.

use nodedb_types::columnar::{ColumnDef, ColumnType};
use nodedb_types::{CollectionType, DEFAULT_IDENTITY_COLUMN, DocumentMode, ROWID_COLUMN};

use super::super::super::result::DdlError;
use super::support::err;

/// The key column of a KV collection whose schema marks none.
const KV_KEY_COLUMN: &str = "key";

/// The column a source row's client-visible identity lives in.
///
/// A strict collection keys by its schema's primary key, which is the system
/// `_rowid` when none was declared. A KV collection keys by its schema's
/// primary key, else `key`. A schemaless collection keys by its declared
/// primary key, else `id`.
pub(super) fn source_key(collection_type: &CollectionType, declared: Option<&str>) -> String {
    let schema_key = |columns: &[ColumnDef]| {
        columns
            .iter()
            .find(|c| c.primary_key)
            .map(|c| c.name.clone())
    };
    match collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            schema_key(&schema.columns).unwrap_or_else(|| ROWID_COLUMN.to_string())
        }
        CollectionType::KeyValue(config) => {
            schema_key(&config.schema.columns).unwrap_or_else(|| KV_KEY_COLUMN.to_string())
        }
        CollectionType::Document(DocumentMode::Schemaless) | CollectionType::Columnar(_) => {
            declared.unwrap_or(DEFAULT_IDENTITY_COLUMN).to_string()
        }
    }
}

/// Key the converted schema by `key`, so every row keeps its identity.
///
/// `explicit` is whether the author wrote the column list. A written list must
/// name the key and must not key on another column. A system row id carries no
/// client identity, so a list may omit it or key on a column of its own. A
/// schema derived from typeguards gains the key column when no guard names it.
///
/// Both refusals raise SQLSTATE `42601`, naming the source key.
pub(super) fn bind_source_key(
    columns: &mut Vec<ColumnDef>,
    key: &str,
    explicit: bool,
) -> Result<(), DdlError> {
    if let Some(other) = columns.iter().find(|c| c.primary_key && c.name != key) {
        if key == ROWID_COLUMN {
            return Ok(());
        }
        return Err(err(
            "42601",
            format!(
                "CONVERT keeps the source primary key '{key}'; the column list keys on '{}'",
                other.name
            ),
        ));
    }
    if let Some(column) = columns.iter_mut().find(|c| c.name == key) {
        column.primary_key = true;
        column.nullable = false;
        return Ok(());
    }
    if explicit && key != ROWID_COLUMN {
        return Err(err(
            "42601",
            format!("CONVERT keeps the source primary key '{key}'; add '{key}' to the column list"),
        ));
    }
    let key_type = if key == ROWID_COLUMN {
        ColumnType::Int64
    } else {
        ColumnType::String
    };
    columns.insert(0, ColumnDef::required(key, key_type).with_primary_key());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(names: &[&str]) -> Vec<ColumnDef> {
        names
            .iter()
            .map(|name| ColumnDef::nullable(*name, ColumnType::String))
            .collect()
    }

    /// A list that names the key gets it marked as the primary key.
    #[test]
    fn a_listed_key_column_becomes_the_primary_key() {
        let mut cols = columns(&["sku", "name"]);
        bind_source_key(&mut cols, "sku", true).expect("the list names the key");
        assert!(cols[0].primary_key);
        assert!(!cols[0].nullable);
        assert!(!cols[1].primary_key);
    }

    /// A written list that omits the source key is refused, naming the key.
    #[test]
    fn a_written_list_without_the_key_is_refused() {
        let mut cols = columns(&["name"]);
        let error = bind_source_key(&mut cols, "sku", true).expect_err("the key is missing");
        assert_eq!(error.sqlstate, "42601");
        assert!(error.message.contains("'sku'"), "{}", error.message);
    }

    /// A schema derived from typeguards gains the key column.
    #[test]
    fn a_derived_schema_gains_the_key_column() {
        let mut cols = columns(&["name"]);
        bind_source_key(&mut cols, "id", false).expect("the key is prepended");
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].primary_key);
    }

    /// A written list that keys on another column is refused.
    #[test]
    fn a_written_list_that_rekeys_is_refused() {
        let mut cols = columns(&["sku", "name"]);
        cols[1].primary_key = true;
        let error = bind_source_key(&mut cols, "sku", true).expect_err("re-keying is refused");
        assert_eq!(error.sqlstate, "42601");
    }

    /// A system row id may be omitted, and the converted schema keeps one.
    #[test]
    fn a_rowid_source_may_omit_the_key() {
        let mut cols = columns(&["name"]);
        bind_source_key(&mut cols, ROWID_COLUMN, true).expect("_rowid may be omitted");
        assert_eq!(cols[0].name, ROWID_COLUMN);
        assert_eq!(cols[0].column_type, ColumnType::Int64);
        assert!(cols[0].primary_key);
    }

    /// A system row id source may key on a column of the list.
    #[test]
    fn a_rowid_source_may_rekey() {
        let mut cols = columns(&["sku", "name"]);
        cols[0].primary_key = true;
        bind_source_key(&mut cols, ROWID_COLUMN, true).expect("a declared key replaces _rowid");
        assert_eq!(cols.len(), 2);
    }

    /// Each source type names the column its rows are keyed by.
    #[test]
    fn source_key_follows_the_source_type() {
        assert_eq!(source_key(&CollectionType::document(), None), "id");
        assert_eq!(source_key(&CollectionType::document(), Some("sku")), "sku");
    }
}
