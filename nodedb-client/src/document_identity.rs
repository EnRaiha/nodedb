// SPDX-License-Identifier: Apache-2.0

//! The identity column of a stored schemaless document.
//!
//! A collection's identity column is its declared primary key, else `id`.
//! The server catalog owns that resolution. Every write stores the document
//! id under the identity column:
//! - A native put sends the id beside the body. The server writes it under
//!   the identity column and refuses a body that names another id there.
//! - A pgwire put names the identity column in its `INSERT`. The client
//!   reads the column from `DESCRIBE`.
//!
//! A read returns the implicit `id` cell as `Document::id`, never as one of
//! `Document::fields`. A declared key column is a declared field: it reads
//! back as a field that holds the document id.

use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

/// `DESCRIBE` output column naming each field.
const DESCRIBE_FIELD_COLUMN: &str = "field";
/// `DESCRIBE` output column marking the key field.
const DESCRIBE_KEY_COLUMN: &str = "primary_key";

/// Whether a stored cell is the document's implicit identity cell.
///
/// An `id` cell that holds any other value is a field the writer set, and
/// stays a field.
pub(crate) fn is_identity_cell(name: &str, value: &Value, document_id: &str) -> bool {
    name == DEFAULT_IDENTITY_COLUMN && matches!(value, Value::String(s) if s == document_id)
}

/// The identity column named by a `DESCRIBE <collection>` result.
///
/// The result holds one row per field, with a `primary_key` cell marking the
/// key. `is_key` decodes that cell in the transport's own shape. Exactly one
/// row must be the key: none or several is an error naming the collection.
pub(crate) fn identity_column_from_describe(
    collection: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    is_key: fn(&Value) -> Option<bool>,
) -> NodeDbResult<String> {
    let index = |name: &str| {
        columns.iter().position(|c| c == name).ok_or_else(|| {
            describe_error(
                collection,
                format!("result has no '{name}' column; columns are {columns:?}"),
            )
        })
    };
    let field_idx = index(DESCRIBE_FIELD_COLUMN)?;
    let key_idx = index(DESCRIBE_KEY_COLUMN)?;
    let mut keys = Vec::new();
    for row in rows {
        let cell = row.get(key_idx).ok_or_else(|| {
            describe_error(collection, format!("row {row:?} has no primary_key cell"))
        })?;
        let marked = is_key(cell).ok_or_else(|| {
            describe_error(
                collection,
                format!("primary_key cell {cell:?} is not a bool"),
            )
        })?;
        if !marked {
            continue;
        }
        match row.get(field_idx) {
            Some(Value::String(field)) => keys.push(field.clone()),
            other => {
                return Err(describe_error(
                    collection,
                    format!("key row names no field: {other:?}"),
                ));
            }
        }
    }
    match keys.as_slice() {
        [key] => Ok(key.clone()),
        [] => Err(describe_error(collection, "no field is the primary key")),
        several => Err(describe_error(
            collection,
            format!("several fields are the primary key: {several:?}"),
        )),
    }
}

fn describe_error(collection: &str, detail: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization(
        "describe",
        format!("identity column of '{collection}': {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_bool(cell: &Value) -> Option<bool> {
        match cell {
            Value::String(s) if s == "t" => Some(true),
            Value::String(s) if s == "f" => Some(false),
            _ => None,
        }
    }

    fn describe(rows: &[(&str, &str)]) -> (Vec<String>, Vec<Vec<Value>>) {
        let columns = ["field", "type", "nullable", "primary_key"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let rows = rows
            .iter()
            .map(|(field, key)| {
                vec![
                    Value::String((*field).into()),
                    Value::String("TEXT".into()),
                    Value::String("true".into()),
                    Value::String((*key).into()),
                ]
            })
            .collect();
        (columns, rows)
    }

    #[test]
    fn only_the_matching_id_cell_is_the_identity() {
        assert!(is_identity_cell("id", &Value::String("d1".into()), "d1"));
        assert!(!is_identity_cell(
            "id",
            &Value::String("other".into()),
            "d1"
        ));
        assert!(!is_identity_cell("id", &Value::Integer(1), "d1"));
        assert!(!is_identity_cell("body", &Value::String("d1".into()), "d1"));
    }

    #[test]
    fn the_describe_key_row_names_the_identity_column() {
        let (columns, rows) = describe(&[("id", "f"), ("sku", "t"), ("name", "f")]);
        let column =
            identity_column_from_describe("c", &columns, &rows, text_bool).expect("one key row");
        assert_eq!(column, "sku");
    }

    #[test]
    fn a_describe_result_without_one_key_row_is_an_error() {
        let (columns, rows) = describe(&[("id", "f"), ("name", "f")]);
        let err =
            identity_column_from_describe("c", &columns, &rows, text_bool).expect_err("no key row");
        assert!(err.to_string().contains("'c'"), "{err}");

        let (columns, rows) = describe(&[("a", "t"), ("b", "t")]);
        assert!(identity_column_from_describe("c", &columns, &rows, text_bool).is_err());

        let (columns, rows) = describe(&[("a", "maybe")]);
        let err =
            identity_column_from_describe("c", &columns, &rows, text_bool).expect_err("not a bool");
        assert!(err.to_string().contains("not a bool"), "{err}");

        let err = identity_column_from_describe("c", &columns[..3], &rows, text_bool)
            .expect_err("no primary_key column");
        assert!(err.to_string().contains("'primary_key'"), "{err}");
    }
}
