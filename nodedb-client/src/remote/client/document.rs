// SPDX-License-Identifier: Apache-2.0

//! Document operation implementations for `NodeDbRemote`.
//!
//! A document's fields are the row's top-level columns, the shape the native
//! client and Lite store. The collection's identity column holds the
//! document id.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use crate::document_identity::is_identity_cell;
use crate::remote_parse::json_to_value;
use crate::sql_escape::{quote_identifier, quote_string_literal};

use super::core::NodeDbRemote;

/// Output column holding the whole row as one JSON object.
const DOCUMENT_COLUMN: &str = "document";

impl NodeDbRemote {
    /// Read every top-level field of the row whose identity column holds
    /// `id`, each with its stored type.
    ///
    /// `to_jsonb(*)` returns the whole row as one JSON object. Each field
    /// decodes to the `Value` kind the native client returns. A key lookup
    /// stays a point read: the server evaluates the item on the one row the
    /// lookup returns.
    pub(super) async fn document_get_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<Option<Document>> {
        let key = self.identity_column(collection).await?;
        let sql = format!(
            "SELECT to_jsonb(*) AS {DOCUMENT_COLUMN} FROM {} WHERE {} = {}",
            quote_identifier(collection),
            quote_identifier(&key),
            quote_string_literal(id)
        );
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        document_from_rows(collection, id, &columns, rows)
    }

    pub(super) async fn document_delete_impl(
        &self,
        collection: &str,
        id: &str,
    ) -> NodeDbResult<()> {
        let key = self.identity_column(collection).await?;
        let sql = format!(
            "DELETE FROM {} WHERE {} = $1",
            quote_identifier(collection),
            quote_identifier(&key)
        );
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}

/// The document a `to_jsonb(*)` point read answered with, or `None` when no
/// row matched.
///
/// The one cell is the JSON text of the row object. More than one row, a
/// missing column, or a cell that is not a JSON object is an error naming
/// the document.
fn document_from_rows(
    collection: &str,
    id: &str,
    columns: &[String],
    rows: Vec<Vec<Value>>,
) -> NodeDbResult<Option<Document>> {
    let mut rows = rows.into_iter();
    let Some(row) = rows.next() else {
        return Ok(None);
    };
    let extra = rows.count();
    if extra > 0 {
        return Err(malformed(
            collection,
            id,
            format!("expected at most one row, got {}", extra + 1),
        ));
    }
    let index = columns
        .iter()
        .position(|c| c == DOCUMENT_COLUMN)
        .ok_or_else(|| {
            malformed(
                collection,
                id,
                format!("no '{DOCUMENT_COLUMN}' column; columns are {columns:?}"),
            )
        })?;
    let text = match row.into_iter().nth(index) {
        Some(Value::String(text)) => text,
        other => {
            return Err(malformed(
                collection,
                id,
                format!("'{DOCUMENT_COLUMN}' cell is not JSON text: {other:?}"),
            ));
        }
    };
    let json: serde_json::Value = sonic_rs::from_str(&text)
        .map_err(|e| malformed(collection, id, format!("row JSON does not parse: {e}")))?;
    let serde_json::Value::Object(fields) = json else {
        return Err(malformed(
            collection,
            id,
            format!("row JSON is not an object: {text}"),
        ));
    };

    let mut doc = Document::new(id);
    for (name, field) in &fields {
        let value = json_to_value(field)
            .map_err(|e| malformed(collection, id, format!("field '{name}': {e}")))?;
        if !is_identity_cell(name, &value, id) {
            doc.set(name.clone(), value);
        }
    }
    Ok(Some(doc))
}

fn malformed(collection: &str, id: &str, detail: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization(
        "pgwire",
        format!("document_get '{collection}'/'{id}': {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(cells: Vec<Vec<Value>>) -> NodeDbResult<Option<Document>> {
        document_from_rows("docs", "d1", &[DOCUMENT_COLUMN.to_string()], cells)
    }

    fn json_cell(text: &str) -> Vec<Vec<Value>> {
        vec![vec![Value::String(text.into())]]
    }

    #[test]
    fn every_field_keeps_its_type() {
        let doc = read(json_cell(
            r#"{"id":"d1","count":5,"ratio":1.5,"whole":2.0,"active":true,"name":"n",
                "tags":["a",2],"missing":null,"meta":{"k":"v"}}"#,
        ))
        .expect("a well-formed row parses")
        .expect("a row is a document");
        assert_eq!(doc.id, "d1");
        assert_eq!(doc.get("count"), Some(&Value::Integer(5)));
        assert_eq!(doc.get("ratio"), Some(&Value::Float(1.5)));
        assert_eq!(doc.get("whole"), Some(&Value::Float(2.0)));
        assert_eq!(doc.get("active"), Some(&Value::Bool(true)));
        assert_eq!(doc.get("name"), Some(&Value::String("n".into())));
        assert_eq!(
            doc.get("tags"),
            Some(&Value::Array(vec![
                Value::String("a".into()),
                Value::Integer(2)
            ]))
        );
        assert_eq!(doc.get("missing"), Some(&Value::Null));
        assert!(matches!(doc.get("meta"), Some(Value::Object(_))));
        assert_eq!(doc.get("id"), None, "the identity cell is not a field");
    }

    #[test]
    fn no_row_is_no_document() {
        assert_eq!(read(Vec::new()).expect("a miss is not a fault"), None);
    }

    #[test]
    fn a_cell_that_is_not_a_json_object_is_an_error() {
        let err = read(json_cell("[1,2]")).expect_err("an array is not a row");
        assert!(err.to_string().contains("not an object"), "{err}");
        let err = read(vec![vec![Value::Null]]).expect_err("a NULL cell is not a row");
        assert!(err.to_string().contains("not JSON text"), "{err}");
        let err = read(json_cell("{")).expect_err("broken JSON");
        assert!(err.to_string().contains("does not parse"), "{err}");
    }

    #[test]
    fn several_rows_are_an_error() {
        let err = read(vec![
            vec![Value::String("{}".into())],
            vec![Value::String("{}".into())],
        ])
        .expect_err("a point read never picks one of several rows");
        assert!(err.to_string().contains("one row"), "{err}");
    }
}
