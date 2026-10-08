// SPDX-License-Identifier: Apache-2.0

//! The identity column of a collection, read from the server catalog.
//!
//! The SQL the remote client generates names the key column: the declared
//! primary key, else `id`. The client reads it from `DESCRIBE` on every
//! call that needs it, so a dropped and recreated collection never answers
//! with a stale key.

use nodedb_types::error::NodeDbResult;
use nodedb_types::value::Value;

use crate::document_identity::identity_column_from_describe;
use crate::sql_escape::quote_identifier;

use super::core::NodeDbRemote;

impl NodeDbRemote {
    /// The identity column of `collection`.
    pub(super) async fn identity_column(&self, collection: &str) -> NodeDbResult<String> {
        let sql = format!("DESCRIBE {}", quote_identifier(collection));
        let (columns, rows) = self.simple_query_raw(&sql).await?;
        identity_column_from_describe(collection, &columns, &rows, pg_text_bool)
    }
}

/// A `bool` cell in the PostgreSQL text format the simple-query protocol
/// carries: `t` or `f`.
fn pg_text_bool(cell: &Value) -> Option<bool> {
    match cell {
        Value::String(s) if s == "t" => Some(true),
        Value::String(s) if s == "f" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_pg_text_bools_decode() {
        assert_eq!(pg_text_bool(&Value::String("t".into())), Some(true));
        assert_eq!(pg_text_bool(&Value::String("f".into())), Some(false));
        assert_eq!(pg_text_bool(&Value::String("true".into())), None);
        assert_eq!(pg_text_bool(&Value::Bool(true)), None);
    }
}
