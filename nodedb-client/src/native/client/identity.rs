// SPDX-License-Identifier: Apache-2.0

//! The identity column of a collection, read from the server catalog.
//!
//! A search restricted to allowed ids names the key column in its SQL: the
//! declared primary key, else `id`. The client reads it from `DESCRIBE` on
//! every call that needs it, so a dropped and recreated collection never
//! answers with a stale key.

use nodedb_types::error::NodeDbResult;
use nodedb_types::value::Value;

use crate::document_identity::identity_column_from_describe;
use crate::sql_escape::quote_identifier;

use super::core::NativeClient;

impl NativeClient {
    /// The identity column of `collection`.
    pub(super) async fn identity_column(&self, collection: &str) -> NodeDbResult<String> {
        let sql = format!("DESCRIBE {}", quote_identifier(collection));
        let result = self.query(&sql).await?;
        identity_column_from_describe(collection, &result.columns, &result.rows, typed_bool)
    }
}

/// A `bool` cell as the native protocol carries it: a typed `Bool`.
fn typed_bool(cell: &Value) -> Option<bool> {
    match cell {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_typed_bools_decode() {
        assert_eq!(typed_bool(&Value::Bool(true)), Some(true));
        assert_eq!(typed_bool(&Value::Bool(false)), Some(false));
        assert_eq!(typed_bool(&Value::String("t".into())), None);
    }
}
