// SPDX-License-Identifier: BUSL-1.1

//! Batch-of-rows msgpack encoding for columnar INSERT paths.

use nodedb_sql::types::SqlValue;

use super::super::convert::ConvertContext;
use super::convert::sql_value_to_nodedb_value;
use super::expand_row_defaults;

pub(crate) fn rows_to_msgpack_array(
    rows: &[&Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    ctx: &ConvertContext,
) -> crate::Result<Vec<u8>> {
    let mut arr: Vec<nodedb_types::Value> = Vec::with_capacity(rows.len());
    for row in rows {
        // The one shared per-row default expander (sequence accessors via
        // the CP registry, stateless via the pure evaluator) — same code as
        // the INSERT/UPSERT document-family paths, so no engine path can
        // swallow a declared default to NULL (#294).
        let expanded = expand_row_defaults(ctx, row, column_defaults)?;
        let mut map = std::collections::HashMap::new();
        for (key, val) in expanded.iter() {
            map.insert(key.clone(), sql_value_to_nodedb_value(val));
        }
        arr.push(nodedb_types::Value::Object(map));
    }
    let val = nodedb_types::Value::Array(arr);
    nodedb_types::value_to_msgpack(&val).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar row batch: {e}"),
    })
}
