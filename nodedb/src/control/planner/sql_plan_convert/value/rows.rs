// SPDX-License-Identifier: BUSL-1.1

//! Batch-of-rows msgpack encoding for columnar INSERT paths.

use nodedb_sql::types::SqlValue;

use super::convert::sql_value_to_nodedb_value;
use nodedb_sql::planner::defaults::evaluate_default_expr;

pub(crate) fn rows_to_msgpack_array(
    rows: &[&Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
) -> crate::Result<Vec<u8>> {
    let mut arr: Vec<nodedb_types::Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut map = std::collections::HashMap::new();
        for (key, val) in row.iter() {
            map.insert(key.clone(), sql_value_to_nodedb_value(val));
        }
        for (col_name, default_expr) in column_defaults {
            if !map.contains_key(col_name)
                && let Some(val) =
                    evaluate_default_expr(default_expr).map_err(|e| crate::Error::PlanError {
                        detail: format!("default for column '{col_name}': {e}"),
                    })?
            {
                map.insert(col_name.clone(), val);
            }
        }
        arr.push(nodedb_types::Value::Object(map));
    }
    let val = nodedb_types::Value::Array(arr);
    nodedb_types::value_to_msgpack(&val).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar row batch: {e}"),
    })
}
