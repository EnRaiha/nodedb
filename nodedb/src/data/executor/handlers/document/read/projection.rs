// SPDX-License-Identifier: BUSL-1.1

//! Projection and computed-column application for document scans.
//!
//! Two parallel paths must agree on missing-key semantics:
//! a projection key absent from the row maps to SQL NULL on **both** the JSON
//! and the msgpack code paths. Silently skipping a missing key drops the
//! column from the response and breaks the pgwire RowDescription contract.

use crate::bridge::expr_eval::ComputedColumn;

/// Apply projection or computed columns to a decoded document.
///
/// Missing projection keys are emitted as `Value::Null`, mirroring
/// [`apply_projection_msgpack`]. Earlier revisions silently skipped them,
/// which produced responses where window-function aliases (or any computed
/// alias not yet present on the row) disappeared from the output without an
/// error.
pub(in crate::data::executor) fn apply_projection(
    data: serde_json::Value,
    computed_cols: &[ComputedColumn],
    projection: &[String],
) -> crate::Result<serde_json::Value> {
    Ok(match data {
        serde_json::Value::Object(obj) => {
            if computed_cols.is_empty() && projection.is_empty() {
                return Ok(serde_json::Value::Object(obj));
            }

            let doc_val = nodedb_types::Value::from(serde_json::Value::Object(obj.clone()));
            let mut out = if projection.is_empty() {
                serde_json::Map::with_capacity(computed_cols.len())
            } else {
                let mut projected =
                    serde_json::Map::with_capacity(projection.len() + computed_cols.len());
                for col in projection {
                    let val = obj.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    projected.insert(col.clone(), val);
                }
                projected
            };

            for cc in computed_cols {
                let existing = out.get(&cc.alias);
                if matches!(existing, Some(v) if !v.is_null()) {
                    continue;
                }
                // A division/modulo-by-zero in a computed column fails the
                // whole query instead of silently materializing NULL into
                // the response.
                let v = cc.expr.eval(&doc_val)?;
                out.insert(cc.alias.clone(), serde_json::Value::from(v));
            }

            serde_json::Value::Object(out)
        }
        other => other,
    })
}

/// Apply projection and computed columns on raw msgpack bytes.
///
/// For projection-only (no computed columns), uses zero-decode binary field extraction.
/// For computed columns, decodes fields on-demand from msgpack.
pub(in crate::data::executor) fn apply_projection_msgpack(
    data: &[u8],
    computed_cols: &[ComputedColumn],
    projection: &[String],
) -> crate::Result<Vec<u8>> {
    if computed_cols.is_empty() && projection.is_empty() {
        return Ok(data.to_vec());
    }

    let field_count = if projection.is_empty() {
        computed_cols.len()
    } else {
        projection.len() + computed_cols.len()
    };

    let mut buf = Vec::with_capacity(data.len());
    nodedb_query::msgpack_scan::write_map_header(&mut buf, field_count);

    if !projection.is_empty() {
        for col in projection {
            nodedb_query::msgpack_scan::write_str(&mut buf, col);
            if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(data, 0, col) {
                buf.extend_from_slice(&data[start..end]);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    if !computed_cols.is_empty() {
        let doc_val = nodedb_types::value_from_msgpack(data).unwrap_or(nodedb_types::Value::Null);
        for cc in computed_cols {
            let already_present = projection.iter().any(|p| p == &cc.alias);
            if already_present {
                continue;
            }
            nodedb_query::msgpack_scan::write_str(&mut buf, &cc.alias);
            // A division/modulo-by-zero in a computed column fails the whole
            // query instead of silently materializing NULL into the
            // response.
            let result = cc.expr.eval(&doc_val)?;
            if let Ok(mp) = nodedb_types::value_to_msgpack(&result) {
                buf.extend_from_slice(&mp);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::SqlExpr;

    #[test]
    fn apply_projection_keeps_base_fields_when_computed_columns_exist() {
        let data = serde_json::json!({
            "id": "u1",
            "name": "Ada",
            "age": 42
        });
        let computed = vec![ComputedColumn {
            alias: "label".into(),
            expr: SqlExpr::Column("name".into()),
        }];
        let projection = vec!["name".to_string(), "age".to_string()];

        let projected = apply_projection(data, &computed, &projection).unwrap();

        assert_eq!(
            projected,
            serde_json::json!({
                "name": "Ada",
                "age": 42,
                "label": "Ada"
            })
        );
    }

    #[test]
    fn apply_projection_does_not_overwrite_existing_window_alias() {
        let data = serde_json::json!({
            "name": "Ada",
            "age": 42,
            "rn": 1
        });
        let computed = vec![ComputedColumn {
            alias: "rn".into(),
            expr: SqlExpr::Function {
                name: "row_number".into(),
                args: Vec::new(),
            },
        }];
        let projection = vec!["name".to_string(), "age".to_string(), "rn".to_string()];

        let projected = apply_projection(data, &computed, &projection).unwrap();

        assert_eq!(
            projected,
            serde_json::json!({
                "name": "Ada",
                "age": 42,
                "rn": 1
            })
        );
    }

    #[test]
    fn apply_projection_emits_null_for_missing_keys() {
        let data = serde_json::json!({
            "id": "u1",
            "score": 1.0
        });
        let projection = vec![
            "id".to_string(),
            "score".to_string(),
            "pr_score".to_string(),
        ];

        let projected = apply_projection(data, &[], &projection).unwrap();

        assert_eq!(
            projected,
            serde_json::json!({
                "id": "u1",
                "score": 1.0,
                "pr_score": serde_json::Value::Null,
            })
        );
    }

    /// A computed column that divides by zero fails the projection instead
    /// of silently materializing `NULL`.
    #[test]
    fn apply_projection_computed_column_division_by_zero_errors() {
        use crate::bridge::expr_eval::BinaryOp;
        let data = serde_json::json!({"denom": 0});
        let computed = vec![ComputedColumn {
            alias: "bad".into(),
            expr: SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Literal(nodedb_types::Value::Integer(10))),
                op: BinaryOp::Div,
                right: Box::new(SqlExpr::Column("denom".into())),
            },
        }];
        let err = apply_projection(data, &computed, &[]).unwrap_err();
        assert!(matches!(err, crate::Error::DivisionByZero), "got {err:?}");
    }
}
