// SPDX-License-Identifier: BUSL-1.1

//! Schema inference, field coercion, bitemporal column injection, and
//! schema-ordered row <-> object conversion.

use nodedb_types::columnar::schema::{TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Ensure a `MutationEngine` is registered for `engine_key`, creating an
    /// empty one (schema resolved from `schema_bytes`, falling back to
    /// inference from `first_row`) when absent, then return its schema.
    ///
    /// Shared by the durable insert path (`execute_columnar_insert`) and the
    /// in-transaction staging path (`stage_columnar_insert`) so a staged
    /// `INSERT` into a collection with no prior durable write registers the
    /// SAME schema a same-transaction `SELECT` will find — without that, an
    /// in-transaction `INSERT` immediately followed by a `SELECT` on a
    /// brand-new collection would hit `execute_columnar_scan`'s "missing
    /// engine -> empty result" branch and never see the staged row, breaking
    /// read-your-own-writes for the first insert into a collection.
    ///
    /// Creating the engine here (rather than only at COMMIT) is safe on
    /// ROLLBACK: an empty, zero-row `MutationEngine` is indistinguishable
    /// from "not yet created" for every read path, and the durable insert
    /// path already treats engine creation as idempotent
    /// (`if !self.columnar_engines.contains_key(...)`).
    pub(in crate::data::executor) fn ensure_columnar_engine_schema(
        &mut self,
        engine_key: &(DatabaseId, TenantId, String),
        collection: &str,
        bitemporal: bool,
        first_row: &Value,
        schema_bytes: &[u8],
    ) -> ColumnarSchema {
        if let Some(engine) = self.columnar_engines.get(engine_key) {
            return engine.schema().clone();
        }
        let flush_threshold = self.query_tuning.columnar_flush_threshold;
        let engine = self
            .columnar_engines
            .entry(engine_key.clone())
            .or_insert_with(|| {
                let base_schema = if !schema_bytes.is_empty() {
                    zerompk::from_msgpack::<ColumnarSchema>(schema_bytes)
                        .unwrap_or_else(|_| infer_schema_from_value(first_row))
                } else {
                    infer_schema_from_value(first_row)
                };
                let schema = if bitemporal {
                    prepend_bitemporal_columns(base_schema)
                } else {
                    base_schema
                };
                nodedb_columnar::MutationEngine::with_flush_threshold(
                    collection.to_string(),
                    schema,
                    flush_threshold,
                )
            });
        engine.schema().clone()
    }
}

/// Build a `nodedb_types::Value::Object` from a schema-ordered row. Used
/// by the ON CONFLICT DO UPDATE path to present `existing` and `EXCLUDED`
/// rows to `apply_on_conflict_updates` in the same shape the document
/// upsert path uses, and by the row-level-security write gate to present the
/// row a statement is about to persist or remove to a policy predicate.
pub(in crate::data::executor) fn row_values_to_object(
    schema: &ColumnarSchema,
    row: &[Value],
) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col, val) in schema.columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), val.clone());
    }
    nodedb_types::Value::Object(map)
}

/// Coerce a `nodedb_types::Value` field to match the column type.
///
/// Returns `Err` if a millisecond timestamp value overflows `i64` microseconds.
pub(in crate::data::executor) fn ndb_field_to_value(
    val: Option<&Value>,
    col_type: &ColumnType,
) -> crate::Result<Value> {
    let Some(val) = val else {
        return Ok(Value::Null);
    };
    let v = match (col_type, val) {
        (_, Value::Null) => Value::Null,
        (ColumnType::Int64, Value::Integer(_)) => val.clone(),
        (ColumnType::Int64, Value::Float(f)) => Value::Integer(*f as i64),
        (ColumnType::Int64, Value::String(s)) => {
            s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
        }
        (ColumnType::Float64, Value::Float(_)) => val.clone(),
        (ColumnType::Float64, Value::Integer(n)) => Value::Float(*n as f64),
        (ColumnType::Float64, Value::String(s)) => {
            s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        (ColumnType::Bool, Value::Bool(_)) => val.clone(),
        (ColumnType::String, Value::String(_)) => val.clone(),
        (ColumnType::Timestamp, Value::Integer(n)) => {
            Value::NaiveDateTime(nodedb_types::NdbDateTime::from_millis(*n).map_err(|e| {
                crate::Error::BadRequest {
                    detail: format!("timestamp coercion: {e}"),
                }
            })?)
        }
        (ColumnType::Timestamp, Value::Float(f)) => {
            Value::NaiveDateTime(nodedb_types::NdbDateTime::from_millis(*f as i64).map_err(
                |e| crate::Error::BadRequest {
                    detail: format!("timestamp coercion: {e}"),
                },
            )?)
        }
        (ColumnType::Timestamp, Value::String(s)) => nodedb_types::datetime::NdbDateTime::parse(s)
            .map(Value::NaiveDateTime)
            .unwrap_or_else(|| Value::String(s.clone())),
        (ColumnType::Timestamptz, Value::Integer(n)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*n).map_err(|e| {
                crate::Error::BadRequest {
                    detail: format!("timestamptz coercion: {e}"),
                }
            })?)
        }
        (ColumnType::Timestamptz, Value::Float(f)) => Value::DateTime(
            nodedb_types::NdbDateTime::from_millis(*f as i64).map_err(|e| {
                crate::Error::BadRequest {
                    detail: format!("timestamptz coercion: {e}"),
                }
            })?,
        ),
        (ColumnType::Timestamptz, Value::String(s)) => {
            nodedb_types::datetime::NdbDateTime::parse(s)
                .map(Value::DateTime)
                .unwrap_or_else(|| Value::String(s.clone()))
        }
        (ColumnType::Uuid, Value::String(_)) => val.clone(),
        // Fallback: integers as floats, strings as strings.
        (ColumnType::Float64, _) => Value::Null,
        (ColumnType::Int64, _) => Value::Null,
        _ => val.clone(),
    };
    Ok(v)
}

/// Infer a columnar schema from a `nodedb_types::Value::Object` (first row).
///
/// Last resort only: reached when the collection has no catalog schema to send
/// (`schema_bytes` empty) — a test fixture, or a WAL redo record replayed
/// before the boot-time schema seed. Types come from the row's own values;
/// a column's *name* says nothing about its type, so a column called
/// `timestamp` holding a string is a string column.
pub(in crate::data::executor) fn infer_schema_from_value(row: &Value) -> ColumnarSchema {
    let obj = match row {
        Value::Object(m) => m,
        _ => {
            return ColumnarSchema::new(vec![ColumnDef::required("value", ColumnType::Float64)])
                .expect("single-column schema");
        }
    };

    let mut columns = Vec::new();
    for (key, val) in obj {
        let col_type = match val {
            Value::Float(_) => ColumnType::Float64,
            Value::Integer(_) => ColumnType::Int64,
            Value::Bool(_) => ColumnType::Bool,
            Value::DateTime(_) => ColumnType::Timestamptz,
            Value::NaiveDateTime(_) => ColumnType::Timestamp,
            _ => ColumnType::String,
        };
        let lower = key.to_lowercase();
        if lower == "id" {
            columns.push(ColumnDef::required(key.clone(), col_type).with_primary_key());
        } else {
            columns.push(ColumnDef::nullable(key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(ColumnDef::required("value", ColumnType::Float64));
    }

    ColumnarSchema::new(columns).expect("inferred schema must be valid")
}

/// Prepend the three reserved bitemporal columns (`_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until`) at positions 0/1/2 of a columnar
/// schema. All three are required Int64; `_ts_system` is engine-stamped
/// on every write, the valid-time pair is client-provided (or defaults
/// to the open interval).
pub(in crate::data::executor) fn prepend_bitemporal_columns(
    base: ColumnarSchema,
) -> ColumnarSchema {
    let mut cols = Vec::with_capacity(3 + base.columns.len());
    cols.push(ColumnDef::required(TS_SYSTEM, ColumnType::Int64));
    cols.push(ColumnDef::required(TS_VALID_FROM, ColumnType::Int64));
    cols.push(ColumnDef::required(TS_VALID_UNTIL, ColumnType::Int64));
    cols.extend(base.columns);
    ColumnarSchema::new(cols).expect("bitemporal columnar schema must be valid")
}

/// Infer a columnar schema from a JSON object — used by the spatial insert path.
pub(super) fn infer_schema_from_json(row: &serde_json::Value) -> ColumnarSchema {
    let ndb: Value = row.clone().into();
    infer_schema_from_value(&ndb)
}
