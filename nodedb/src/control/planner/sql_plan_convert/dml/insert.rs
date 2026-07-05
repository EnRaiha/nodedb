// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{EngineType, SqlExpr, SqlValue};
use nodedb_types::Surrogate;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{
    assignments_to_update_values, row_to_msgpack, rows_to_msgpack_array, sql_value_to_string,
};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// The `id` column is treated as the primary key when present; all other
/// columns are treated as nullable.
///
/// Returns an empty `Vec` when `column_schema` is empty (no catalog schema
/// available — test fixtures and legacy paths).
fn build_schema_bytes(column_schema: &[(String, String)]) -> Vec<u8> {
    if column_schema.is_empty() {
        return Vec::new();
    }
    let mut cols = Vec::with_capacity(column_schema.len());
    let mut has_id = false;
    for (name, type_str) in column_schema {
        // `type_str` may contain SQL modifiers such as `NOT NULL` or `PRIMARY KEY`
        // (e.g. "BIGINT NOT NULL"). Strip everything after the first token so that
        // `ColumnType::from_str` receives the bare type name (e.g. "BIGINT").
        let bare_type = type_str
            .split_whitespace()
            .next()
            .unwrap_or(type_str.as_str());
        let col_type = bare_type
            .parse::<ColumnType>()
            .unwrap_or(ColumnType::String);
        let is_id = name == "id" || name == "document_id";
        if is_id {
            has_id = true;
            cols.push(ColumnDef::required(name.clone(), col_type).with_primary_key());
        } else {
            cols.push(ColumnDef::nullable(name.clone(), col_type));
        }
    }
    // If no PK column found in stored.fields, inject a synthetic one.
    if !has_id {
        cols.insert(
            0,
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
        );
    }
    let schema = match ColumnarSchema::new(cols) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    zerompk::to_msgpack_vec(&schema).unwrap_or_default()
}

/// Extract the document-id value from a row, keyed off the declared
/// `primary_key` column when present, falling back to the legacy
/// `id`/`document_id`/`key` convention otherwise.
fn extract_doc_id(row: &[(String, SqlValue)], primary_key: Option<&str>) -> String {
    row.iter()
        .find(|(k, _)| match primary_key {
            Some(pk) => k == pk,
            None => k == "id" || k == "document_id" || k == "key",
        })
        .map(|(_, v)| sql_value_to_string(v))
        .unwrap_or_default()
}

pub(super) fn assign_for_pk(
    ctx: &ConvertContext,
    collection: &str,
    pk_bytes: &[u8],
) -> crate::Result<Surrogate> {
    match ctx.surrogate_assigner.as_ref() {
        Some(a) => a.assign(ctx.database_id, ctx.tenant_id, collection, pk_bytes),
        None => Ok(Surrogate::ZERO),
    }
}

/// Allocate a fresh, unique surrogate for a row whose primary key is the
/// auto-generated `_rowid` (no `PRIMARY KEY` declared). Content-addressing an
/// empty pk here would collapse every such row onto one surrogate — a
/// duplicate-key violation on the second insert.
pub(super) fn assign_fresh(ctx: &ConvertContext, collection: &str) -> crate::Result<Surrogate> {
    match ctx.surrogate_assigner.as_ref() {
        Some(a) => a.assign_fresh(ctx.database_id, ctx.tenant_id, collection),
        None => Ok(Surrogate::ZERO),
    }
}

/// Whether a collection's declared primary key is the auto-generated `_rowid`
/// sentinel — injected by strict-schema construction when no `PRIMARY KEY` was
/// declared. Such rows carry no user identity: each needs a fresh surrogate.
fn is_auto_rowid_pk(primary_key: Option<&str>) -> bool {
    primary_key == Some("_rowid")
}

/// Mirrors the document-engine identity path (`extract_doc_id` +
/// `is_auto_rowid_pk` + `assign_fresh` / `assign_for_pk`) for
/// columnar/spatial rows. The declared `primary_key` — not the legacy
/// `id`/`document_id`/`key` name guess — determines each row's identity, so
/// a natural key on any column (e.g. `sku`) gets its own surrogate. A
/// missing/empty key mints a fresh unique surrogate rather than collapsing
/// onto `Surrogate::ZERO`, which would silently merge distinct rows.
pub(super) fn columnar_row_surrogates(
    ctx: &ConvertContext,
    collection: &str,
    columnar_rows: &[&Vec<(String, SqlValue)>],
    primary_key: Option<&str>,
) -> crate::Result<Vec<Surrogate>> {
    let mut out = Vec::with_capacity(columnar_rows.len());
    for row in columnar_rows {
        if is_auto_rowid_pk(primary_key) {
            out.push(assign_fresh(ctx, collection)?);
            continue;
        }
        let pk = extract_doc_id(row, primary_key);
        if pk.is_empty() {
            out.push(assign_fresh(ctx, collection)?);
        } else {
            out.push(assign_for_pk(ctx, collection, pk.as_bytes())?);
        }
    }
    Ok(out)
}

pub(in super::super) fn nodedb_value_to_sql(val: nodedb_types::Value) -> SqlValue {
    match val {
        nodedb_types::Value::Integer(n) => SqlValue::Int(n),
        nodedb_types::Value::Float(f) => SqlValue::Float(f),
        nodedb_types::Value::String(s) => SqlValue::String(s),
        nodedb_types::Value::Bool(b) => SqlValue::Bool(b),
        nodedb_types::Value::Null => SqlValue::Null,
        _ => SqlValue::String(format!("{val:?}")),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_insert(
    collection: &str,
    engine: &EngineType,
    rows: &[Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    column_schema: &[(String, String)],
    if_absent: bool,
    primary_key: Option<&str>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    let mut expanded_rows: Vec<Vec<(String, SqlValue)>> = Vec::with_capacity(rows.len());
    for row in rows {
        if column_defaults.is_empty() {
            expanded_rows.push(row.clone());
            continue;
        }
        let mut expanded = row.clone();
        for (col_name, default_expr) in column_defaults {
            if !expanded.iter().any(|(k, _)| k == col_name)
                && let Some(val) = super::super::value::evaluate_default_expr(default_expr)
                    .map_err(|e| crate::Error::PlanError {
                        detail: format!("default for column '{col_name}': {e}"),
                    })?
            {
                expanded.push((col_name.clone(), nodedb_value_to_sql(val)));
            }
        }
        expanded_rows.push(expanded);
    }

    for (i, row) in expanded_rows.iter().enumerate() {
        let doc_id = extract_doc_id(row, primary_key);

        match engine {
            EngineType::KeyValue => {
                return Err(crate::Error::PlanError {
                    detail: "KV INSERT must use SqlPlan::KvInsert path".into(),
                });
            }
            EngineType::Timeseries => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "INSERT into '{collection}': timeseries collections use TimeseriesIngest, not Insert"
                    ),
                });
            }
            EngineType::Columnar | EngineType::Spatial => {
                columnar_rows.push(&rows[i]);
            }
            EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
                let value_bytes = row_to_msgpack(row)?;
                // Auto-`_rowid` collections carry no user identity: mint a fresh
                // surrogate per row and use it as the document id (the Data
                // Plane sets `_rowid` to the same value). A content-addressed
                // assign on the empty pk would give every row one surrogate.
                let (doc_id, surrogate) = if is_auto_rowid_pk(primary_key) {
                    let s = assign_fresh(ctx, collection)?;
                    (s.as_u32().to_string(), s)
                } else {
                    let s = assign_for_pk(ctx, collection, doc_id.as_bytes())?;
                    (doc_id, s)
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    database_id: ctx.database_id,
                    plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                        if_absent,
                        surrogate,
                    }),
                    post_set_op: PostSetOp::None,
                    txn_id: None,
                });
            }
            EngineType::Array => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "INSERT into '{collection}': array engine uses INSERT INTO ARRAY syntax"
                    ),
                });
            }
        }
    }

    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows, column_defaults)?;
        let intent = if if_absent {
            ColumnarInsertIntent::InsertIfAbsent
        } else {
            ColumnarInsertIntent::Insert
        };
        let surrogates = columnar_row_surrogates(ctx, collection, &columnar_rows, primary_key)?;
        let schema_bytes = build_schema_bytes(column_schema);
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.into(),
                payload,
                format: "msgpack".into(),
                intent,
                on_conflict_updates: Vec::new(),
                surrogates,
                schema_bytes,
                provenance: None,
                wal_lsn: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn convert_upsert(
    collection: &str,
    engine: &EngineType,
    rows: &[Vec<(String, SqlValue)>],
    column_defaults: &[(String, String)],
    column_schema: &[(String, String)],
    on_conflict_updates: &[(String, SqlExpr)],
    primary_key: Option<&str>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();

    let on_conflict_values = if on_conflict_updates.is_empty() {
        Vec::new()
    } else {
        assignments_to_update_values(on_conflict_updates)?
    };

    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    for row in rows {
        let doc_id = extract_doc_id(row, primary_key);

        match engine {
            EngineType::DocumentSchemaless | EngineType::DocumentStrict => {
                let value_bytes = row_to_msgpack(row)?;
                // Auto-`_rowid` collections have no user identity to match on,
                // so an upsert degenerates to an insert with a fresh surrogate;
                // the on-conflict clause can never match a prior row.
                let (doc_id, surrogate) = if is_auto_rowid_pk(primary_key) {
                    let s = assign_fresh(ctx, collection)?;
                    (s.as_u32().to_string(), s)
                } else {
                    let s = assign_for_pk(ctx, collection, doc_id.as_bytes())?;
                    (doc_id, s)
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    database_id: ctx.database_id,
                    plan: PhysicalPlan::Document(DocumentOp::Upsert {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                        on_conflict_updates: on_conflict_values.clone(),
                        surrogate,
                    }),
                    post_set_op: PostSetOp::None,
                    txn_id: None,
                });
            }
            EngineType::Columnar | EngineType::Spatial => {
                columnar_rows.push(row);
            }
            EngineType::Timeseries | EngineType::KeyValue | EngineType::Array => {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "UPSERT into '{collection}': engine type {engine:?} does not support upsert"
                    ),
                });
            }
        }
    }

    if !columnar_rows.is_empty() {
        let payload = rows_to_msgpack_array(&columnar_rows, column_defaults)?;
        let surrogates = columnar_row_surrogates(ctx, collection, &columnar_rows, primary_key)?;
        let schema_bytes = build_schema_bytes(column_schema);
        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.into(),
                payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Put,
                on_conflict_updates: on_conflict_values,
                surrogates,
                schema_bytes,
                provenance: None,
                wal_lsn: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}
