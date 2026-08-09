// SPDX-License-Identifier: BUSL-1.1

use nodedb_sql::types::{EngineType, SqlValue};
use nodedb_types::Surrogate;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ColumnarInsertIntent;
use nodedb_physical::physical_plan::*;

use super::super::convert::ConvertContext;
use super::super::value::{row_to_msgpack, rows_to_msgpack_array, sql_value_to_string};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Build a `ColumnarSchema` from raw catalog column-type strings.
///
/// `column_schema` is the list of `(column_name, type_str)` pairs from the
/// DDL catalog (`stored.fields`). Unknown type strings are treated as
/// `ColumnType::String` (matching the memtable's existing fallback).
///
/// The `id` column is treated as the primary key when present; all other
/// columns are treated as nullable.
///
/// Returns `None` when `column_schema` is empty (no catalog schema available
/// — test fixtures and legacy paths) or the resulting schema fails
/// validation.
///
/// This is the single source of truth for turning a catalog's raw
/// `(name, type_str)` field list into a typed `ColumnarSchema` — shared by
/// the live SQL insert path (via [`build_schema_bytes`]) and
/// `bootstrap::data_plane::load_columnar_schema_seed`, which pre-registers
/// each columnar-family collection's real schema before WAL replay so a
/// fresh `MutationEngine` never falls back to type-lossy inference.
pub(crate) fn build_columnar_schema(column_schema: &[(String, String)]) -> Option<ColumnarSchema> {
    if column_schema.is_empty() {
        return None;
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
    ColumnarSchema::new(cols).ok()
}

/// Build a `ColumnarSchema` from raw catalog column-type strings, then
/// serialize it as MessagePack for the `ColumnarOp::Insert::schema_bytes` field.
///
/// Returns an empty `Vec` when `column_schema` is empty or fails validation
/// — see [`build_columnar_schema`] for the typed builder this wraps.
pub(super) fn build_schema_bytes(column_schema: &[(String, String)]) -> Vec<u8> {
    build_columnar_schema(column_schema)
        .map(|schema| zerompk::to_msgpack_vec(&schema).unwrap_or_default())
        .unwrap_or_default()
}

/// Extract the document-id value from a row, keyed off the declared
/// `primary_key` column when present, falling back to the legacy
/// `id`/`document_id`/`key` convention otherwise.
pub(super) fn extract_doc_id(row: &[(String, SqlValue)], primary_key: Option<&str>) -> String {
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
    ctx.surrogate_for_pk(collection, pk_bytes)
}

/// Allocate a fresh, unique surrogate for a row whose primary key is the
/// auto-generated `_rowid` (no `PRIMARY KEY` declared). Content-addressing an
/// empty pk here would collapse every such row onto one surrogate — a
/// duplicate-key violation on the second insert.
pub(super) fn assign_fresh(ctx: &ConvertContext, collection: &str) -> crate::Result<Surrogate> {
    ctx.fresh_surrogate(collection)
}

/// Whether a collection's declared primary key is the auto-generated `_rowid`
/// sentinel — injected by strict-schema construction when no `PRIMARY KEY` was
/// declared. Such rows carry no user identity: each needs a fresh surrogate.
pub(super) fn is_auto_rowid_pk(primary_key: Option<&str>) -> bool {
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

/// Bundled arguments for [`convert_insert`].
pub(in super::super) struct ConvertInsertArgs<'a> {
    pub collection: &'a str,
    pub engine: &'a EngineType,
    pub rows: &'a [Vec<(String, SqlValue)>],
    pub column_defaults: &'a [(String, String)],
    pub column_schema: &'a [(String, String)],
    pub if_absent: bool,
    pub primary_key: Option<&'a str>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_insert(
    args: ConvertInsertArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertInsertArgs {
        collection,
        engine,
        rows,
        column_defaults,
        column_schema,
        if_absent,
        primary_key,
        tenant_id,
        ctx,
    } = args;
    let coll_qualified = super::super::convert::db_qualified(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    let mut tasks = Vec::new();
    let mut columnar_rows: Vec<&Vec<(String, SqlValue)>> = Vec::new();

    // Both INSERT routing gates, read from the catalog once for the whole
    // statement (never re-hit per row).
    //
    // `IF NOT EXISTS` (ON CONFLICT DO NOTHING → `if_absent`) cannot be honored by
    // `CrdtOp::DocUpsert`, which is an unconditional LWW full-replace: reject.
    let gates = super::balanced_gate::document_collection_write_gates(ctx, collection)?;
    let is_crdt = gates.crdt;
    if is_crdt && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on CRDT collection '{collection}' is not supported; \
                 CRDT documents converge via last-writer-wins full replace"
            ),
        });
    }

    // A balanced collection's rows are judged as a set, so the statement lowers
    // to ONE page rather than one task per row — see `balanced_gate`.
    let is_balanced = gates.balanced && !is_crdt;
    // `ON CONFLICT DO NOTHING` skips rows whose key already exists, and which
    // rows those are is decided per row at apply time. A journal that silently
    // loses one leg that way is exactly the unbalanced state the constraint
    // exists to refuse, and the page shape cannot express the per-row skip, so
    // the combination is rejected rather than half-honored.
    if is_balanced && if_absent {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "INSERT ... IF NOT EXISTS on BALANCED collection '{collection}' is not \
                 supported; a row skipped on conflict would leave its journal unbalanced"
            ),
        });
    }
    // Rows of a balanced INSERT, accumulated across the loop below and emitted
    // as one `BatchInsert` task after it.
    let mut balanced_documents: Vec<(String, Vec<u8>)> = Vec::new();
    let mut balanced_surrogates: Vec<Surrogate> = Vec::new();

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
                // Mint a fresh surrogate + document id when the row carries no
                // primary-key value: either an auto-`_rowid` collection (no
                // `PRIMARY KEY` declared) or an INSERT that simply omitted the
                // pk column. A content-addressed `assign` on the empty pk would
                // bind EVERY such row to one surrogate and one empty document
                // id, collapsing distinct id-less rows onto a single document
                // (each insert overwriting the last). The Data Plane sets the
                // row identity to this surrogate — matching the columnar path
                // (`columnar_row_surrogates`).
                let (doc_id, surrogate) = if is_auto_rowid_pk(primary_key) || doc_id.is_empty() {
                    let s = assign_fresh(ctx, collection)?;
                    (s.as_u32().to_string(), s)
                } else {
                    let s = assign_for_pk(ctx, collection, doc_id.as_bytes())?;
                    (doc_id, s)
                };
                // One page for the whole statement: the rows of a balanced
                // INSERT are judged together, so they may not be split across
                // one task — one boundary — per row.
                if is_balanced {
                    balanced_documents.push((doc_id, value_bytes));
                    balanced_surrogates.push(surrogate);
                    continue;
                }
                let plan = if is_crdt {
                    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                        collection: collection.into(),
                        document_id: doc_id,
                        fields_json: super::crdt_gate::row_to_fields_json(row)?,
                        surrogate,
                        partial: false,
                        returning: None,
                        rls_filters: Vec::new(),
                    })
                } else {
                    PhysicalPlan::Document(DocumentOp::PointInsert {
                        collection: collection.into(),
                        document_id: doc_id,
                        value: value_bytes,
                        if_absent,
                        surrogate,
                        // Both filled in after conversion: the RETURNING spec
                        // by the protocol layer's injection pass, the read
                        // filter by the RLS injection pass.
                        returning: None,
                        rls_filters: Vec::new(),
                        // Filled by the materialized-sum resolution pass,
                        // which runs after conversion (it needs the catalog
                        // and, in cluster mode, a routed lookup).
                        resolved_sum_targets: Vec::new(),
                        deferred_sum_targets: Vec::new(),
                    })
                };
                tasks.push(PhysicalTask {
                    tenant_id,
                    vshard_id: vshard,
                    database_id: ctx.database_id,
                    plan,
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

    if !balanced_documents.is_empty() {
        tasks.push(super::balanced_gate::balanced_batch_task(
            super::balanced_gate::BalancedBatch {
                collection,
                tenant_id,
                vshard,
                documents: balanced_documents,
                surrogates: balanced_surrogates,
            },
            ctx.database_id,
        ));
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
                rls_write_check: Vec::new(),
                // Both slots are filled by later passes over the built plan —
                // `inject_returning_spec` from the statement's RETURNING list,
                // and the row-level-security injector from the collection's read
                // policy. Filling either here would duplicate a decision that
                // has one owner.
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        });
    }

    Ok(tasks)
}
