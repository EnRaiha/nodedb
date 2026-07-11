// SPDX-License-Identifier: BUSL-1.1

//! Shared parsing, SQL-building, and plan-dispatch helpers for the
//! protocol-neutral INSERT/UPSERT DML handlers.
//!
//! Relocated verbatim from the pgwire `ddl::collection::insert_parse` module
//! (now deleted) except for the result type, which is [`DdlError`] /
//! [`DdlResult`] instead of pgwire `Response` / `PgWireResult`.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sql_parse::{parse_sql_value, split_values};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    DmlTxnCtx, InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::TraceId;

/// Parsed INSERT/UPSERT statement fields.
pub(super) struct ParsedInsert {
    pub coll_name: String,
    pub doc_id: String,
    pub fields: std::collections::HashMap<String, nodedb_types::Value>,
    pub has_returning: bool,
    /// Collection type looked up from the catalog. Drives the write plan.
    pub collection_type: Option<nodedb_types::CollectionType>,
}

pub(super) fn extract_vector_fields(
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Vec<(String, Vec<f32>)> {
    fields
        .iter()
        .filter_map(|(field_name, value)| match value {
            nodedb_types::Value::Array(items) => {
                let vector: Vec<f32> = items
                    .iter()
                    .map(|item| match item {
                        nodedb_types::Value::Float(v) => Some(*v as f32),
                        nodedb_types::Value::Integer(v) => Some(*v as f32),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some((field_name.clone(), vector))
            }
            _ => None,
        })
        .collect()
}

/// Parse an INSERT/UPSERT SQL statement into structured fields.
///
/// `keyword` is the SQL prefix to match (e.g., "INSERT INTO " or "UPSERT INTO ").
/// Returns `None` if the collection has a typed schema (let the SQL path handle it).
pub(super) fn parse_write_statement(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    keyword: &str,
) -> Option<Result<ParsedInsert, DdlError>> {
    let upper = sql.to_uppercase();
    let kw_pos = upper.find(keyword)?;
    let after_into = sql[kw_pos + keyword.len()..].trim_start();
    let coll_name_str = after_into.split_whitespace().next()?;
    let coll_name = coll_name_str.to_lowercase();

    // Check if collection is schemaless. Let the SQL path handle typed INSERT
    // with VALUES syntax, but always handle here for pre-write concerns:
    // - UPSERT (triggers + nodedb-sql handles the routing)
    // - { } object literal syntax (triggers + nodedb-sql handles the routing)
    let tenant_id = identity.tenant_id;
    let is_upsert = keyword.starts_with("UPSERT");
    let after_coll_trimmed = after_into[coll_name_str.len()..].trim_start();
    let is_object_literal =
        after_coll_trimmed.starts_with('{') || after_coll_trimmed.starts_with('[');
    let mut coll_type: Option<nodedb_types::CollectionType> = None;
    let catalog = state.credentials.catalog();
    if let Ok(Some(coll)) =
        catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &coll_name)
    {
        // Skip non-schemaless collections for standard VALUES INSERT (let SQL path handle).
        // But always handle here for: UPSERT, { } object literal (any collection type).
        if !is_upsert && !is_object_literal && !coll.collection_type.is_schemaless() {
            return None;
        }
        coll_type = Some(coll.collection_type.clone());
    }

    // Determine which form this statement uses: { } object literal or (cols) VALUES (vals).
    // If { }, rewrite to VALUES SQL via nodedb-sql's preprocess, then parse that.
    let after_coll_name = after_into[coll_name_str.len()..].trim_start();

    if after_coll_name.starts_with('{') || after_coll_name.starts_with('[') {
        if let Ok(Some(preprocessed)) = nodedb_sql::parser::preprocess::preprocess(sql) {
            let rewritten = preprocessed.sql;
            let rewritten_upper = rewritten.to_uppercase();
            // The preprocessed SQL is always INSERT INTO regardless of original keyword.
            return parse_values_form(
                &rewritten,
                &rewritten_upper,
                "INSERT INTO ",
                &coll_name,
                coll_type,
            );
        }
        return Some(Err(ddl_err(
            "42601",
            "failed to parse object literal in INSERT/UPSERT statement",
        )));
    }

    parse_values_form(sql, &upper, keyword, &coll_name, coll_type)
}

/// Parse the `(cols) VALUES (vals)` form.
fn parse_values_form(
    sql: &str,
    upper: &str,
    keyword: &str,
    coll_name: &str,
    coll_type: Option<nodedb_types::CollectionType>,
) -> Option<Result<ParsedInsert, DdlError>> {
    let first_open = match sql.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err(
                "42601",
                format!("missing column list in {}", keyword.trim()),
            )));
        }
    };
    let values_kw = match upper.find("VALUES") {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err("42601", "missing VALUES clause")));
        }
    };
    let first_close = match sql[first_open..values_kw].rfind(')') {
        Some(p) => first_open + p,
        None => {
            return Some(Err(ddl_err("42601", "missing closing ) for column list")));
        }
    };
    let cols_str = &sql[first_open + 1..first_close];
    let columns: Vec<&str> = cols_str.split(',').map(|c| c.trim()).collect();

    let after_values = sql[values_kw + 6..].trim_start();
    let vals_open = match after_values.find('(') {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err("42601", "missing VALUES (...)")));
        }
    };
    let vals_close = match after_values.rfind(')') {
        Some(p) => p,
        None => {
            return Some(Err(ddl_err("42601", "missing closing ) for VALUES")));
        }
    };
    let vals_str = &after_values[vals_open + 1..vals_close];
    let values: Vec<&str> = split_values(vals_str);

    if columns.len() != values.len() {
        return Some(Err(ddl_err(
            "42601",
            format!(
                "column count ({}) doesn't match value count ({})",
                columns.len(),
                values.len()
            ),
        )));
    }

    let mut doc_id = String::new();
    let mut fields = std::collections::HashMap::new();

    for (col, val) in columns.iter().zip(values.iter()) {
        let col = col.trim().trim_matches('"');
        let val = val.trim();
        if col.eq_ignore_ascii_case("id")
            || col.eq_ignore_ascii_case("document_id")
            || col.eq_ignore_ascii_case("key")
        {
            doc_id = val.trim_matches('\'').to_string();
        }
        fields.insert(col.to_string(), parse_sql_value(val));
    }

    if doc_id.is_empty() {
        doc_id = nodedb_types::id_gen::uuid_v7();
    }

    let has_returning = upper.contains("RETURNING");

    Some(Ok(ParsedInsert {
        coll_name: coll_name.to_string(),
        doc_id,
        fields,
        has_returning,
        collection_type: coll_type,
    }))
}

/// Format a RETURNING response from parsed fields.
pub(super) fn returning_response(
    doc_id: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> Result<Vec<DdlResult>, DdlError> {
    use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
    use serde_json::{Map, Value as JsonValue};

    let mut result_doc = fields.clone();
    result_doc.insert(
        "id".to_string(),
        nodedb_types::Value::String(doc_id.to_string()),
    );
    let json_str =
        sonic_rs::to_string(&nodedb_types::Value::Object(result_doc)).unwrap_or_default();

    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String(json_str));

    Ok(vec![DdlResult::Rows(ShapedRows {
        columns: vec!["result".to_string()],
        column_types: vec![DdlColType::Text],
        rows: vec![row],
        notice: None,
    })])
}

/// Dispatch a plan to WAL + Data Plane, returning an error response on failure.
pub(super) async fn dispatch_plan(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // The WAL append is performed inside the dispatch core, under the
    // write-admission guard and just before the enqueue, so LSN order matches
    // apply order.
    if let Err(e) = crate::control::server::dispatch_utils::dispatch_autocommit_write(
        state,
        crate::control::server::dispatch_utils::AutocommitWrite {
            tenant_id,
            database_id: crate::types::DatabaseId::DEFAULT,
            vshard_id,
            plan,
            trace_id: TraceId::ZERO,
            event_source: crate::event::EventSource::User,
            txn_id: None,
        },
    )
    .await
    {
        return Some(Err(ddl_err("XX000", e.to_string())));
    }
    None
}

/// Quote a user-supplied map key for safe inclusion as a SQL column
/// identifier. Object-literal keys come from untrusted client payloads;
/// concatenating them raw into generated SQL allows a crafted key to
/// close the column list and inject arbitrary statements. Double-quoted
/// identifiers are the SQL standard form; embedded double quotes are
/// escaped by doubling.
fn quote_column_identifier(key: &str) -> String {
    format!("\"{}\"", key.replace('"', "\"\""))
}

/// Build a SQL INSERT statement from field map.
///
/// Produces `INSERT INTO coll ("col1", "col2") VALUES ('val1', 'val2')`.
/// Column identifiers are double-quoted so that map keys containing
/// punctuation, whitespace, or SQL syntax are treated as a single
/// identifier by the downstream parser instead of fragmenting the
/// statement.
pub(in crate::control::server::shared::ddl::neutral::collection) fn fields_to_insert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());

    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    for (key, value) in entries {
        cols.push(quote_column_identifier(key));
        vals.push(value_to_sql_literal(value));
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        collection,
        cols.join(", "),
        vals.join(", ")
    )
}

/// Build a SQL UPSERT statement from field map. See
/// `fields_to_insert_sql` for the identifier quoting rationale.
pub(super) fn fields_to_upsert_sql(
    collection: &str,
    fields: &std::collections::HashMap<String, nodedb_types::Value>,
) -> String {
    let mut cols = Vec::with_capacity(fields.len());
    let mut vals = Vec::with_capacity(fields.len());

    let mut entries: Vec<_> = fields.iter().collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    for (key, value) in entries {
        cols.push(quote_column_identifier(key));
        vals.push(value_to_sql_literal(value));
    }

    format!(
        "UPSERT INTO {} ({}) VALUES ({})",
        collection,
        cols.join(", "),
        vals.join(", ")
    )
}

/// Delegate to the shared implementation in nodedb-sql.
fn value_to_sql_literal(value: &nodedb_types::Value) -> String {
    nodedb_sql::parser::preprocess::value_to_sql_literal(value)
}

/// Plan SQL through nodedb-sql and dispatch the resulting physical plans.
///
/// This is the shared path: SQL → nodedb-sql → EngineRules → SqlPlan → PhysicalPlan → dispatch.
/// Returns `Ok(())` on success, or a protocol-neutral error on failure.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<(), DdlError> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let (mut tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, database_id)
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // Schemaless INSERT / UPSERT / object-literal documents carrying `_from`
    // / `_to` mirror an implicit graph edge. Extract it here — the same as the
    // main SQL and native surfaces — so the edge routes through the shared
    // dispatch path (Calvin dual-home cross-shard, single-home otherwise)
    // instead of the removed Data-Plane hook.
    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // A cross-shard write (e.g. a doc + its dual-homed implicit edge, or a
    // multi-row insert spanning vShards) must commit atomically through the
    // Calvin sequencer when one is wired (cluster). Single-node has no sequencer,
    // so fall through to the per-task local dispatch below — every vShard is
    // local there, so a single-home edge is still reachable by forward traversal.
    if state.sequencer_inbox.get().is_some()
        && matches!(
            crate::control::planner::calvin::classify_dispatch(&tasks),
            crate::control::planner::calvin::DispatchClass::MultiShard { .. }
        )
    {
        crate::control::planner::calvin::dispatch_tasks_to_calvin(
            state,
            &tasks,
            tenant_id,
            crate::control::planner::calvin::CrossShardTxnMode::Strict,
            false,
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
        return Ok(());
    }

    for task in tasks {
        let task_vshard_id = task.vshard_id;
        let task_database_id = task.database_id;

        let routed = route_in_tx_write(txn_ctx.sessions, txn_ctx.addr, task, |staged| {
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                staged.tenant_id,
                staged.database_id,
                staged.vshard_id,
                staged.plan,
                TraceId::ZERO,
                staged.txn_id,
            )
        })
        .await;

        let task = match routed {
            Ok(InTxnRoute::Read(task)) => *task,
            Ok(InTxnRoute::Buffered) => continue,
            // Staged into the per-transaction overlay + buffered for COMMIT
            // replay; `plan_and_dispatch` returns `()` on success so the
            // affected count has no return path here (its callers already
            // build their own "OK" result independent of row counts).
            Ok(InTxnRoute::Staged(_outcome)) => continue,
            Err(StagingGateError::Dispatch(e)) => {
                return Err(ddl_err("XX000", e.to_string()));
            }
            Err(StagingGateError::Rejected { code }) => {
                let (_, sqlstate, message) = match code {
                    Some(code) => error_code_to_sqlstate(&code),
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                return Err(ddl_err(sqlstate, message));
            }
        };

        // Not in a transaction block (or a read): the immediate autocommit path.
        // The WAL append is performed inside the dispatch core, under the
        // write-admission guard and just before the enqueue, so LSN order matches
        // apply order.
        let response = crate::control::server::dispatch_utils::dispatch_autocommit_write(
            state,
            crate::control::server::dispatch_utils::AutocommitWrite {
                tenant_id,
                database_id: task_database_id,
                vshard_id: task_vshard_id,
                plan: task.plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
            },
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;

        // Data Plane returns `Ok(Response { status: Error, .. })` for
        // constraint violations (UNIQUE, CHECK-at-write, etc.). Surface
        // them as errors — without this mapping the Data Plane's
        // rejection would be silently swallowed and the write appears
        // to succeed at the SQL layer.
        if response.status == crate::bridge::envelope::Status::Error {
            let detail = match &response.error_code {
                Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
                Some(other) => format!("{other:?}"),
                None => String::from_utf8_lossy(&response.payload).into_owned(),
            };
            let sqlstate = if detail.to_lowercase().contains("unique") {
                "23505"
            } else {
                "XX000"
            };
            return Err(ddl_err(sqlstate, detail));
        }
    }
    Ok(())
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    //! Object-literal rewrite contract tests.
    //!
    //! Map keys originate from untrusted client input. The SQL builders
    //! MUST either reject / quote unsafe identifiers and MUST canonicalize
    //! non-finite floats before concatenation. These tests drive the
    //! helpers directly to pin the contract; integration tests would
    //! mask the bug behind accidental downstream parser rejection.
    use super::*;
    use std::collections::HashMap;

    fn one_field(key: &str, value: nodedb_types::Value) -> HashMap<String, nodedb_types::Value> {
        let mut m = HashMap::new();
        m.insert("id".into(), nodedb_types::Value::String("r1".into()));
        m.insert(key.into(), value);
        m
    }

    #[test]
    fn upsert_sql_quotes_injection_key_as_single_identifier() {
        // Spec: a key containing SQL syntax characters must appear only
        // inside a double-quoted identifier, never as bare tokens. The
        // test payload contains `);` which, if unquoted, would close
        // the column list and start a new statement.
        let fields = one_field(
            "a); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(1),
        );
        let sql = fields_to_upsert_sql("t", &fields);
        let quoted = "\"a); DROP COLLECTION other; --\"";
        assert!(
            sql.contains(quoted),
            "expected injection key to be safely double-quoted; got {sql}"
        );
        // Regression guard on the specific exploit: no bare `DROP` token
        // appears outside the quoted identifier window.
        let parts: Vec<&str> = sql.split(quoted).collect();
        for p in &parts {
            assert!(
                !p.contains("DROP"),
                "unquoted DROP token leaked outside identifier: {sql}"
            );
        }
    }

    #[test]
    fn insert_sql_quotes_injection_key_as_single_identifier() {
        let fields = one_field(
            "b); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(2),
        );
        let sql = fields_to_insert_sql("t", &fields);
        let quoted = "\"b); DROP COLLECTION other; --\"";
        assert!(
            sql.contains(quoted),
            "expected injection key to be safely double-quoted; got {sql}"
        );
        let parts: Vec<&str> = sql.split(quoted).collect();
        for p in &parts {
            assert!(
                !p.contains("DROP"),
                "unquoted DROP token leaked outside identifier: {sql}"
            );
        }
    }

    #[test]
    fn upsert_sql_escapes_embedded_double_quote_in_key() {
        // Spec: a key containing `"` must be escaped by doubling, so the
        // quoted-identifier form stays closed correctly. Otherwise the
        // key could still terminate the identifier and inject syntax.
        let fields = one_field(
            "a\"); DROP COLLECTION other; --",
            nodedb_types::Value::Integer(1),
        );
        let sql = fields_to_upsert_sql("t", &fields);
        let escaped = "\"a\"\"); DROP COLLECTION other; --\"";
        assert!(
            sql.contains(escaped),
            "embedded `\"` must be doubled; got {sql}"
        );
    }

    #[test]
    fn upsert_sql_rejects_or_canonicalizes_nan_float() {
        // Spec: `Value::Float(NaN)` must not reach the generated SQL as
        // the bare token `NaN`. `format!("{f}")` currently emits `NaN`
        // verbatim, which the planner parses as an identifier reference.
        let mut fields = HashMap::new();
        fields.insert("id".into(), nodedb_types::Value::String("r1".into()));
        fields.insert("score".into(), nodedb_types::Value::Float(f64::NAN));
        let sql = fields_to_upsert_sql("t", &fields);
        assert!(
            !sql.contains("NaN"),
            "non-finite float leaked as bare `NaN` token: {sql}"
        );
    }

    #[test]
    fn upsert_sql_rejects_or_canonicalizes_infinity_float() {
        let mut fields = HashMap::new();
        fields.insert("id".into(), nodedb_types::Value::String("r1".into()));
        fields.insert("score".into(), nodedb_types::Value::Float(f64::INFINITY));
        let sql = fields_to_upsert_sql("t", &fields);
        assert!(
            !sql.contains("inf"),
            "non-finite float leaked as bare `inf` token: {sql}"
        );
    }
}
