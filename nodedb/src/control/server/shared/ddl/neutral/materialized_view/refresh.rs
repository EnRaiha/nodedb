// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `REFRESH MATERIALIZED VIEW` — re-materialize the view target
//! by executing the view's stored `SELECT` and writing each computed row to the
//! target collection.
//!
//! The refresh runs entirely in the Control Plane:
//!
//!   1. Plan the stored `SELECT` through `nodedb-sql`.
//!   2. Dispatch each produced `PhysicalTask` to the Data Plane and collect rows.
//!   3. Clear the target (`DELETE FROM <view>`).
//!   4. Write each collected row back with `INSERT INTO <view> (cols)
//!      VALUES (...)` through the same SQL pipeline.
//!
//! Decoupling the scan from the insert is what makes projection, `WHERE`,
//! `GROUP BY`/aggregates, and JOIN work uniformly — the Data Plane never needs a
//! specialised refresh opcode; every engine feature reachable by a normal
//! `SELECT` is reachable by refresh.
//!
//! Ported from the pgwire `ddl::materialized_view::refresh` handler. The plan /
//! Data-Plane dispatch path (`plan_sql`, `wal_append_if_write`,
//! `dispatch_to_data_plane`), the scan-row normalisation, and the INSERT
//! synthesis are preserved verbatim; only the result construction changed from
//! pgwire `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;
use crate::types::TraceId;

use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

pub async fn refresh_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 4 {
        return Err(err(
            "42601",
            "syntax: REFRESH MATERIALIZED VIEW <name>".to_string(),
        ));
    }

    let name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id;

    let view = if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_materialized_view(tenant_id.as_u64(), &name) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(err(
                    "42P01",
                    format!("materialized view '{name}' does not exist"),
                ));
            }
            Err(e) => return Err(err("XX000", e.to_string())),
        }
    } else {
        return Err(err("XX000", "catalog unavailable".to_string()));
    };

    // 1) Run the stored SELECT and collect every row.
    let rows = execute_select(state, tenant_id, &view.query_sql).await?;

    // 2) Clear the target so rows no longer selected by the SELECT
    //    (narrowed WHERE, dropped JOIN match, deleted source row,
    //    regrouped aggregate) disappear from the view.
    dispatch_sql(state, tenant_id, &format!("DELETE FROM {}", view.name)).await?;

    // 3) Re-insert every row produced by the SELECT.
    //
    //    Rows produced by aggregate/join paths do not carry a document
    //    `id`; the schemaless target collection needs a unique primary
    //    key per row, otherwise successive rows overwrite each other
    //    under the same default id. Synthesize a deterministic id from
    //    the row index when the SELECT output has no `id` column.
    for (idx, row) in rows.iter().enumerate() {
        let mut row = row.clone();
        if !row.contains_key("id") {
            row.insert(
                "id".to_string(),
                serde_json::Value::String(format!("mv_{idx}")),
            );
        }
        let insert_sql = build_insert_sql(&view.name, &row)?;
        dispatch_sql(state, tenant_id, &insert_sql).await?;
    }

    tracing::info!(
        view = view.name,
        rows = rows.len(),
        "materialized view refreshed"
    );

    Ok(vec![DdlResult::Status {
        command: "REFRESH MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}

/// Plan and execute a `SELECT` via the standard SQL pipeline, collect
/// the result rows as `serde_json::Map` objects. Response payloads may
/// come back as wrapped scan rows (`{id, data: {...}}`) or as flat
/// aggregate/join rows — both are normalised to the logical row map.
async fn execute_select(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    sql: &str,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, DdlError> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let (tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, crate::types::DatabaseId::DEFAULT)
        .await
        .map_err(|e| err("XX000", format!("plan '{sql}': {e}")))?;

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for task in tasks {
        let response = crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.database_id,
            task.vshard_id,
            task.plan,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| err("XX000", format!("dispatch: {e}")))?;

        let payload = response.payload.as_ref();
        if payload.is_empty() {
            continue;
        }
        let json = decode_payload_to_json(payload);
        if json.is_empty() {
            continue;
        }
        let parsed: serde_json::Value = sonic_rs::from_str(&json)
            .map_err(|e| err("XX000", format!("decode scan payload: {e}")))?;

        collect_rows(parsed, &mut rows);
    }
    Ok(rows)
}

/// Normalise a decoded response into row maps. Handles arrays of rows,
/// scan wrappers, and single-object responses uniformly.
fn collect_rows(
    value: serde_json::Value,
    out: &mut Vec<serde_json::Map<String, serde_json::Value>>,
) {
    match value {
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_rows(v, out);
            }
        }
        serde_json::Value::Object(mut obj) => {
            // Scan-path rows are wrapped `{id, data: {...}}`; we want the
            // `data` payload as the logical row. If `data` is itself an
            // object, unwrap; otherwise keep the outer map (aggregates,
            // joins emit flat row objects directly).
            if obj.len() == 2
                && obj.contains_key("id")
                && matches!(obj.get("data"), Some(serde_json::Value::Object(_)))
            {
                if let Some(serde_json::Value::Object(inner)) = obj.remove("data") {
                    out.push(inner);
                }
            } else {
                out.push(obj);
            }
        }
        _ => {}
    }
}

/// Build `INSERT INTO <target> (col1, col2, ...) VALUES (lit1, lit2, ...)`
/// from a row map. Preserves insertion order of JSON map keys.
fn build_insert_sql(
    target: &str,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, DdlError> {
    if row.is_empty() {
        return Err(err(
            "XX000",
            "materialized view SELECT produced an empty row (no columns)".to_string(),
        ));
    }
    let mut cols: Vec<String> = Vec::with_capacity(row.len());
    let mut vals: Vec<String> = Vec::with_capacity(row.len());
    for (k, v) in row {
        // Double-quote column identifiers so reserved words and names
        // with special characters survive the INSERT round-trip.
        cols.push(format!("\"{}\"", k.replace('"', "\"\"")));
        vals.push(json_value_to_sql_literal(v)?);
    }
    Ok(format!(
        "INSERT INTO {} ({}) VALUES ({})",
        target,
        cols.join(", "),
        vals.join(", ")
    ))
}

fn json_value_to_sql_literal(v: &serde_json::Value) -> Result<String, DdlError> {
    Ok(match v {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = sonic_rs::to_string(v)
                .map_err(|e| err("XX000", format!("encode nested value: {e}")))?;
            format!("'{}'", s.replace('\'', "''"))
        }
    })
}

async fn dispatch_sql(
    state: &SharedState,
    tenant_id: nodedb_types::TenantId,
    sql: &str,
) -> Result<(), DdlError> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let (tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, crate::types::DatabaseId::DEFAULT)
        .await
        .map_err(|e| err("42P20", format!("plan '{sql}': {e}")))?;
    for task in tasks {
        crate::control::server::wal_dispatch::wal_append_if_write(
            &state.wal,
            tenant_id,
            task.vshard_id,
            task.database_id,
            &task.plan,
        )
        .map_err(|e| err("58030", format!("wal append: {e}")))?;
        crate::control::server::dispatch_utils::dispatch_to_data_plane(
            state,
            tenant_id,
            task.database_id,
            task.vshard_id,
            task.plan,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| err("08006", format!("dispatch: {e}")))?;
    }
    Ok(())
}
