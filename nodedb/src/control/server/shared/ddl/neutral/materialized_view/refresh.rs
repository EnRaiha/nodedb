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

use nodedb_types::DatabaseId;

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
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let name = parse_refresh_target(sql)?;
    let tenant_id = identity.tenant_id;

    let view = {
        let catalog = state.credentials.catalog();
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
    };

    // 1) Run the stored SELECT and collect every row.
    let rows = execute_select(state, identity, database_id, &view.query_sql).await?;

    // 2) Clear the target so rows no longer selected by the SELECT
    //    (narrowed WHERE, dropped JOIN match, deleted source row,
    //    regrouped aggregate) disappear from the view.
    dispatch_sql(
        state,
        identity,
        database_id,
        &format!("DELETE FROM {}", ::nodedb_types::quote_ident(&view.name)),
    )
    .await?;

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
        dispatch_sql(state, identity, database_id, &insert_sql).await?;
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
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, DdlError> {
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            sql,
            database_id,
        )
        .await
        .map_err(|error| DdlError {
            sqlstate: error.sqlstate,
            message: format!("plan '{sql}': {}", error.message),
        })?;

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
    for task in tasks.into_tasks() {
        let response = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            state,
            task,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| err("XX000", format!("dispatch: {e}")))?;
        require_ok_response(&response)?;

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
    let cols = row
        .keys()
        .map(String::as_str)
        .map(::nodedb_types::quote_ident)
        .collect::<Vec<_>>()
        .join(", ");
    let vals = row
        .values()
        .map(json_value_to_sql_literal)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "INSERT INTO {} ({cols}) VALUES ({vals})",
        ::nodedb_types::quote_ident(target)
    ))
}

fn json_value_to_sql_literal(v: &serde_json::Value) -> Result<String, DdlError> {
    Ok(match v {
        serde_json::Value::Null => "NULL".into(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.into(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => ::nodedb_types::quote_literal(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let s = sonic_rs::to_string(v)
                .map_err(|e| err("XX000", format!("encode nested value: {e}")))?;
            ::nodedb_types::quote_literal(&s)
        }
    })
}

async fn dispatch_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<(), DdlError> {
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            sql,
            database_id,
        )
        .await
        .map_err(|error| DdlError {
            sqlstate: error.sqlstate,
            message: format!("plan '{sql}': {}", error.message),
        })?;
    for task in tasks.into_tasks() {
        crate::control::server::wal_dispatch::wal_append_if_write(
            &state.wal,
            identity.tenant_id,
            task.vshard_id(),
            task.database_id(),
            task.plan(),
        )
        .map_err(|e| err("58030", format!("wal append: {e}")))?;
        let response = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            state,
            task,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| err("08006", format!("dispatch: {e}")))?;
        require_ok_response(&response)?;
    }
    Ok(())
}

fn require_ok_response(response: &crate::bridge::envelope::Response) -> Result<(), DdlError> {
    if response.status == crate::bridge::envelope::Status::Ok {
        return Ok(());
    }

    let detail = response.error_code.as_deref().map_or_else(
        || String::from_utf8_lossy(response.payload.as_ref()).into_owned(),
        |code| format!("{code:?}"),
    );
    Err(err(
        "XX000",
        format!("data-plane refresh task failed: {detail}"),
    ))
}

fn parse_refresh_target(sql: &str) -> Result<String, DdlError> {
    const PREFIX: &str = "REFRESH MATERIALIZED VIEW";
    let trimmed = sql.trim();
    let prefix = trimmed
        .get(..PREFIX.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(PREFIX))
        .ok_or_else(|| {
            err(
                "42601",
                "syntax: REFRESH MATERIALIZED VIEW <name>".to_string(),
            )
        })?;
    if prefix.len() == trimmed.len()
        || !trimmed[PREFIX.len()..]
            .chars()
            .next()
            .is_some_and(char::is_whitespace)
    {
        return Err(err(
            "42601",
            "syntax: REFRESH MATERIALIZED VIEW <name>".to_string(),
        ));
    }

    let (name, rest) = parse_identifier_token(trimmed[PREFIX.len()..].trim_start())?;
    let trailing = rest.trim();
    if !trailing.is_empty() && trailing != ";" {
        return Err(err(
            "42601",
            "unexpected trailing tokens after materialized view name".to_string(),
        ));
    }
    Ok(name)
}

fn parse_identifier_token(input: &str) -> Result<(String, &str), DdlError> {
    if input.is_empty() {
        return Err(err("42601", "missing materialized view name".to_string()));
    }
    if let Some(mut rest) = input.strip_prefix('"') {
        let mut value = String::new();
        loop {
            let Some(ch) = rest.chars().next() else {
                return Err(err("42601", "unterminated quoted identifier".to_string()));
            };
            rest = &rest[ch.len_utf8()..];
            if ch == '"' {
                if let Some(next) = rest.strip_prefix('"') {
                    value.push('"');
                    rest = next;
                    continue;
                }
                if value.chars().any(char::is_control) {
                    return Err(err(
                        "42601",
                        "identifier contains a control character".to_string(),
                    ));
                }
                return Ok((value, rest));
            }
            value.push(ch);
        }
    }

    let end = input
        .char_indices()
        .find_map(|(index, ch)| (!is_bare_identifier_char(ch)).then_some(index))
        .unwrap_or(input.len());
    let name = &input[..end];
    if name.is_empty()
        || !name
            .chars()
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_alphabetic())
    {
        return Err(err("42601", "invalid materialized view name".to_string()));
    }
    Ok((name.to_lowercase(), &input[end..]))
}

fn is_bare_identifier_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_alphanumeric()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_insert_quotes_target_columns_and_literals() {
        let mut row = serde_json::Map::new();
        row.insert(
            "column\"; DELETE FROM audit_log; --".to_string(),
            serde_json::Value::String("value'; DELETE FROM audit_log; --".to_string()),
        );
        row.insert(
            "nested".to_string(),
            serde_json::json!({"name": "O'Reilly"}),
        );

        let sql = build_insert_sql("view\"; DROP TABLE audit_log; --", &row)
            .expect("generated INSERT is valid");

        assert_eq!(
            sql,
            "INSERT INTO \"view\"\"; DROP TABLE audit_log; --\" (\"column\"\"; DELETE FROM audit_log; --\", \"nested\") VALUES ('value''; DELETE FROM audit_log; --', '{\"name\":\"O''Reilly\"}')"
        );
    }

    #[test]
    fn generated_literals_preserve_non_string_sql_types() {
        assert_eq!(
            json_value_to_sql_literal(&serde_json::Value::Null).expect("null literal"),
            "NULL"
        );
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!(true)).expect("boolean literal"),
            "TRUE"
        );
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!(42)).expect("number literal"),
            "42"
        );
    }

    #[test]
    fn refresh_target_parses_quoted_and_bare_identifiers() {
        assert_eq!(
            parse_refresh_target("REFRESH MATERIALIZED VIEW \"Sales View\"").expect("quoted"),
            "Sales View"
        );
        assert_eq!(
            parse_refresh_target(" refresh materialized view \"a\"\"b\"; ").expect("escaped"),
            "a\"b"
        );
        assert_eq!(
            parse_refresh_target("REFRESH MATERIALIZED VIEW MiXeD_Name").expect("bare"),
            "mixed_name"
        );
    }

    #[test]
    fn refresh_target_rejects_malformed_or_trailing_input() {
        for sql in [
            "REFRESH MATERIALIZED VIEW",
            "REFRESH MATERIALIZED VIEW \"unterminated",
            "REFRESH MATERIALIZED VIEW view extra",
            "REFRESH MATERIALIZED VIEW view;;",
        ] {
            assert!(parse_refresh_target(sql).is_err(), "{sql}");
        }
    }
}
