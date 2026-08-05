// SPDX-License-Identifier: BUSL-1.1

//! Entry point: `copy_to_file`, path validation, scan, and atomic file write.
//!
//! Relocated verbatim from the pgwire `ddl::collection::copy_to::entry`
//! module (now deleted) except for the result type, which is [`DdlResult`] /
//! [`DdlError`] throughout instead of pgwire `Response` / `PgWireResult`.

use nodedb_types::DatabaseId;
use std::path::Path;

use sonic_rs;

use nodedb_sql::ddl_ast::statement::{CopyFormat, CopyToSource};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::redaction::RedactionStore;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_decoded_value};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::format::serialize_rows;

/// COPY TO format and serialization options.
#[derive(Clone, Copy, Debug)]
pub struct CopyToOptions<'a> {
    pub format: Option<&'a CopyFormat>,
    pub delimiter: Option<char>,
    pub header: bool,
}

/// Build a [`DdlError`] from an ANSI SQLSTATE code and a message.
fn ddl_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message: message.into(),
    }
}

/// Execute `COPY <source> TO '<path>' [WITH (...)]`.
pub async fn copy_to_file(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    source: &CopyToSource,
    path: &str,
    options: CopyToOptions<'_>,
    database_id: DatabaseId,
) -> Result<Vec<DdlResult>, DdlError> {
    let CopyToOptions {
        format,
        delimiter,
        header,
    } = options;

    validate_path(path)?;

    // Resolve format (caller has already auto-detected from extension).
    let resolved_format = format.ok_or_else(|| {
        ddl_err(
            "42601",
            format!(
                "COPY TO: cannot infer format for '{path}'; \
                 add WITH (FORMAT ndjson|json|csv)"
            ),
        )
    })?;

    // Build the SELECT SQL from the source.
    let select_sql = build_select_sql(source)?;

    // Validate collection existence (for table-form sources) and engine support.
    if let CopyToSource::Collection(coll) = source {
        check_collection_exists(state, identity, database_id, coll)?;
    }

    // Execute the query and collect all JSON rows.
    let rows = execute_and_collect(state, identity, database_id, &select_sql).await?;

    // Serialize to the requested format.
    let bytes = serialize_rows(&rows, resolved_format, delimiter.unwrap_or(','), header)?;

    // Atomic write: temp file → rename.
    let tmp_path = format!("{path}.tmp");
    tokio::fs::write(&tmp_path, &bytes).await.map_err(|e| {
        ddl_err(
            "58030",
            format!("COPY TO: cannot write to '{tmp_path}': {e}"),
        )
    })?;
    tokio::fs::rename(&tmp_path, path).await.map_err(|e| {
        // Clean up the temp file on rename failure.
        let _ = std::fs::remove_file(&tmp_path);
        ddl_err(
            "58030",
            format!("COPY TO: cannot rename '{tmp_path}' to '{path}': {e}"),
        )
    })?;

    let row_count = rows.len();
    Ok(vec![DdlResult::Status {
        command: format!("COPY {row_count}"),
        rows_affected: None,
    }])
}

/// Build a SELECT SQL string from the source.
fn build_select_sql(source: &CopyToSource) -> Result<String, DdlError> {
    match source {
        CopyToSource::Collection(coll) => Ok(format!(
            "SELECT * FROM {}",
            ::nodedb_types::quote_ident(coll)
        )),
        CopyToSource::Query(q) => Ok(q.clone()),
    }
}

/// Verify the named collection exists.
fn check_collection_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    match catalog.get_collection(database_id, identity.tenant_id.as_u64(), collection) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(ddl_err(
            "42P01",
            format!("COPY TO: collection \"{collection}\" does not exist"),
        )),
        Err(e) => Err(ddl_err(
            "XX000",
            format!("COPY TO: catalog lookup failed: {e}"),
        )),
    }
}

/// Execute the SELECT SQL and collect the results as `serde_json::Value` rows.
///
/// The exported rows are the query's result rows, so they carry exactly the
/// disclosure a `SELECT` of the same source would — and the same column
/// redaction applies. The dispatched payloads never reach the named-projection
/// shaping core (they are decoded and written to the file directly), so the
/// masking hook is applied here, once per export, at the same level the other
/// shape-bypassing transports apply it.
async fn execute_and_collect(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    select_sql: &str,
) -> Result<Vec<serde_json::Value>, DdlError> {
    let (tasks, _output_schema, _lease_scope) =
        crate::control::server::shared::ddl::neutral::planning::plan_authorized_sql(
            state,
            identity,
            select_sql,
            database_id,
        )
        .await
        .map_err(|error| DdlError {
            sqlstate: error.sqlstate,
            message: format!("COPY TO: {}", error.message),
        })?;

    let tasks = tasks.into_tasks();

    // Resolved once per export from the planned tasks' own plans, never per
    // task and never per row. Taking the sources from the plans rather than
    // from the `COPY` target covers the `COPY (<query>) TO` form too, whose
    // sources are whatever the query joins — the explicit target collection
    // only names the table form.
    //
    // The scope is rebuilt with the identical derivation
    // `plan_authorized_sql` used to plan and authorize these tasks
    // (`RequestAuthScope::for_database` over the same identity and database),
    // so the roles a row is redacted for cannot disagree with the roles it was
    // authorized for.
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
    let redaction = QueryRedaction::for_plans(
        identity.tenant_id,
        scope.auth(),
        tasks.iter().map(|task| task.plan()),
    );

    let mut all_rows: Vec<serde_json::Value> = Vec::new();

    for task in tasks {
        let resp = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            state,
            task,
            TraceId::ZERO,
        )
        .await
        .map_err(|e| ddl_err("XX000", format!("COPY TO: dispatch failed: {e}")))?;

        if resp.payload.is_empty() {
            continue;
        }

        let json = crate::data::executor::response_codec::decode_payload_to_json(&resp.payload);
        extract_json_rows(&json, &redaction, &state.redaction, &mut all_rows)?;
    }

    Ok(all_rows)
}

/// Parse a JSON string (array or single object), redact it, and append the
/// rows to `out`.
///
/// A dispatched scan decodes into one of the shapes
/// [`redact_decoded_value`] dispatches over — an array of `{id, data}`
/// document envelopes, an array of flat column maps (KV / columnar / aggregate
/// results), or a single object — so redaction is applied to the decoded value
/// as a whole, before it is split into rows and handed to the serializers.
fn extract_json_rows(
    json: &str,
    redaction: &QueryRedaction,
    store: &RedactionStore,
    out: &mut Vec<serde_json::Value>,
) -> Result<(), DdlError> {
    if json.is_empty() {
        return Ok(());
    }
    let mut parsed: serde_json::Value = sonic_rs::from_str(json).map_err(|e| {
        ddl_err(
            "XX000",
            format!("COPY TO: failed to decode result rows: {e}"),
        )
    })?;
    redact_decoded_value(Some(redaction), store, &mut parsed);
    match parsed {
        serde_json::Value::Array(items) => {
            out.extend(items);
        }
        obj @ serde_json::Value::Object(_) => {
            out.push(obj);
        }
        _ => {} // Scalar or null result — skip.
    }
    Ok(())
}

/// Reject paths with `..` segments and non-absolute paths.
fn validate_path(path: &str) -> Result<(), DdlError> {
    if !path.starts_with('/') {
        return Err(ddl_err(
            "42601",
            format!(
                "COPY TO: path '{path}' is not absolute; \
                 only absolute server-side paths are accepted"
            ),
        ));
    }
    let p = Path::new(path);
    for component in p.components() {
        use std::path::Component;
        if matches!(component, Component::ParentDir) {
            return Err(ddl_err(
                "42501",
                format!(
                    "COPY TO: path '{path}' contains '..'; \
                     directory traversal is not permitted"
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
    use crate::types::TenantId;

    use super::*;

    fn store_with_mask(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
        QueryRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            vec![(String::new(), collection.to_string())],
        )
    }

    fn collect(
        json: &str,
        redaction: &QueryRedaction,
        store: &RedactionStore,
    ) -> Vec<serde_json::Value> {
        let mut rows = Vec::new();
        extract_json_rows(json, redaction, store, &mut rows).expect("rows decode");
        rows
    }

    /// A document scan decodes into `{id, data}` envelopes (see the Data-Plane
    /// raw row encoder), so the ruled column lives one level down. Before the
    /// fix these bytes were written to the export file in the clear.
    #[test]
    fn exported_document_envelope_rows_are_masked() {
        let store = store_with_mask("users", "support", "email");
        let json = r#"[{"id":"1","data":{"email":"a@b.c","name":"Alice"}}]"#;

        let rows = collect(json, &redaction_for("users", "support"), &store);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["data"]["email"], "***");
        assert_eq!(rows[0]["data"]["name"], "Alice");
        assert_eq!(rows[0]["id"], "1");
    }

    /// A KV / columnar scan decodes into flat column maps instead — the same
    /// hook has to reach those without an envelope to unwrap.
    #[test]
    fn exported_flat_rows_are_masked() {
        let store = store_with_mask("users", "support", "email");
        let json = r#"[{"key":"k1","email":"a@b.c"}]"#;

        let rows = collect(json, &redaction_for("users", "support"), &store);

        assert_eq!(rows[0]["email"], "***");
        assert_eq!(rows[0]["key"], "k1");
    }

    /// A role the policy does not name exports the stored value.
    #[test]
    fn export_for_an_unruled_role_keeps_the_stored_value() {
        let store = store_with_mask("users", "support", "email");
        let json = r#"[{"id":"1","data":{"email":"a@b.c"}}]"#;

        let rows = collect(json, &redaction_for("users", "analyst"), &store);

        assert_eq!(rows[0]["data"]["email"], "a@b.c");
    }

    /// With no policy registered at all, the exported rows must be exactly
    /// what the payload decoded to.
    #[test]
    fn export_without_any_policy_is_unchanged() {
        let store = RedactionStore::new();
        let json = r#"[{"id":"1","data":{"email":"a@b.c"}},{"id":"2","data":{"email":"d@e.f"}}]"#;
        let expected: serde_json::Value = sonic_rs::from_str(json).expect("fixture parses");

        let rows = collect(json, &redaction_for("users", "support"), &store);

        assert_eq!(serde_json::Value::Array(rows), expected);
    }

    #[test]
    fn generated_collection_scan_quotes_identifier() {
        let sql = build_select_sql(&CopyToSource::Collection(
            "orders\"; DELETE FROM audit_log; --".to_string(),
        ))
        .expect("collection scan SQL builds");

        assert_eq!(
            sql,
            "SELECT * FROM \"orders\"\"; DELETE FROM audit_log; --\""
        );
    }

    #[test]
    fn query_source_is_not_reconstructed() {
        let query = "SELECT * FROM safe_source WHERE note = 'literal'".to_string();
        assert_eq!(
            build_select_sql(&CopyToSource::Query(query.clone())).expect("query SQL builds"),
            query
        );
    }
}
