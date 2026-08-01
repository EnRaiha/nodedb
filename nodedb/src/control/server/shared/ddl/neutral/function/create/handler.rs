// SPDX-License-Identifier: BUSL-1.1

//! The protocol-neutral `create_function` handler.
//!
//! Ported from the pgwire `ddl::function::create` handler. All non-return logic
//! (privilege gate, parsing, body compilation/validation, StoredFunction build,
//! catalog propose-and-apply, dependency extraction into the replicated
//! definition, Lite definition-sync broadcast, and the `audit_record` call)
//! is preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::catalog::StoredFunction;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::catalog::propose_and_apply;
use crate::control::server::shared::ddl::neutral::auth_support::{require_tenant_admin, status};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::super::validate::validate_function_body;
use super::deps::extract_dependencies;
use super::parse::{ParsedCreateFunction, parse_create_function};

/// Handle `CREATE [OR REPLACE] FUNCTION <name>(<params>) RETURNS
/// <type> [IMMUTABLE|STABLE|VOLATILE] AS <body>`.
///
/// Requires superuser or tenant_admin — function bodies are SQL
/// expressions that can reference any collection, so creation is
/// a privileged operation.
pub fn create_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create functions")?;

    let parsed = parse_create_function(sql)?;
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    if !parsed.or_replace
        && let Ok(Some(_)) = catalog.get_function_in_database(database_id, tenant_id, &parsed.name)
    {
        return Err(DdlError {
            sqlstate: "42723".to_string(),
            message: format!("function '{}' already exists", parsed.name),
        });
    }

    // Detect body kind and compile/validate accordingly.
    use crate::control::planner::procedural::ast::BodyKind;
    let compiled_body_sql =
        match BodyKind::detect(&parsed.body_sql) {
            BodyKind::Expression => {
                validate_function_body(&parsed)?;
                None
            }
            BodyKind::Procedural => {
                let block = crate::control::planner::procedural::parse_block(&parsed.body_sql)
                    .map_err(|e| DdlError {
                        sqlstate: "42601".to_string(),
                        message: format!("procedural parse error: {e}"),
                    })?;

                crate::control::planner::procedural::validate_function_block(&block).map_err(
                    |e| DdlError {
                        sqlstate: "42601".to_string(),
                        message: format!("procedural validation: {e}"),
                    },
                )?;

                let compiled = crate::control::planner::procedural::compile_to_sql(&block)
                    .map_err(|e| DdlError {
                        sqlstate: "42601".to_string(),
                        message: format!("procedural compile: {e}"),
                    })?;

                // Validate the compiled expression via DataFusion.
                let compiled_parsed = ParsedCreateFunction {
                    or_replace: parsed.or_replace,
                    name: parsed.name.clone(),
                    parameters: parsed.parameters.clone(),
                    return_type: parsed.return_type.clone(),
                    volatility: parsed.volatility,
                    body_sql: compiled.clone(),
                };
                validate_function_body(&compiled_parsed)?;

                Some(compiled)
            }
        };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock before UNIX epoch".to_string(),
        })?
        .as_secs();

    let mut stored = StoredFunction {
        tenant_id,
        database_id,
        name: parsed.name.clone(),
        parameters: parsed.parameters,
        return_type: parsed.return_type,
        body_sql: parsed.body_sql,
        compiled_body_sql,
        volatility: parsed.volatility,
        security: crate::control::security::catalog::FunctionSecurity::default(),
        language: crate::control::security::catalog::function_types::FunctionLanguage::Sql,
        wasm_hash: None,
        wasm_module: None,
        dependencies: vec![],
        wasm_fuel: 1_000_000,
        wasm_memory: 16 * 1024 * 1024,
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    // The complete dependency list is part of the replicated definition so
    // followers replace the same catalog row atomically with the function.
    stored.dependencies = extract_dependencies(&stored);

    // Propose through the metadata raft group. Every node's applier
    // writes the function record to local redb and clears the
    // parsed block cache so subsequent calls re-parse the new body.
    // (The WASM binary itself, if any, stays on the proposing node
    // only until a future batch adds replicated WASM distribution.)
    // Ownership replicates through the parent `PutFunction`
    // post_apply on every node — `stored.owner` carries the creator
    // and `apply::function::put` installs the owner record. On the
    // single-node / rolling-upgrade / DDL-buffer fallback path
    // `propose_and_apply` runs the same applier locally so the
    // OWNERS row lands too.
    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(stored.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        // The no-Raft fallback still uses the CatalogEntry applier for the
        // durable row. Run the matching post-apply hook so its owner and
        // function-cache effects match a replicated apply.
        crate::control::catalog_entry::post_apply::function::put(stored.clone(), state);
    }

    // Broadcast to connected Lite sessions after the catalog commit is durable.
    emit_function_put(state, &stored);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE FUNCTION {}", stored.name),
    );

    Ok(status("CREATE FUNCTION"))
}

/// Encode the stored function into a `DefinitionSyncMsg` and broadcast to
/// all connected Lite sessions.
///
/// Called after `propose_and_apply` succeeds — the catalog mutation is
/// durable before this runs, so no Lite client can receive a definition
/// that gets rolled back.
pub(crate) fn emit_function_put(
    state: &crate::control::state::SharedState,
    stored: &crate::control::security::catalog::StoredFunction,
) {
    use nodedb_types::sync::wire::DefinitionSyncMsg;

    // Build the Lite-compatible JSON payload from the stored function.
    // LiteStoredFunction has a subset of fields; serialize only what Lite
    // expects so the schema stays forward-compatible.
    let lite_params: Vec<serde_json::Value> = stored
        .parameters
        .iter()
        .map(|p| serde_json::json!({ "name": p.name, "data_type": p.data_type }))
        .collect();

    let payload_json = serde_json::json!({
        "name": stored.name,
        "parameters": lite_params,
        "return_type": stored.return_type,
        "body_sql": stored.body_sql,
        "owner": stored.owner,
        "created_at": stored.created_at,
    });

    match sonic_rs::to_vec(&payload_json) {
        Ok(payload) => {
            let msg = DefinitionSyncMsg {
                tenant_id: stored.tenant_id,
                database_id: stored.database_id.as_u64(),
                definition_type: "function".into(),
                name: stored.name.clone(),
                action: "put".into(),
                payload,
            };
            state.definition_sync_fanout.broadcast(&msg);
        }
        Err(e) => {
            tracing::warn!(
                name = %stored.name,
                error = %e,
                "definition_sync: failed to serialize function payload; skipping broadcast"
            );
        }
    }
}
