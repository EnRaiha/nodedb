// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE TRIGGER` DDL handler.
//!
//! Ported from the pgwire `ddl::trigger::create` handler. The catalog path
//! (`propose_and_apply` + `log_index == 0` local registry refresh, the
//! `emit_trigger_put` definition-sync broadcast, and the `audit_record` call)
//! is preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use crate::control::security::catalog::trigger_types::{
    TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity, TriggerTiming,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Parsed `CREATE TRIGGER` request — fields extracted by the nodedb-sql parser.
#[derive(Clone, Copy)]
pub struct CreateTriggerRequest<'a> {
    pub or_replace: bool,
    pub execution_mode: &'a str,
    pub name: &'a str,
    pub timing: &'a str,
    pub events_insert: bool,
    pub events_update: bool,
    pub events_delete: bool,
    pub collection: &'a str,
    pub granularity: &'a str,
    pub when_condition: Option<&'a str>,
    pub priority: i32,
    pub security: Option<&'a str>,
    pub body_sql: &'a str,
}

/// Handle `CREATE [OR REPLACE] TRIGGER ...` from typed AST fields.
pub fn create_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: CreateTriggerRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateTriggerRequest {
        or_replace,
        execution_mode,
        name,
        timing,
        events_insert,
        events_update,
        events_delete,
        collection,
        granularity,
        when_condition,
        priority,
        security,
        body_sql,
    } = req;
    require_tenant_admin(identity, "create triggers")?;

    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    // Check for existing trigger.
    if !or_replace
        && let Ok(Some(_)) = catalog.get_trigger_in_database(database_id, tenant_id, name)
    {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!("trigger '{name}' already exists"),
        });
    }

    // Validate the trigger body parses as procedural SQL.
    crate::control::planner::procedural::parse_block(body_sql).map_err(|e| DdlError {
        sqlstate: "42601".to_string(),
        message: format!("trigger body parse error: {e}"),
    })?;

    let execution_mode_enum = parse_execution_mode(execution_mode);
    let timing_enum = parse_timing(timing);
    let granularity_enum = parse_granularity(granularity);
    let security_enum = parse_security(security)?;

    if execution_mode_enum == TriggerExecutionMode::Sync {
        tracing::info!(
            trigger = %name,
            collection = %collection,
            "SYNC trigger created — trigger body DML must target same vShard"
        );
    }
    if execution_mode_enum == TriggerExecutionMode::Async {
        tracing::debug!(
            trigger = %name,
            collection = %collection,
            "ASYNC trigger: side effects are eventually consistent"
        );
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock before UNIX epoch".to_string(),
        })?
        .as_secs();

    let batch_mode = crate::control::trigger::batch::classify::classify_trigger_body(body_sql);

    let events = TriggerEvents {
        on_insert: events_insert,
        on_update: events_update,
        on_delete: events_delete,
    };

    let stored = crate::control::security::catalog::trigger_types::StoredTrigger {
        tenant_id,
        database_id,
        name: name.to_string(),
        collection: collection.to_string(),
        timing: timing_enum,
        events,
        granularity: granularity_enum,
        when_condition: when_condition.map(|s| s.to_string()),
        body_sql: body_sql.to_string(),
        priority,
        enabled: true,
        execution_mode: execution_mode_enum,
        security: security_enum,
        batch_mode,
        owner: identity.username.clone(),
        created_at: now,
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutTrigger(Box::new(stored.clone()));
    let log_index = super::super::super::catalog::propose_and_apply(state, &entry)?;
    if log_index == 0 {
        // The local fallback has already applied the durable CatalogEntry.
        // Mirror the applier's registry and ownership effects through its
        // post-apply hook rather than writing the registry directly.
        crate::control::catalog_entry::post_apply::trigger::put(stored.clone(), state);
    }

    // Broadcast to connected Lite sessions after the catalog commit is durable.
    emit_trigger_put(state, &stored);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "CREATE TRIGGER {} {} {} ON {}",
            stored.name,
            stored.timing.as_str(),
            stored.events.display(),
            stored.collection
        ),
    );

    Ok(status("CREATE TRIGGER"))
}

/// Encode the stored trigger and broadcast a `DefinitionSyncMsg` to all
/// connected Lite sessions after the catalog commit is durable.
pub(super) fn emit_trigger_put(
    state: &crate::control::state::SharedState,
    stored: &crate::control::security::catalog::trigger_types::StoredTrigger,
) {
    use nodedb_types::sync::wire::DefinitionSyncMsg;

    let mut events: Vec<&str> = Vec::new();
    if stored.events.on_insert {
        events.push("INSERT");
    }
    if stored.events.on_update {
        events.push("UPDATE");
    }
    if stored.events.on_delete {
        events.push("DELETE");
    }

    let payload_json = serde_json::json!({
        "name": stored.name,
        "collection": stored.collection,
        "timing": stored.timing.as_str(),
        "events": events,
        "granularity": stored.granularity.as_str(),
        "when_condition": stored.when_condition,
        "body_sql": stored.body_sql,
        "priority": stored.priority,
        "enabled": stored.enabled,
        "execution_mode": stored.execution_mode.as_str(),
        "owner": stored.owner,
        "created_at": stored.created_at,
    });

    match sonic_rs::to_vec(&payload_json) {
        Ok(payload) => {
            let msg = DefinitionSyncMsg {
                tenant_id: stored.tenant_id,
                database_id: stored.database_id.as_u64(),
                definition_type: "trigger".into(),
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
                "definition_sync: failed to serialize trigger payload; skipping broadcast"
            );
        }
    }
}

fn parse_execution_mode(s: &str) -> TriggerExecutionMode {
    match s.to_uppercase().as_str() {
        "SYNC" => TriggerExecutionMode::Sync,
        "DEFERRED" => TriggerExecutionMode::Deferred,
        _ => TriggerExecutionMode::Async,
    }
}

fn parse_timing(s: &str) -> TriggerTiming {
    match s.to_uppercase().as_str() {
        "BEFORE" => TriggerTiming::Before,
        "INSTEAD OF" => TriggerTiming::InsteadOf,
        _ => TriggerTiming::After,
    }
}

fn parse_granularity(s: &str) -> TriggerGranularity {
    if s.to_uppercase() == "STATEMENT" {
        TriggerGranularity::Statement
    } else {
        TriggerGranularity::Row
    }
}

/// Resolve the `SECURITY` clause, rejecting a mode the execution model cannot
/// honour.
///
/// Trigger bodies run asynchronously on the Event Plane, driven by a
/// `WriteEvent` that carries a tenant but no user identity — the invoking
/// session is long gone by the time the body fires. `SECURITY INVOKER` is
/// therefore not implementable here, and silently storing it while executing as
/// definer would leave the catalog describing a guarantee that does not exist.
/// An unspecified clause resolves to `DEFINER`, which is what actually happens.
fn parse_security(s: Option<&str>) -> Result<TriggerSecurity, DdlError> {
    let Some(mode) = s else {
        return Ok(TriggerSecurity::Definer);
    };
    match mode.to_uppercase().as_str() {
        "DEFINER" => Ok(TriggerSecurity::Definer),
        "INVOKER" => Err(DdlError {
            sqlstate: "0A000".to_string(),
            message: "SECURITY INVOKER is not supported for triggers: bodies execute \
                      asynchronously from a write event that carries no invoking identity. \
                      Use SECURITY DEFINER (the default)."
                .to_string(),
        }),
        other => Err(DdlError {
            sqlstate: "42601".to_string(),
            message: format!("unrecognised trigger SECURITY mode '{other}'"),
        }),
    }
}
