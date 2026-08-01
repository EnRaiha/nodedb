// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP TRIGGER` and `ALTER TRIGGER ... ENABLE/DISABLE/OWNER`
//! DDL handlers.
//!
//! Every trigger definition mutation is committed as a `CatalogEntry`, so the
//! selected database scope, descriptor stamp, owner row, and registry state are
//! applied consistently on every node.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::create::emit_trigger_put;

/// Existence check used by the `DROP TRIGGER IF EXISTS` guard in the neutral
/// router. Mirrors the pgwire `exists::trigger_exists` helper: `false` when the
/// catalog is unavailable or the read errors.
pub fn trigger_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let catalog = state.credentials.catalog();
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
    matches!(
        catalog.get_trigger_in_database(database_id, tenant_id, name),
        Ok(Some(_))
    )
}

/// Handle `DROP TRIGGER [IF EXISTS] <name>`
pub fn drop_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop triggers")?;

    let (name, if_exists) = parse_drop_trigger(parts)?;
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    // Check existence before proposing (so `IF EXISTS` + missing
    // trigger returns a clean success without touching raft).
    let exists_before = catalog
        .get_trigger_in_database(database_id, tenant_id, &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .is_some();
    if !exists_before && !if_exists {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("trigger '{name}' does not exist"),
        });
    }
    if !exists_before {
        return Ok(status("DROP TRIGGER"));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteTrigger {
        database_id,
        tenant_id,
        name: name.clone(),
    };
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        crate::control::catalog_entry::post_apply::trigger::delete(
            database_id,
            tenant_id,
            name.clone(),
            state,
        );
    }

    // Broadcast deletion to connected Lite sessions.
    {
        use nodedb_types::sync::wire::DefinitionSyncMsg;
        let msg = DefinitionSyncMsg {
            tenant_id,
            database_id: database_id.as_u64(),
            definition_type: "trigger".into(),
            name: name.clone(),
            action: "delete".into(),
            payload: vec![],
        };
        state.definition_sync_fanout.broadcast(&msg);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TRIGGER {name}"),
    );

    Ok(status("DROP TRIGGER"))
}

/// Handle `ALTER TRIGGER <name> ENABLE|DISABLE|OWNER TO <new_owner>`.
///
/// `name` and `action` come from the typed `AutomationStmt::AlterTrigger`
/// variant. `new_owner` is `Some` when `action == "OWNER"`.
pub fn alter_trigger(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    action: &str,
    new_owner: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter triggers")?;

    if action == "OWNER" {
        return alter_trigger_owner(state, identity, name, new_owner);
    }

    let enabled = match action {
        "ENABLE" => true,
        "DISABLE" => false,
        _ => {
            return Err(DdlError {
                sqlstate: "42601".to_string(),
                message: format!("expected ENABLE, DISABLE, or OWNER TO, got '{action}'"),
            });
        }
    };

    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
    let catalog = state.credentials.catalog();

    let mut trigger = catalog
        .get_trigger_in_database(database_id, tenant_id, name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42704".to_string(),
            message: format!("trigger '{name}' does not exist"),
        })?;

    trigger.enabled = enabled;
    let entry = crate::control::catalog_entry::CatalogEntry::PutTrigger(Box::new(trigger.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        crate::control::catalog_entry::post_apply::trigger::put(trigger.clone(), state);
    }
    emit_trigger_put(state, &trigger);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER TRIGGER {name} {action}"),
    );

    Ok(status("ALTER TRIGGER"))
}

/// Handle `ALTER TRIGGER <name> OWNER TO <new_owner>`
fn alter_trigger_owner(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    new_owner: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    let new_owner = new_owner
        .ok_or_else(|| DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER TRIGGER <name> OWNER TO <new_owner>".to_string(),
        })?
        .trim_end_matches(';')
        .to_string();

    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
    let catalog = state.credentials.catalog();

    let mut trigger = catalog
        .get_trigger_in_database(database_id, tenant_id, name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42704".to_string(),
            message: format!("trigger '{name}' does not exist"),
        })?;

    // Do not mutate a definition until the target principal is known to
    // exist. The tenant-admin gate in `alter_trigger` authorizes the transfer.
    if state.credentials.get_user(&new_owner).is_none() {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("user '{new_owner}' not found"),
        });
    }

    let old_owner = trigger.owner.clone();
    trigger.owner = new_owner.clone();
    let entry = crate::control::catalog_entry::CatalogEntry::PutTrigger(Box::new(trigger.clone()));
    let log_index = propose_and_apply(state, &entry)?;
    if log_index == 0 {
        crate::control::catalog_entry::post_apply::trigger::put(trigger.clone(), state);
    }
    emit_trigger_put(state, &trigger);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER TRIGGER {name} OWNER TO {new_owner} (was: {old_owner})"),
    );

    Ok(status("ALTER TRIGGER"))
}

fn parse_drop_trigger(parts: &[&str]) -> Result<(String, bool), DdlError> {
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: DROP TRIGGER [IF EXISTS] <name>".to_string(),
        });
    }
    let mut idx = 2;
    let if_exists = if parts.len() > 4
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        idx = 4;
        true
    } else {
        false
    };
    if idx >= parts.len() {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "trigger name required".to_string(),
        });
    }
    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    Ok((name, if_exists))
}
