// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP TRIGGER` and `ALTER TRIGGER ... ENABLE/DISABLE/OWNER`
//! DDL handlers.
//!
//! Ported from the pgwire `ddl::trigger::drop` handler. `drop_trigger` keeps its
//! original `propose_catalog_entry` + `log_index == 0` local-delete fallback;
//! `alter_trigger` / `alter_trigger_owner` keep their original direct
//! `catalog.put_trigger` + in-memory registry writes (no metadata propose). The
//! definition-sync broadcast and `audit_record` calls are preserved verbatim;
//! only the result construction changed from pgwire `Response` / `PgWireError`
//! to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Existence check used by the `DROP TRIGGER IF EXISTS` guard in the neutral
/// router. Mirrors the pgwire `exists::trigger_exists` helper: `false` when the
/// catalog is unavailable or the read errors.
pub fn trigger_exists(state: &SharedState, identity: &AuthenticatedIdentity, name: &str) -> bool {
    let Some(catalog) = state.credentials.catalog() else {
        return false;
    };
    let tid = identity.tenant_id.as_u64();
    matches!(catalog.get_trigger(tid, name), Ok(Some(_)))
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

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    // Check existence before proposing (so `IF EXISTS` + missing
    // trigger returns a clean success without touching raft).
    let exists_before = catalog
        .get_trigger(tenant_id, &name)
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
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    if log_index == 0 {
        catalog
            .delete_trigger(tenant_id, &name)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog write: {e}"),
            })?;
        state.trigger_registry.unregister(tenant_id, &name);
    }

    // Broadcast deletion to connected Lite sessions.
    {
        use nodedb_types::sync::wire::DefinitionSyncMsg;
        let msg = DefinitionSyncMsg {
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
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    let mut trigger = catalog
        .get_trigger(tenant_id, name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42704".to_string(),
            message: format!("trigger '{name}' does not exist"),
        })?;

    trigger.enabled = enabled;
    catalog.put_trigger(&trigger).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: e.to_string(),
    })?;

    // Update in-memory registry.
    state.trigger_registry.set_enabled(tenant_id, name, enabled);

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
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    let mut trigger = catalog
        .get_trigger(tenant_id, name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42704".to_string(),
            message: format!("trigger '{name}' does not exist"),
        })?;

    let old_owner = trigger.owner.clone();
    trigger.owner = new_owner.clone();
    catalog.put_trigger(&trigger).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: e.to_string(),
    })?;

    // Re-register with updated owner in the in-memory registry.
    state.trigger_registry.register(trigger);

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
