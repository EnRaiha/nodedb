// SPDX-License-Identifier: BUSL-1.1

//! `ALTER FUNCTION ... OWNER TO` DDL handler.
//!
//! Ported from the pgwire `ddl::function::alter` handler. The catalog path
//! (`propose_and_apply` for both OWNER TO and SET (FUEL/MEMORY), plus the
//! `audit_record` calls) is preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Handle `ALTER FUNCTION <name> OWNER TO <new_owner>`
pub fn alter_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "alter functions")?;

    if parts.len() < 4 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER FUNCTION <name> OWNER TO <user> | SET (FUEL=N, MEMORY=N)"
                .to_string(),
        });
    }

    let name = parts[2].to_lowercase();
    let action = parts[3].to_uppercase();

    // ALTER FUNCTION <name> SET (FUEL = N, MEMORY = N)
    if action == "SET" {
        return alter_function_limits(state, identity, &name, parts);
    }

    // ALTER FUNCTION <name> OWNER TO <new_owner>
    if action != "OWNER" || parts.len() < 6 || !parts[4].eq_ignore_ascii_case("TO") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: ALTER FUNCTION <name> OWNER TO <user> | SET (FUEL=N, MEMORY=N)"
                .to_string(),
        });
    }

    let new_owner = parts[5].trim_end_matches(';').to_string();

    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    let mut func = catalog
        .get_function(tenant_id, &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42883".to_string(),
            message: format!("function '{name}' does not exist"),
        })?;

    let old_owner = func.owner.clone();
    func.owner = new_owner.clone();
    // Route through the same metadata-raft propose path every other
    // parent-replicated ALTER uses. The applier's
    // `owner::put_parent_owner` companion write rebinds the OWNERS
    // table to the new owner cluster-wide — without this, an
    // ALTER FUNCTION OWNER TO updated only the function row's
    // in-band `owner` field and the OWNERS table still resolved the
    // function to the previous owner, silently breaking permission
    // transfer.
    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(func.clone()));
    propose_and_apply(state, &entry)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER FUNCTION {name} OWNER TO {new_owner} (was: {old_owner})"),
    );

    Ok(status("ALTER FUNCTION"))
}

/// Handle `ALTER FUNCTION <name> SET (FUEL = N, MEMORY = N)`
fn alter_function_limits(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    let mut func = catalog
        .get_function(tenant_id, name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: e.to_string(),
        })?
        .ok_or_else(|| DdlError {
            sqlstate: "42883".to_string(),
            message: format!("function '{name}' does not exist"),
        })?;

    // Parse SET (...) from remaining parts.
    let rest = parts[4..].join(" ");
    let rest = rest
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_end_matches(';');
    for part in rest.split(',') {
        let kv: Vec<&str> = part.split('=').map(str::trim).collect();
        if kv.len() != 2 {
            continue;
        }
        match kv[0].to_uppercase().as_str() {
            "FUEL" => {
                if let Ok(v) = kv[1].parse::<u64>() {
                    func.wasm_fuel = v;
                }
            }
            "MEMORY" => {
                if let Ok(v) = kv[1].parse::<usize>() {
                    func.wasm_memory = v;
                }
            }
            _ => {}
        }
    }

    let entry = crate::control::catalog_entry::CatalogEntry::PutFunction(Box::new(func.clone()));
    propose_and_apply(state, &entry)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "ALTER FUNCTION {name} SET (FUEL={}, MEMORY={})",
            func.wasm_fuel, func.wasm_memory
        ),
    );

    Ok(status("ALTER FUNCTION"))
}
