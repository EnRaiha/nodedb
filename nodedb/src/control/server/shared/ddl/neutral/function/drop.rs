// SPDX-License-Identifier: BUSL-1.1

//! `DROP FUNCTION [IF EXISTS]` DDL handler.
//!
//! Ported from the pgwire `ddl::function::drop` handler. The catalog path
//! (`propose_catalog_entry` + local applier fallback, dependency-block check,
//! replicated dependency deletion, Lite definition-sync broadcast, and the `audit_record`
//! call) is preserved verbatim; only the result construction changed from
//! pgwire `Response` / `PgWireError` to protocol-neutral result types.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::parse::validate_identifier;

/// Handle `DROP FUNCTION [IF EXISTS] <name>`
///
/// Requires superuser or tenant_admin — same privilege level as CREATE FUNCTION.
pub fn drop_function(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop functions")?;

    let (name, if_exists) = parse_drop_function(parts)?;
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    // Check if function exists.
    let func_exists = catalog
        .get_function_in_database(database_id, tenant_id, &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .is_some();

    if !func_exists && !if_exists {
        return Err(DdlError {
            sqlstate: "42883".to_string(),
            message: format!("function '{name}' does not exist"),
        });
    }

    if !func_exists {
        // IF EXISTS and function doesn't exist — no-op.
        return Ok(status("DROP FUNCTION"));
    }

    // Check dependencies: block DROP if other objects depend on this function.
    let dependents = catalog
        .find_dependents(database_id, tenant_id, "function", &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("dependency check: {e}"),
        })?;
    if !dependents.is_empty() {
        let dep_list: Vec<String> = dependents
            .iter()
            .map(|(t, n)| format!("{t} '{n}'"))
            .collect();
        return Err(DdlError {
            sqlstate: "2BP01".to_string(),
            message: format!(
                "cannot drop function '{name}': depended on by {}",
                dep_list.join(", ")
            ),
        });
    }

    // Delete function definition (including its dependencies) + ownership.
    // Replicate the deletion through the metadata raft group;
    // followers' applier clears their block cache and deletes
    // the record from their local redb.
    let entry = crate::control::catalog_entry::CatalogEntry::DeleteFunction {
        database_id,
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    crate::control::catalog_entry::apply::local::apply_locally_if_needed(state, &entry, log_index);

    // Broadcast deletion to connected Lite sessions.
    {
        use nodedb_types::sync::wire::DefinitionSyncMsg;
        let msg = DefinitionSyncMsg {
            tenant_id,
            database_id: database_id.as_u64(),
            definition_type: "function".into(),
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
        &format!("DROP FUNCTION {name}"),
    );

    Ok(status("DROP FUNCTION"))
}

/// Parse `DROP FUNCTION [IF EXISTS] <name>`.
fn parse_drop_function(parts: &[&str]) -> Result<(String, bool), DdlError> {
    // parts[0] = "DROP", parts[1] = "FUNCTION", ...
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "syntax: DROP FUNCTION [IF EXISTS] <name>".to_string(),
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
            message: "function name required".to_string(),
        });
    }

    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    validate_identifier(&name)?;
    Ok((name, if_exists))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_drop_basic() {
        let parts: Vec<&str> = "DROP FUNCTION normalize_email".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "normalize_email");
        assert!(!if_exists);
    }

    #[test]
    fn parse_drop_if_exists() {
        let parts: Vec<&str> = "DROP FUNCTION IF EXISTS myf".split_whitespace().collect();
        let (name, if_exists) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
        assert!(if_exists);
    }

    #[test]
    fn parse_drop_with_semicolon() {
        let parts: Vec<&str> = "DROP FUNCTION myf;".split_whitespace().collect();
        let (name, _) = parse_drop_function(&parts).unwrap();
        assert_eq!(name, "myf");
    }

    #[test]
    fn parse_drop_too_short() {
        let parts: Vec<&str> = "DROP FUNCTION".split_whitespace().collect();
        assert!(parse_drop_function(&parts).is_err());
    }
}
