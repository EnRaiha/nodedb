// SPDX-License-Identifier: BUSL-1.1

//! `DROP PROCEDURE [IF EXISTS]` DDL handler.
//!
//! Ported from the pgwire `ddl::procedure::drop` handler. The catalog path
//! (existence pre-check so `IF EXISTS` on a missing procedure never touches
//! raft, `propose_catalog_entry` + `LocalOnly` local-delete fallback, Lite
//! definition-sync broadcast, and the `audit_record` call) is preserved
//! verbatim; only the result construction changed from pgwire `Response` /
//! `PgWireError` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Handle `DROP PROCEDURE [IF EXISTS] <name>`
pub fn drop_procedure(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop procedures")?;

    if parts.len() < 3 {
        return Err(DdlError::new(
            "42601",
            "syntax: DROP PROCEDURE [IF EXISTS] <name>",
        ));
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
        return Err(DdlError::new("42601", "procedure name required"));
    }
    let name = parts[idx].to_lowercase().trim_end_matches(';').to_string();
    let tenant_id = identity.tenant_id.as_u64();
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);

    let catalog = state.credentials.catalog();

    // Pre-check existence so `IF EXISTS` on a missing procedure is
    // a clean no-op that never touches raft.
    let exists_before = catalog
        .get_procedure_in_database(database_id, tenant_id, &name)
        .map_err(|e| DdlError::new("XX000", format!("catalog read: {e}")))?
        .is_some();
    if !exists_before && !if_exists {
        return Err(DdlError::new(
            "42883",
            format!("procedure '{name}' does not exist"),
        ));
    }
    if !exists_before {
        return Ok(status("DROP PROCEDURE"));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteProcedure {
        database_id,
        tenant_id,
        name: name.clone(),
    };
    let outcome = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError::new("XX000", format!("metadata propose: {e}")))?;
    if outcome.needs_local_apply() {
        let _ = catalog
            .delete_procedure_in_database(database_id, tenant_id, &name)
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
    }

    // Broadcast deletion to connected Lite sessions.
    {
        use nodedb_types::sync::wire::DefinitionSyncMsg;
        let msg = DefinitionSyncMsg {
            tenant_id,
            database_id: database_id.as_u64(),
            definition_type: "procedure".into(),
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
        &format!("DROP PROCEDURE {name}"),
    );

    Ok(status("DROP PROCEDURE"))
}
