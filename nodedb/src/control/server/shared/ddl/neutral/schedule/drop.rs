// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP SCHEDULE` DDL handler.
//!
//! Ported from the pgwire `ddl::schedule::drop` handler. The original
//! `propose_catalog_entry` + `log_index == 0` local-delete fallback (direct
//! `catalog.delete_schedule` + in-memory registry unregister), the `_schedules`
//! CRDT-sync tombstone delta, and the `audit_record` call are preserved
//! verbatim; only the result construction changed from pgwire `Response` /
//! `PgWireError` to the protocol-neutral [`DdlResult`] / [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Existence check used by the `DROP SCHEDULE IF EXISTS` short-circuit in the
/// neutral router. Mirrors the pgwire `exists::schedule_exists` helper.
pub fn schedule_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    name: &str,
) -> bool {
    state
        .schedule_registry
        .get(database_id, identity.tenant_id.as_u64(), name)
        .is_some()
}

/// Handle `DROP SCHEDULE [IF EXISTS] <name>`
pub fn drop_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop schedules")?;

    // parts: ["DROP", "SCHEDULE", ...]
    let (if_exists, name) = if parts.len() >= 5
        && parts[2].eq_ignore_ascii_case("IF")
        && parts[3].eq_ignore_ascii_case("EXISTS")
    {
        (true, parts[4].to_lowercase())
    } else if parts.len() >= 3 {
        (false, parts[2].to_lowercase())
    } else {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP SCHEDULE [IF EXISTS] <name>".to_string(),
        });
    };

    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    // Pre-check existence: `IF EXISTS` + missing is a no-op that
    // doesn't touch raft. Check via the in-memory registry since
    // `schedules.rs` has no `get_schedule` method today.
    let existed_before = state
        .schedule_registry
        .get(database_id, tenant_id, &name)
        .is_some();
    if !existed_before && !if_exists {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("schedule '{name}' does not exist"),
        });
    }
    if !existed_before {
        return Ok(status("DROP SCHEDULE"));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteSchedule {
        database_id,
        tenant_id,
        name: name.clone(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("metadata propose: {e}"),
        })?;
    if log_index == 0 {
        let _ = catalog
            .delete_schedule_in_database(database_id, tenant_id, &name)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog delete: {e}"),
            })?;
        catalog
            .delete_owner("schedule", database_id.as_u64(), tenant_id, &name)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog owner delete: {e}"),
            })?;
        state
            .schedule_registry
            .unregister(database_id, tenant_id, &name);
        state
            .permissions
            .install_replicated_remove_owner_in_database(
                "schedule",
                database_id.as_u64(),
                tenant_id,
                &name,
            );
    }

    // Emit tombstone delta for Lite visibility (removes schedule from Lite catalog).
    {
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            database_id,
            collection: "_schedules".into(),
            document_id: format!("{}:{name}", database_id.as_u64()),
            payload: Vec::new(),
            op: crate::event::crdt_sync::types::DeltaOp::Delete,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(delta);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP SCHEDULE {name}"),
    );

    Ok(status("DROP SCHEDULE"))
}
