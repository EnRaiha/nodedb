// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP CHANGE STREAM` DDL handler.
//!
//! Ported from the pgwire `ddl::change_stream::drop` handler. The catalog path
//! (`propose_catalog_entry` + `log_index == 0` local delete / registry /
//! cdc-router cleanup, the local-only webhook task stop, and the `audit_record`
//! call) is preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Existence check used by the neutral router's `DROP CHANGE STREAM IF EXISTS`
/// short-circuit. Folded in verbatim from the pgwire `change_stream_exists`
/// guard helper: checks the in-memory stream registry for the identity tenant.
pub fn change_stream_exists(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
) -> bool {
    let tid = identity.tenant_id.as_u64();
    state.stream_registry.get(database_id, tid, name).is_some()
}

/// Handle `DROP CHANGE STREAM [IF EXISTS] <name>`
pub fn drop_change_stream(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop change streams")?;

    // parts: ["DROP", "CHANGE", "STREAM", ...]
    let (if_exists, name) = if parts.len() >= 6
        && parts[3].eq_ignore_ascii_case("IF")
        && parts[4].eq_ignore_ascii_case("EXISTS")
    {
        (true, parts[5].to_lowercase())
    } else if parts.len() >= 4 {
        (false, parts[3].to_lowercase())
    } else {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP CHANGE STREAM [IF EXISTS] <name>".to_string(),
        });
    };

    let tenant_id = identity.tenant_id.as_u64();

    let catalog = state.credentials.catalog();

    // Pre-check existence via the catalog (no separate get_change_stream
    // method — use `load_all_change_streams` + filter, the set is
    // small and this is a DDL path so cost is irrelevant).
    let existed_before = catalog
        .get_change_stream(database_id, tenant_id, &name)
        .map(|opt| opt.is_some())
        .unwrap_or(false);
    if !existed_before && !if_exists {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("change stream '{name}' does not exist"),
        });
    }
    if !existed_before {
        return Ok(status("DROP CHANGE STREAM"));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeleteChangeStream {
        database_id: database_id.as_u64(),
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
            .delete_change_stream(database_id, tenant_id, &name)
            .map_err(|e| DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog delete: {e}"),
            })?;
        state
            .stream_registry
            .unregister(database_id, tenant_id, &name);
        state
            .cdc_router
            .remove_buffer(database_id, tenant_id, &name);
    }

    // Stop webhook delivery task if one was running for this stream.
    // Only the proposing node had a webhook task active; followers
    // never started one. This is the local-only cleanup.
    state
        .webhook_manager
        .stop_task(database_id, tenant_id, &name);
    state.kafka_manager.stop(database_id, tenant_id, &name);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CHANGE STREAM {name}"),
    );

    Ok(status("DROP CHANGE STREAM"))
}
