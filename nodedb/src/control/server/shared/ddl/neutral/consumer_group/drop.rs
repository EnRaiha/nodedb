// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP CONSUMER GROUP` DDL handler.
//!
//! Ported from the pgwire `ddl::consumer_group::drop` handler. The token-based
//! syntax check, the direct `catalog.delete_consumer_group` path (NOT a
//! `propose_catalog_entry` proposal — this family writes the catalog directly),
//! the `group_registry.unregister`, the best-effort `offset_store.delete_group`
//! (warn-and-continue, preserved verbatim as the pre-existing behavior), and the
//! `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `DROP CONSUMER GROUP <name> ON <stream>`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::identity::canonical_stream_name;

/// Handle `DROP CONSUMER GROUP <name> ON <stream>`
pub async fn drop_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop consumer groups")?;

    // parts: ["DROP", "CONSUMER", "GROUP", "<name>", "ON", "<stream>"]
    if parts.len() < 6 || !parts[4].eq_ignore_ascii_case("ON") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP CONSUMER GROUP <name> ON <stream>".to_string(),
        });
    }

    let group_name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();
    let requested_stream = parts[5];
    let mut stream_name = canonical_stream_name(state, database_id, tenant_id, requested_stream);
    let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
        state
            .ep_topic_registry
            .lifecycle_lock(database_id, tenant_id, topic)
    });
    let _topic_guard = match topic_lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    };
    stream_name = canonical_stream_name(state, database_id, tenant_id, requested_stream);
    let lifecycle_lock =
        state
            .group_registry
            .lifecycle_lock(database_id, tenant_id, &stream_name, &group_name);
    let _group_guard = lifecycle_lock.lock().await;
    let legacy_group_lock = stream_name.strip_prefix("topic:").map(|legacy_stream| {
        state
            .group_registry
            .lifecycle_lock(database_id, tenant_id, legacy_stream, &group_name)
    });
    let _legacy_group_guard = match legacy_group_lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    };
    if let Err(error) = super::identity::migrate_legacy_topic_group(
        state,
        database_id,
        tenant_id,
        &stream_name,
        &group_name,
    ) {
        return Err(DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("consumer-group migration: {error}"),
        });
    }

    let catalog = state.credentials.catalog();

    let existed = catalog
        .delete_consumer_group(database_id, tenant_id, &stream_name, &group_name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;

    if !existed {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!(
                "consumer group '{group_name}' does not exist on stream '{stream_name}'"
            ),
        });
    }

    state
        .group_registry
        .unregister(database_id, tenant_id, &stream_name, &group_name);

    // Delete committed offsets for this group.
    if let Err(e) =
        state
            .offset_store
            .delete_group(database_id, tenant_id, &stream_name, &group_name)
    {
        tracing::warn!(
            error = %e,
            "failed to delete offsets for consumer group {group_name}"
        );
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("DROP CONSUMER GROUP"))
}
