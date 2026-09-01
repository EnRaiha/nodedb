// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP CONSUMER GROUP` DDL handler.
//!
//! The removal is replicated: the row, the `GroupRegistry` entry, and the
//! node-local committed offsets are dropped on every node. The registry is the
//! existence authority, and it is rebuilt from the catalog at boot.
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
        return Err(DdlError::new(
            "42601",
            "expected DROP CONSUMER GROUP <name> ON <stream>",
        ));
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
    super::identity::migrate_legacy_topic_group(
        state,
        database_id,
        tenant_id,
        &stream_name,
        &group_name,
    )?;

    if state
        .group_registry
        .get(database_id, tenant_id, &stream_name, &group_name)
        .is_none()
    {
        return Err(DdlError::new(
            "42704",
            format!("consumer group '{group_name}' does not exist on stream '{stream_name}'"),
        ));
    }

    // The entry carries the registry teardown and the committed-offset delete
    // to every node; the offset store is node-local and no entry can hold it.
    super::replicate::propose_delete(state, database_id, tenant_id, &stream_name, &group_name)?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("DROP CONSUMER GROUP"))
}
