// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CONSUMER GROUP` DDL handler.
//!
//! Ported from the pgwire `ddl::consumer_group::create` handler. The stream /
//! topic existence check, the duplicate-group check, the `ConsumerGroupDef`
//! build, the durable insert-if-absent catalog write + `group_registry.register`
//! path (NOT `propose_and_apply` — this family writes the catalog directly), and
//! the `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `CREATE CONSUMER GROUP <name> ON <stream>`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};
use super::identity::canonical_stream_name;

/// Handle `CREATE CONSUMER GROUP <name> ON <stream>`.
///
/// `group_name` and `stream_name` come from the typed
/// [`nodedb_sql::ddl_ast::statement::StreamViewStmt::CreateConsumerGroup`] variant.
pub async fn create_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    group_name: &str,
    stream_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create consumer groups")?;

    let group_name = group_name.to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();
    let requested_stream_name = stream_name.to_lowercase();

    // Consumer groups can be created on change streams or durable topics.
    let mut stream_name =
        canonical_stream_name(state, database_id, tenant_id, &requested_stream_name);
    let topic_lock = stream_name.strip_prefix("topic:").map(|topic| {
        state
            .ep_topic_registry
            .lifecycle_lock(database_id, tenant_id, topic)
    });
    let _topic_guard = match topic_lock {
        Some(lock) => Some(lock.lock_owned().await),
        None => None,
    };
    // Re-resolve after taking the topic lock so a completed DROP cannot leave
    // this CREATE targeting its removed topic incarnation.
    stream_name = canonical_stream_name(state, database_id, tenant_id, &requested_stream_name);
    let is_stream = state
        .stream_registry
        .get(database_id, tenant_id, &requested_stream_name)
        .is_some();
    let is_topic = stream_name.starts_with("topic:");
    if !is_stream && !is_topic {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("change stream or topic '{stream_name}' does not exist"),
        });
    }

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

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock error".to_string(),
        })?
        .as_secs();

    let def = ConsumerGroupDef {
        database_id,
        tenant_id,
        name: group_name.clone(),
        stream_name: stream_name.clone(),
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state.credentials.catalog();

    let inserted = catalog
        .put_consumer_group_if_absent(&def)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog write: {e}"),
        })?;
    if !inserted {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!(
                "consumer group '{group_name}' already exists on stream '{stream_name}'"
            ),
        });
    }

    state.group_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("CREATE CONSUMER GROUP"))
}
