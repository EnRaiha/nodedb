// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE CONSUMER GROUP` DDL handler.
//!
//! Ported from the pgwire `ddl::consumer_group::create` handler. The stream /
//! topic existence check, the duplicate-group check, the `ConsumerGroupDef`
//! build, the direct `catalog.put_consumer_group` + `group_registry.register`
//! path (NOT `propose_and_apply` — this family writes the catalog directly), and
//! the `audit_record` call are preserved verbatim; only the result construction
//! changed from pgwire `Response` / `PgWireError` to the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `CREATE CONSUMER GROUP <name> ON <stream>`

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Handle `CREATE CONSUMER GROUP <name> ON <stream>`.
///
/// `group_name` and `stream_name` come from the typed
/// [`nodedb_sql::ddl_ast::statement::StreamViewStmt::CreateConsumerGroup`] variant.
pub fn create_consumer_group(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    group_name: &str,
    stream_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "create consumer groups")?;

    let group_name = group_name.to_lowercase();
    let stream_name = stream_name.to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    // Verify the stream or topic exists.
    // Consumer groups can be created on change streams or durable topics.
    let is_stream = state.stream_registry.get(tenant_id, &stream_name).is_some();
    let is_topic = state
        .ep_topic_registry
        .get(tenant_id, &stream_name)
        .is_some();
    // Topics use "topic:<name>" as buffer key — check with prefix too.
    let topic_bare = stream_name.strip_prefix("topic:").unwrap_or(&stream_name);
    let is_topic = is_topic || state.ep_topic_registry.get(tenant_id, topic_bare).is_some();
    if !is_stream && !is_topic {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("change stream or topic '{stream_name}' does not exist"),
        });
    }

    // Check for duplicate group.
    if state
        .group_registry
        .get(tenant_id, &stream_name, &group_name)
        .is_some()
    {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!(
                "consumer group '{group_name}' already exists on stream '{stream_name}'"
            ),
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
        tenant_id,
        name: group_name.clone(),
        stream_name: stream_name.clone(),
        owner: identity.username.clone(),
        created_at: now,
    };

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system catalog not available".to_string(),
        })?;

    catalog.put_consumer_group(&def).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("catalog write: {e}"),
    })?;

    state.group_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE CONSUMER GROUP {group_name} ON {stream_name}"),
    );

    Ok(status("CREATE CONSUMER GROUP"))
}
