// SPDX-License-Identifier: BUSL-1.1

//! Canonical consumer-group stream identities.

use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Resolve a SQL stream token to its durable consumer-group identity.
///
/// A name identifies a topic only when a topic definition exists for its bare
/// name. Such groups always use the topic buffer key (`topic:<name>`); ordinary
/// change-stream names are left unchanged.
pub fn canonical_stream_name(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
) -> String {
    let stream_name = stream_name.to_lowercase();
    let bare = stream_name.strip_prefix("topic:").unwrap_or(&stream_name);
    if state
        .ep_topic_registry
        .get(database_id, tenant_id, bare)
        .is_some()
    {
        format!("topic:{bare}")
    } else {
        stream_name
    }
}

/// Migrate one legacy bare-topic group and its offsets once a topic definition
/// has established its canonical identity. Returns whether a migration ran.
pub fn migrate_legacy_topic_group(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    canonical_stream: &str,
    group: &str,
) -> crate::Result<bool> {
    let Some(legacy_stream) = canonical_stream.strip_prefix("topic:") else {
        return Ok(false);
    };
    if state
        .group_registry
        .get(database_id, tenant_id, canonical_stream, group)
        .is_some()
    {
        return Ok(false);
    }
    let Some(def) = state
        .group_registry
        .get(database_id, tenant_id, legacy_stream, group)
    else {
        return Ok(false);
    };
    state
        .credentials
        .catalog()
        .migrate_consumer_group_stream(&def, legacy_stream)?;
    state.group_registry.migrate_stream(
        database_id,
        tenant_id,
        legacy_stream,
        canonical_stream,
        group,
    );
    state.offset_store.migrate_group_stream(
        database_id,
        tenant_id,
        legacy_stream,
        canonical_stream,
        group,
    )?;
    Ok(true)
}
