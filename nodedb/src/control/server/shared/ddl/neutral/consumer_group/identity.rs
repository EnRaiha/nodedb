// SPDX-License-Identifier: BUSL-1.1

//! Canonical consumer-group stream identities.

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;

/// Resolve a stream name to its durable consumer-group identity.
///
/// A name identifies a topic only when a topic definition exists for its bare
/// name. Such groups always use the topic buffer key (`topic:<name>`); ordinary
/// change-stream names are left unchanged.
///
/// `stream_name` arrives in its stored form. A caller holding a raw SQL token
/// decodes it first through
/// `crate::control::server::shared::ddl::sql_parse::parse_stream_ident_token`.
pub fn canonical_stream_name(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
) -> String {
    let bare = stream_name.strip_prefix("topic:").unwrap_or(stream_name);
    if state
        .ep_topic_registry
        .get(database_id, tenant_id, bare)
        .is_some()
    {
        format!("topic:{bare}")
    } else {
        stream_name.to_string()
    }
}

/// Migrate one legacy bare-topic group and its offsets once a topic definition
/// has established its canonical identity. Returns whether a migration ran.
///
/// The catalog re-key is replicated; the offsets move first, in their separate
/// database, so a failure there leaves the legacy identity whole.
pub fn migrate_legacy_topic_group(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    canonical_stream: &str,
    group: &str,
) -> Result<bool, DdlError> {
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
        .offset_store
        .migrate_group_stream(
            database_id,
            tenant_id,
            legacy_stream,
            canonical_stream,
            group,
        )
        .map_err(|error| DdlError::new("XX000", format!("consumer-group migration: {error}")))?;
    super::replicate::propose_migrate(state, &def, legacy_stream)?;
    Ok(true)
}
