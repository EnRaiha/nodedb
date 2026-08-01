// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `DROP TOPIC` DDL handler.
//!
//! The topic mutation lock serializes durable deletion with publication. Offset
//! cleanup commits first in its separate database; then one catalog transaction
//! removes the definition, messages, and both consumer-group identities.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::topic::validate_topic_name;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

pub async fn drop_topic(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "drop topics")?;

    // parts: ["DROP", "TOPIC", "<name>"]
    if parts.len() < 3 {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected DROP TOPIC <name>".to_string(),
        });
    }

    let name = parts[2].to_lowercase();
    validate_topic_name(&name).map_err(|message| DdlError {
        sqlstate: "42601".to_string(),
        message: message.to_string(),
    })?;
    let tenant_id = identity.tenant_id.as_u64();
    let lifecycle_lock = state
        .ep_topic_registry
        .lifecycle_lock(database_id, tenant_id, &name);
    let _guard = lifecycle_lock.lock().await;

    let catalog = state.credentials.catalog();
    let buffer_key = format!("topic:{name}");

    // Enumerate every durable and runtime group before changing either store.
    // A catalog-read failure is fatal: reporting success without identifying a
    // legacy group would let it attach to a recreated topic.
    let mut group_names = std::collections::BTreeSet::new();
    for stream in [&buffer_key, &name] {
        for group in state
            .group_registry
            .list_for_stream(database_id, tenant_id, stream)
        {
            group_names.insert(group.name);
        }
    }
    for group_name in catalog
        .topic_consumer_group_names(database_id, tenant_id, &name)
        .map_err(|error| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog enumerate topic consumer groups: {error}"),
        })?
    {
        group_names.insert(group_name);
    }

    // Topic lifecycle is held above. Acquire every group pair in globally
    // deterministic order (group name, canonical stream, legacy stream) before
    // either durable migration/cleanup or runtime mutation.
    let mut group_guards = Vec::with_capacity(group_names.len() * 2);
    for group_name in &group_names {
        for stream in [&buffer_key, &name] {
            let lock =
                state
                    .group_registry
                    .lifecycle_lock(database_id, tenant_id, stream, group_name);
            group_guards.push(lock.lock_owned().await);
        }
    }

    // The offsets live in a separate redb database. Commit their complete
    // cleanup first; any failure leaves the catalog topic and groups intact and
    // returns an error, so DROP TOPIC can never claim success with cursors that
    // could revive on a recreate.
    let offset_groups: Vec<(String, String)> = group_names
        .iter()
        .flat_map(|group| {
            [
                (buffer_key.clone(), group.clone()),
                (name.clone(), group.clone()),
            ]
        })
        .collect();
    state
        .offset_store
        .delete_groups(database_id, tenant_id, &offset_groups)
        .map_err(|error| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("durable topic offset cleanup: {error}"),
        })?;

    // Definition, retained messages, and both consumer-group identities share
    // one catalog transaction. There is no best-effort path after this point.
    let existed = catalog
        .delete_ep_topic_with_consumer_groups(database_id, tenant_id, &name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;
    if !existed {
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("topic '{name}' does not exist"),
        });
    }

    state
        .ep_topic_registry
        .unregister(database_id, tenant_id, &name);
    state
        .cdc_router
        .remove_buffer(database_id, tenant_id, &buffer_key);
    for group_name in &group_names {
        for stream in [&buffer_key, &name] {
            state
                .group_registry
                .unregister(database_id, tenant_id, stream, group_name);
        }
    }
    drop(group_guards);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP TOPIC {name}"),
    );

    Ok(status("DROP TOPIC"))
}
