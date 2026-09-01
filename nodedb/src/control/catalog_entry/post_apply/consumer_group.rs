// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for consumer-group catalog entries.
//!
//! `GroupRegistry` is an in-memory map rebuilt from redb only at boot. Every
//! node installs the applied definition here, so a group created on one node
//! resolves on all of them without a restart.

use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

/// Install an applied consumer group. An existing registration is kept, so the
/// create-only entry never overwrites a live definition.
pub fn put_if_absent(def: &ConsumerGroupDef, shared: &SharedState) {
    if shared
        .group_registry
        .get(def.database_id, def.tenant_id, &def.stream_name, &def.name)
        .is_some()
    {
        return;
    }
    shared.group_registry.register(def.clone());
}

/// Drop an applied consumer group from the registry and its durable offsets.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    stream_name: &str,
    name: &str,
    shared: &SharedState,
) {
    let database_id = DatabaseId::new(database_id);
    shared
        .group_registry
        .unregister(database_id, tenant_id, stream_name, name);
    forget_offsets(database_id, tenant_id, stream_name, name, shared);
}

/// Re-key an applied group onto its canonical `topic:<name>` stream, carrying
/// its committed offsets across.
pub fn migrate_stream(def: &ConsumerGroupDef, legacy_stream: &str, shared: &SharedState) {
    let canonical = format!("topic:{legacy_stream}");
    shared.group_registry.migrate_stream(
        def.database_id,
        def.tenant_id,
        legacy_stream,
        &canonical,
        &def.name,
    );
    if let Err(error) = shared.offset_store.migrate_group_stream(
        def.database_id,
        def.tenant_id,
        legacy_stream,
        &canonical,
        &def.name,
    ) {
        crate::diag::consumer_group_offsets_retained(
            &error,
            def.database_id.as_u64(),
            def.tenant_id,
            legacy_stream,
            &def.name,
            "migrate_group_stream",
        );
    }
}

/// Clear one group's durable offsets on this node.
///
/// The offset store is node-local, so a failure here leaves a cursor no
/// replicated entry can clear. It is filed, never dropped.
pub(super) fn forget_offsets(
    database_id: DatabaseId,
    tenant_id: u64,
    stream: &str,
    group: &str,
    shared: &SharedState,
) {
    if let Err(error) = shared
        .offset_store
        .delete_group(database_id, tenant_id, stream, group)
    {
        crate::diag::consumer_group_offsets_retained(
            &error,
            database_id.as_u64(),
            tenant_id,
            stream,
            group,
            "delete_group",
        );
    }
}
