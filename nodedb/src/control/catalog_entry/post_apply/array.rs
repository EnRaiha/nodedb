// SPDX-License-Identifier: BUSL-1.1

//! The in-memory array mirror and the bitemporal retention registry, as the
//! array catalog entries change them.
//!
//! The Data Plane opens an array from the mirror, so the mirror changes
//! before the applied-index watcher advances. A `PutArray` installs its row
//! in the synchronous lane. A `DeleteArray` removes or moves its row in the
//! async lane, under the incarnation's gate, together with the per-core drop or
//! rekey, so a replica never routes a cell write to a key its cores left.

use nodedb_types::config::retention::BitemporalRetention;

use crate::control::array_catalog::ArrayCatalogEntry;
use crate::control::state::SharedState;
use crate::engine::bitemporal::BitemporalEngineKind;
use crate::types::{DatabaseId, TenantId};

/// Install `entry` in the mirror, replacing any row of the same identity,
/// and register its retention window.
pub fn put_sync(entry: &ArrayCatalogEntry, shared: &SharedState) {
    let array_id = &entry.array_id;
    {
        // A poisoned lock still holds a consistent mirror: every writer
        // replaces whole entries. Skipping the install hides the array
        // from every planner on this node for good.
        let mut mirror = shared
            .array_catalog
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        mirror.unregister_in_database(array_id.tenant_id, array_id.database_id, &entry.name);
        if let Err(error) = mirror.register(entry.clone()) {
            tracing::warn!(array = %entry.name, %error, "array mirror install failed");
        }
    }
    register_retention(entry, shared);
}

/// Register `entry`'s retention window, or unregister it when it has none.
fn register_retention(entry: &ArrayCatalogEntry, shared: &SharedState) {
    let array_id = &entry.array_id;
    let registry = &shared.bitemporal_retention_registry;
    let Some(audit_retain_ms) = entry.audit_retain_ms else {
        registry.unregister(array_id.database_id, array_id.tenant_id, &entry.name);
        return;
    };
    let retention = BitemporalRetention {
        data_retain_ms: 0,
        audit_retain_ms: u64::try_from(audit_retain_ms).unwrap_or(0),
        minimum_audit_retain_ms: entry.minimum_audit_retain_ms.unwrap_or(0),
    };
    if let Err(error) = registry.register(
        array_id.database_id,
        array_id.tenant_id,
        &entry.name,
        BitemporalEngineKind::Array,
        retention,
    ) {
        tracing::warn!(array = %entry.name, %error, "array retention register failed");
    }
}

/// Remove the array from the mirror and the retention registry. With `to`,
/// install the same definition under database `to` in one mirror write, so
/// the incarnation is never absent from the mirror.
pub fn remove_or_move(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    to: Option<u64>,
    shared: &SharedState,
) {
    let (database_id, tenant_id) = (DatabaseId::new(database_id), TenantId::new(tenant_id));
    let moved = {
        // See `put_sync`: a poisoned mirror is still consistent.
        let mut mirror = shared
            .array_catalog
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed = mirror.unregister_in_database(tenant_id, database_id, name);
        match (removed, to) {
            (Some(entry), Some(to)) => {
                let to = DatabaseId::new(to);
                let moved = ArrayCatalogEntry {
                    array_id: nodedb_array::types::ArrayId::in_database(tenant_id, to, name),
                    ..entry
                };
                mirror.unregister_in_database(tenant_id, to, name);
                if let Err(error) = mirror.register(moved.clone()) {
                    tracing::warn!(array = %name, %error, "array mirror move failed");
                }
                Some(moved)
            }
            _ => None,
        }
    };
    shared
        .bitemporal_retention_registry
        .unregister(database_id, tenant_id, name);
    if let Some(moved) = moved {
        register_retention(&moved, shared);
    }
}
