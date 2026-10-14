// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for retention policy catalog entries.
//!
//! The enforcement loop and the auto-tier planner both read
//! `RetentionPolicyRegistry`, an in-memory map reloaded from redb only at
//! boot. Installing the applied definition here makes the policy enforce and
//! route on every node the moment the entry applies, not at the next restart.
//!
//! The policy's downsample tiers are continuous aggregates. They register on
//! every core of this node when the policy applies, and unregister when its
//! removal applies, as a user aggregate's do. Like a user aggregate's, that
//! dispatch runs on its own task: boot registers the tiers of every stored
//! policy again, so a dispatch lost to a crash is repaired at the next boot.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;
use crate::engine::timeseries::retention_policy::autowire::{register_tiers, unregister_tiers};

/// Install an applied retention policy into the live registry and register
/// its tiers on every core.
pub fn put(def: &RetentionPolicyDef, shared: &Arc<SharedState>) {
    shared.retention_policy_registry.register(def.clone());
    tracing::debug!(
        database_id = def.database_id,
        tenant_id = def.tenant_id,
        policy = %def.name,
        collection = %def.collection,
        "post_apply: retention policy replicated"
    );
    if def.downsample_tiers().is_empty() {
        return;
    }
    let shared = Arc::clone(shared);
    let def = def.clone();
    tokio::spawn(async move {
        if let Err(e) = register_tiers(&shared, &def).await {
            crate::diag::retention_autowire_orphaned(
                &e,
                def.database_id,
                def.tenant_id,
                &def.name,
                &def.collection,
            );
        }
    });
}

/// Drop an applied retention policy from the live registry and unregister
/// its tiers from every core.
pub fn delete(database_id: u64, tenant_id: u64, name: &str, shared: &Arc<SharedState>) {
    let removed = shared
        .retention_policy_registry
        .get(database_id, tenant_id, name);
    shared
        .retention_policy_registry
        .unregister(database_id, tenant_id, name);
    tracing::debug!(
        database_id,
        tenant_id,
        policy = %name,
        "post_apply: retention policy removal replicated"
    );
    let Some(def) = removed.filter(|def| !def.downsample_tiers().is_empty()) else {
        return;
    };
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        if let Err(e) = unregister_tiers(&shared, &def).await {
            crate::diag::retention_autowire_orphaned(
                &e,
                def.database_id,
                def.tenant_id,
                &def.name,
                &def.collection,
            );
        }
    });
}
