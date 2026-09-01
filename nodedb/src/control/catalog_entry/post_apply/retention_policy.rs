// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for retention policy catalog entries.
//!
//! The enforcement loop and the auto-tier planner both read
//! `RetentionPolicyRegistry`, an in-memory map reloaded from redb only at
//! boot. Installing the applied definition here makes the policy enforce and
//! route on every node the moment the entry applies, not at the next restart.

use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;

/// Install an applied retention policy into the live registry.
pub fn put(def: &RetentionPolicyDef, shared: &SharedState) {
    shared.retention_policy_registry.register(def.clone());
    tracing::debug!(
        database_id = def.database_id,
        tenant_id = def.tenant_id,
        policy = %def.name,
        collection = %def.collection,
        "post_apply: retention policy replicated"
    );
}

/// Drop an applied retention policy from the live registry.
pub fn delete(database_id: u64, tenant_id: u64, name: &str, shared: &SharedState) {
    shared
        .retention_policy_registry
        .unregister(database_id, tenant_id, name);
    tracing::debug!(
        database_id,
        tenant_id,
        policy = %name,
        "post_apply: retention policy removal replicated"
    );
}
