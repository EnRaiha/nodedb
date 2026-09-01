// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for alert rule catalog entries.
//!
//! The alert eval loop reads `AlertRegistry`, an in-memory map reloaded from
//! redb only at boot. Installing the applied definition here makes the rule
//! evaluate on every node the moment the entry applies, not at the next
//! restart. A delete also clears the rule's hysteresis counters.

use crate::control::state::SharedState;
use crate::event::alert::types::AlertDef;

/// Install an applied alert rule into the live registry.
pub fn put(def: &AlertDef, shared: &SharedState) {
    shared.alert_registry.register(def.clone());
    tracing::debug!(
        database_id = def.database_id,
        tenant_id = def.tenant_id,
        alert = %def.name,
        collection = %def.collection,
        "post_apply: alert rule replicated"
    );
}

/// Drop an applied alert rule from the live registry and its hysteresis state.
pub fn delete(database_id: u64, tenant_id: u64, name: &str, shared: &SharedState) {
    shared.alert_hysteresis.remove_alert(tenant_id, name);
    shared
        .alert_registry
        .unregister(database_id, tenant_id, name);
    tracing::debug!(
        database_id,
        tenant_id,
        alert = %name,
        "post_apply: alert rule removal replicated"
    );
}
