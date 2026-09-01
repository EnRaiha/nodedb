// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for per-scope token quota catalog entries.
//!
//! `QuotaManager` answers every admission check from an in-memory map, and
//! reloads it from redb only at boot. Installing the applied definition here
//! makes the cap enforce on every node the moment the entry applies, not at
//! the next restart.

use crate::control::security::catalog::auth_types::StoredScopeQuota;
use crate::control::security::metering::quota::QuotaDefinition;
use crate::control::state::SharedState;

/// Install an applied scope quota into live enforcement.
///
/// A stored enforcement mode the local build cannot parse is logged and the
/// definition is skipped: the row stays durable, so a build that understands
/// the mode installs it at boot.
pub fn put(stored: &StoredScopeQuota, shared: &SharedState) {
    match QuotaDefinition::from_stored(stored.clone()) {
        Ok(definition) => {
            shared.quota_manager.install_replicated_quota(definition);
            tracing::debug!(
                scope = %stored.scope_name,
                max_tokens = stored.max_tokens,
                period_secs = stored.period_secs,
                "post_apply: scope quota replicated"
            );
        }
        Err(e) => {
            crate::diag::scope_quota_not_installed(&e, &stored.scope_name, &stored.enforcement);
            tracing::error!(
                scope = %stored.scope_name,
                enforcement = %stored.enforcement,
                error = %e,
                "post_apply: scope quota not installed; the cap does not enforce on this node"
            );
        }
    }
}

/// Drop an applied scope quota from live enforcement.
pub fn delete(scope_name: &str, shared: &SharedState) {
    let removed = shared.quota_manager.install_replicated_removal(scope_name);
    tracing::debug!(scope = %scope_name, removed, "post_apply: scope quota removal replicated");
}
