// SPDX-License-Identifier: BUSL-1.1

//! Enrich an [`AuthContext`] with scope-grant status from a
//! [`ScopeGrantStore`].

use crate::control::security::auth_context::AuthContext;
use crate::control::security::scope::grant::ScopeGrantStore;

/// Enrich AuthContext with scope status data from the scope grant store.
///
/// Populates metadata entries for `scope_status.<name>` and `scope_expires_at.<name>`
/// so RLS predicates can reference `$auth.metadata.scope_status.pro:all`.
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &ScopeGrantStore,
    org_ids: &[String],
) {
    let effective = scope_grants.effective_scopes(&ctx.id, org_ids);
    for scope_name in &effective {
        let status = scope_grants.scope_status(scope_name, "user", &ctx.id);
        ctx.metadata
            .insert(format!("scope_status.{scope_name}"), status.to_string());
        let expires_at = scope_grants.scope_expires_at(scope_name, "user", &ctx.id);
        if expires_at > 0 {
            ctx.metadata.insert(
                format!("scope_expires_at.{scope_name}"),
                expires_at.to_string(),
            );
        }
    }
    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<String> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata.insert("scopes".into(), scope_list.join(","));
    }
}
