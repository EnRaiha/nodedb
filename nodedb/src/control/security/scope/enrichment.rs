// SPDX-License-Identifier: BUSL-1.1

//! Enrich an [`AuthContext`] with scope-grant status and quota state from a
//! [`ScopeGrantStore`] and [`QuotaManager`].

use crate::control::security::auth_context::AuthContext;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::scope::grant::ScopeGrantStore;

/// Enrich AuthContext with scope status and quota data from the scope grant
/// and quota stores.
///
/// Populates metadata entries for `scope_status.<name>`,
/// `scope_expires_at.<name>`, `quota_remaining.<name>`, and
/// `quota_pct.<name>` so RLS predicates can reference
/// `$auth.scope_status(...)` / `$auth.quota_remaining(...)` /
/// `$auth.quota_pct(...)`.
///
/// `quota_remaining.<name>` / `quota_pct.<name>` are populated only for
/// scopes that both (a) the identity currently holds and (b) have a
/// `QuotaDefinition` registered under that scope name — `QuotaManager::get_status`
/// returns `None` for a held scope with no quota defined, and that's the
/// correct outcome: no quota metadata, not a zero-value one. Like
/// `scope_status`, this reflects quota state as of enrichment time (request
/// start), not the live value at predicate-evaluation time: usage charged by
/// the request in flight is recorded only after dispatch completes (see
/// `control::server::shared::metering`), so the value can be one request
/// stale under concurrent load. Callers needing the current live count use
/// `QuotaManager::get_status` directly (e.g. `SHOW QUOTA FOR AUTH USER`).
///
/// `now_secs` is supplied by the caller rather than read here so that the
/// clock used to *read* quota state is the same one used to *charge* it —
/// `QuotaManager` rolls a quota period over lazily on access, so a reader on a
/// different clock than the writer would roll the period over out from under
/// the recorded usage and report a full allowance.
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &ScopeGrantStore,
    quota_manager: &QuotaManager,
    org_ids: &[String],
    now_secs: u64,
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
        if let Some(quota_status) = quota_manager.get_status(scope_name, &ctx.id, now_secs) {
            ctx.metadata.insert(
                format!("quota_remaining.{scope_name}"),
                quota_status.remaining.to_string(),
            );
            ctx.metadata.insert(
                format!("quota_pct.{scope_name}"),
                quota_status.pct_used.to_string(),
            );
        }
    }
    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<String> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata.insert("scopes".into(), scope_list.join(","));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
    use crate::control::security::scope::grant::ScopeGrantParams;
    use crate::types::TenantId;

    fn test_ctx(user_id: &str) -> AuthContext {
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            None,
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
                nodedb_types::id::DatabaseId::DEFAULT,
            ]),
        );
        let mut ctx = AuthContext::from_identity(&identity, "s_test".into());
        ctx.id = user_id.to_string();
        ctx
    }

    #[test]
    fn quota_metadata_present_for_held_scope_with_quota() {
        let grants = ScopeGrantStore::new();
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "u1",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
            })
            .unwrap();
        let quotas = QuotaManager::new();
        quotas.define_quota(QuotaDefinition {
            scope_name: "pro:all".into(),
            max_tokens: 1000,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });
        quotas.record_usage("pro:all", "u1", 250, 1_000);

        let mut ctx = test_ctx("u1");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], 1_000);

        assert_eq!(
            ctx.metadata.get("quota_remaining.pro:all"),
            Some(&"750".to_string())
        );
        assert_eq!(
            ctx.metadata.get("quota_pct.pro:all"),
            Some(&"0.25".to_string())
        );
    }

    #[test]
    fn no_quota_metadata_for_scope_without_quota_definition() {
        let grants = ScopeGrantStore::new();
        grants
            .grant(ScopeGrantParams {
                scope_name: "free:all",
                grantee_type: "user",
                grantee_id: "u2",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
            })
            .unwrap();
        let quotas = QuotaManager::new();

        let mut ctx = test_ctx("u2");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], 1_000);

        assert!(!ctx.metadata.contains_key("quota_remaining.free:all"));
        assert!(!ctx.metadata.contains_key("quota_pct.free:all"));
    }

    /// Reading past the end of the quota period rolls it over lazily, so the
    /// enriched metadata reports a fresh full allowance rather than the
    /// previous period's usage.
    #[test]
    fn quota_metadata_reflects_lazy_period_rollover() {
        let grants = ScopeGrantStore::new();
        grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "u3",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
            })
            .unwrap();
        let quotas = QuotaManager::new();
        quotas.define_quota(QuotaDefinition {
            scope_name: "pro:all".into(),
            max_tokens: 1000,
            period_secs: 86400,
            enforcement: QuotaEnforcement::Hard,
            warning_threshold: 0.8,
        });
        quotas.record_usage("pro:all", "u3", 250, 1_000);

        let mut ctx = test_ctx("u3");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], 1_000 + 86_401);

        assert_eq!(
            ctx.metadata.get("quota_remaining.pro:all"),
            Some(&"1000".to_string())
        );
        assert_eq!(
            ctx.metadata.get("quota_pct.pro:all"),
            Some(&"0".to_string())
        );
    }
}
