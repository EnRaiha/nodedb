// SPDX-License-Identifier: BUSL-1.1

//! Enrich an [`AuthContext`] with scope-grant status and quota state from a
//! [`ScopeGrantStore`] and [`QuotaManager`].

use std::collections::HashSet;

use tracing::debug;

use nodedb_types::Value;

use crate::control::security::auth_context::AuthContext;
use crate::control::security::conditional::evaluate_conditions;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::scope::grant::ScopeGrantStore;

/// Metadata key prefix for the per-scope grant status this function owns.
const SCOPE_STATUS_PREFIX: &str = "scope_status.";

/// Metadata key prefix naming the condition that withheld a granted scope.
const SCOPE_DENIED_PREFIX: &str = "scope_denied.";

/// Metadata key holding the comma-separated list of scopes in effect.
const SCOPES_KEY: &str = "scopes";

/// Metadata key prefix for the per-scope expiry timestamp this function owns.
const SCOPE_EXPIRES_PREFIX: &str = "scope_expires_at.";

/// Metadata key prefix for the per-scope quota-remaining figure this
/// function owns.
const QUOTA_REMAINING_PREFIX: &str = "quota_remaining.";

/// Metadata key prefix for the per-scope quota-percent-used figure this
/// function owns.
const QUOTA_PCT_PREFIX: &str = "quota_pct.";

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
/// the recorded usage and report a full allowance. Grant conditions are
/// evaluated on that same clock, so a temporal window cannot open or close
/// midway through resolving one request.
///
/// # Conditional grants
///
/// This is the one place a scope grant is paired with the request it might
/// apply to, so it is where `WHEN` / `REQUIRE` conditions are evaluated. A
/// grant whose conditions fail contributes nothing: no `scope_status.<name>`,
/// no quota metadata, and no entry in the `scopes` list. Instead
/// `scope_denied.<name>` records the reason, so an operator (or an RLS
/// predicate) can tell "withheld by a condition" from "never granted".
///
/// `client_ip` is the request's real client address, or `None` when the
/// transport had no usable one. `REQUIRE IP` fails closed on `None` rather
/// than skipping the check — see `conditional::evaluate`.
///
/// The keys this function owns (`scope_status.*`, `scope_denied.*`,
/// `scope_expires_at.*`, `quota_remaining.*`, `quota_pct.*`, and `scopes`)
/// are cleared before it repopulates them. A JWT can carry an arbitrary
/// `metadata` claim (including its own `scope_expires` claim, which
/// `AuthContext::from_verified_jwt` stamps into `scope_expires_at.*` ahead of
/// this call), and a stale or forged entry under one of those keys would
/// otherwise survive here and answer `$auth.scope_status()` /
/// `$auth.scope_expires_at()` / `$auth.quota_remaining()` /
/// `$auth.quota_pct()` for a scope the store never granted, one a condition
/// just withheld, or one held permanently (no `expires_at` to overwrite it).
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &ScopeGrantStore,
    quota_manager: &QuotaManager,
    org_ids: &[String],
    client_ip: Option<&str>,
    now_secs: u64,
) {
    ctx.metadata.retain(|key, _| {
        key.as_str() != SCOPES_KEY
            && !key.starts_with(SCOPE_STATUS_PREFIX)
            && !key.starts_with(SCOPE_DENIED_PREFIX)
            && !key.starts_with(SCOPE_EXPIRES_PREFIX)
            && !key.starts_with(QUOTA_REMAINING_PREFIX)
            && !key.starts_with(QUOTA_PCT_PREFIX)
    });

    // Evaluate every grant against this request first: the verdicts borrow
    // `ctx` immutably, and recording them below borrows it mutably.
    let grants = scope_grants.effective_grants(&ctx.id, org_ids);
    let verdicts: Vec<(String, Option<String>)> = grants
        .iter()
        .map(|grant| {
            let refused = evaluate_conditions(&grant.conditions, ctx, client_ip, now_secs)
                .err()
                .map(|refusal| {
                    debug!(
                        scope = %grant.scope_name,
                        session = %ctx.session_id,
                        detail = %refusal.audit_detail,
                        "scope grant withheld by a condition"
                    );
                    refusal.resource
                });
            (grant.scope_name.clone(), refused)
        })
        .collect();

    // A scope may be granted more than once (directly and through an org);
    // it applies when any one of those grants passes its conditions.
    let effective: HashSet<&str> = verdicts
        .iter()
        .filter(|(_, refused)| refused.is_none())
        .map(|(scope_name, _)| scope_name.as_str())
        .collect();

    for (scope_name, refused) in &verdicts {
        let Some(reason) = refused else {
            continue;
        };
        if !effective.contains(scope_name.as_str()) {
            ctx.metadata.insert(
                format!("{SCOPE_DENIED_PREFIX}{scope_name}"),
                Value::String(reason.clone()),
            );
        }
    }

    for scope_name in &effective {
        let status = scope_grants.scope_status(scope_name, "user", &ctx.id);
        ctx.metadata.insert(
            format!("{SCOPE_STATUS_PREFIX}{scope_name}"),
            Value::String(status.to_string()),
        );
        let expires_at = scope_grants.scope_expires_at(scope_name, "user", &ctx.id);
        if expires_at > 0 {
            ctx.metadata.insert(
                format!("{SCOPE_EXPIRES_PREFIX}{scope_name}"),
                Value::Integer(expires_at as i64),
            );
        }
        if let Some(quota_status) = quota_manager.get_status(scope_name, &ctx.id, now_secs) {
            ctx.metadata.insert(
                format!("{QUOTA_REMAINING_PREFIX}{scope_name}"),
                Value::Integer(quota_status.remaining as i64),
            );
            ctx.metadata.insert(
                format!("{QUOTA_PCT_PREFIX}{scope_name}"),
                Value::Float(quota_status.pct_used),
            );
        }
    }

    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<&str> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata
            .insert(SCOPES_KEY.into(), Value::String(scope_list.join(",")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::conditional::GrantCondition;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
    use crate::control::security::risk::STEP_UP_REQUIRED;
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
                conditions: Vec::new(),
            })
            .unwrap();
        let quotas = QuotaManager::new();
        quotas
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        quotas.record_usage("pro:all", "u1", 250, 1_000);

        let mut ctx = test_ctx("u1");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], None, 1_000);

        assert_eq!(
            ctx.metadata.get("quota_remaining.pro:all"),
            Some(&Value::Integer(750))
        );
        assert_eq!(
            ctx.metadata.get("quota_pct.pro:all"),
            Some(&Value::Float(0.25))
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
                conditions: Vec::new(),
            })
            .unwrap();
        let quotas = QuotaManager::new();

        let mut ctx = test_ctx("u2");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], None, 1_000);

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
                conditions: Vec::new(),
            })
            .unwrap();
        let quotas = QuotaManager::new();
        quotas
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        quotas.record_usage("pro:all", "u3", 250, 1_000);

        let mut ctx = test_ctx("u3");
        enrich_auth_context_with_scopes(&mut ctx, &grants, &quotas, &[], None, 1_000 + 86_401);

        assert_eq!(
            ctx.metadata.get("quota_remaining.pro:all"),
            Some(&Value::Integer(1000))
        );
        assert_eq!(
            ctx.metadata.get("quota_pct.pro:all"),
            Some(&Value::Float(0.0))
        );
    }

    // ── Conditional grants ──────────────────────────────────────────────

    /// A Thursday (the Unix epoch's weekday) at 12:00 and 20:00 UTC.
    const INSIDE_BUSINESS_HOURS: u64 = 12 * 3_600;
    const AFTER_BUSINESS_HOURS: u64 = 20 * 3_600;

    fn grant_with(
        grants: &ScopeGrantStore,
        scope_name: &str,
        grantee_id: &str,
        conditions: Vec<GrantCondition>,
    ) {
        grants
            .grant(ScopeGrantParams {
                scope_name,
                grantee_type: "user",
                grantee_id,
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions,
            })
            .expect("grant");
    }

    #[test]
    fn temporal_grant_applies_inside_its_window_only() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        grant_with(
            &grants,
            "pro:all",
            "u10",
            vec![GrantCondition::Temporal {
                start_hour: 9,
                end_hour: 17,
                days: Vec::new(),
            }],
        );

        let mut inside = test_ctx("u10");
        enrich_auth_context_with_scopes(
            &mut inside,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );
        assert_eq!(
            inside.metadata.get("scope_status.pro:all"),
            Some(&Value::String("active".into()))
        );
        assert_eq!(
            inside.metadata.get("scopes"),
            Some(&Value::String("pro:all".into()))
        );

        let mut outside = test_ctx("u10");
        enrich_auth_context_with_scopes(
            &mut outside,
            &grants,
            &quotas,
            &[],
            None,
            AFTER_BUSINESS_HOURS,
        );
        assert!(
            !outside.metadata.contains_key("scope_status.pro:all"),
            "a grant outside its window must not contribute its scope"
        );
        assert!(!outside.metadata.contains_key("scopes"));
        assert_eq!(
            outside.metadata.get("scope_denied.pro:all"),
            Some(&Value::String("outside the grant's permitted hours".into()))
        );
    }

    #[test]
    fn ip_restricted_grant_applies_only_from_the_permitted_network() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        grant_with(
            &grants,
            "ops:all",
            "u11",
            vec![GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            }],
        );

        let mut inside = test_ctx("u11");
        enrich_auth_context_with_scopes(
            &mut inside,
            &grants,
            &quotas,
            &[],
            Some("10.1.2.3"),
            INSIDE_BUSINESS_HOURS,
        );
        assert_eq!(
            inside.metadata.get("scope_status.ops:all"),
            Some(&Value::String("active".into()))
        );

        let mut elsewhere = test_ctx("u11");
        enrich_auth_context_with_scopes(
            &mut elsewhere,
            &grants,
            &quotas,
            &[],
            Some("203.0.113.9"),
            INSIDE_BUSINESS_HOURS,
        );
        assert!(!elsewhere.metadata.contains_key("scope_status.ops:all"));
    }

    /// The fail-closed case: no client address at all means the IP condition
    /// cannot be evaluated, so the grant must be withheld rather than
    /// applied unchecked.
    #[test]
    fn ip_restricted_grant_is_withheld_when_no_client_address_is_known() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        grant_with(
            &grants,
            "ops:all",
            "u12",
            vec![GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into()],
            }],
        );

        let mut ctx = test_ctx("u12");
        enrich_auth_context_with_scopes(
            &mut ctx,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );

        assert!(!ctx.metadata.contains_key("scope_status.ops:all"));
        assert_eq!(
            ctx.metadata.get("scope_denied.ops:all"),
            Some(&Value::String(
                "client address unavailable for an IP-restricted grant".into()
            ))
        );
    }

    #[test]
    fn mfa_condition_reports_the_shared_step_up_refusal() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        grant_with(
            &grants,
            "admin:all",
            "u13",
            vec![GrantCondition::RequireMfa],
        );

        let mut ctx = test_ctx("u13");
        enrich_auth_context_with_scopes(
            &mut ctx,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );
        assert_eq!(
            ctx.metadata.get("scope_denied.admin:all"),
            Some(&Value::String(STEP_UP_REQUIRED.to_string()))
        );

        let mut verified = test_ctx("u13");
        verified
            .metadata
            .insert("mfa_verified".into(), "true".into());
        enrich_auth_context_with_scopes(
            &mut verified,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );
        assert_eq!(
            verified.metadata.get("scope_status.admin:all"),
            Some(&Value::String("active".into()))
        );
    }

    /// A quota is only read for a scope that is actually in effect: a
    /// withheld grant must not leave quota metadata behind that would make
    /// `$auth.quota_remaining(...)` answer for an entitlement the request
    /// never received.
    #[test]
    fn withheld_grant_leaves_no_quota_metadata() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        grant_with(&grants, "pro:all", "u14", vec![GrantCondition::RequireMfa]);
        quotas
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        quotas.record_usage("pro:all", "u14", 250, INSIDE_BUSINESS_HOURS);

        let mut ctx = test_ctx("u14");
        enrich_auth_context_with_scopes(
            &mut ctx,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );

        assert!(!ctx.metadata.contains_key("quota_remaining.pro:all"));
    }

    /// Claim-supplied metadata must never answer for a scope the store did
    /// not grant this request — enrichment owns those keys outright.
    #[test]
    fn claim_supplied_scope_status_does_not_survive_enrichment() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();

        let mut ctx = test_ctx("u15");
        ctx.metadata
            .insert("scope_status.pro:all".into(), "active".into());
        ctx.metadata.insert("scopes".into(), "pro:all".into());
        enrich_auth_context_with_scopes(
            &mut ctx,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );

        assert!(!ctx.metadata.contains_key("scope_status.pro:all"));
        assert!(!ctx.metadata.contains_key("scopes"));
    }

    /// The same forgery exposure as the scope-status case, for the other
    /// three keys this function owns: `AuthContext::from_verified_jwt` stamps
    /// `scope_expires_at.*` straight from the JWT's own `scope_expires`
    /// claim, and a permanently-held or quota-less scope never gets its
    /// `scope_expires_at.*` / `quota_remaining.*` / `quota_pct.*` overwritten
    /// by the loop below — so a forged entry under one of those keys must be
    /// cleared up front rather than surviving because nothing recomputed it.
    #[test]
    fn claim_supplied_expiry_and_quota_metadata_does_not_survive_enrichment() {
        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();
        // "pro:all" is granted with no expiry and no quota definition, so
        // neither the expiry nor the quota branch below would overwrite a
        // forged entry for it.
        grant_with(&grants, "pro:all", "u16", Vec::new());

        let mut ctx = test_ctx("u16");
        ctx.metadata
            .insert("scope_expires_at.pro:all".into(), "9999999999".into());
        ctx.metadata
            .insert("quota_remaining.pro:all".into(), "1000000".into());
        ctx.metadata.insert("quota_pct.pro:all".into(), "0".into());
        enrich_auth_context_with_scopes(
            &mut ctx,
            &grants,
            &quotas,
            &[],
            None,
            INSIDE_BUSINESS_HOURS,
        );

        assert!(!ctx.metadata.contains_key("scope_expires_at.pro:all"));
        assert!(!ctx.metadata.contains_key("quota_remaining.pro:all"));
        assert!(!ctx.metadata.contains_key("quota_pct.pro:all"));
    }
}
