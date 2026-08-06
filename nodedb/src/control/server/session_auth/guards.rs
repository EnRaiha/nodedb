// SPDX-License-Identifier: BUSL-1.1

//! Post-identity authorization guards: blacklist, risk, and rate-limit checks.

use crate::control::security::audit::AuditEvent;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Check if a user is blacklisted. Returns `Err` if blocked.
///
/// Called after identity is resolved, before authorization.
pub fn check_blacklist(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    peer_addr: &str,
) -> crate::Result<()> {
    // Check user blacklist.
    let user_id = identity.user_id.to_string();
    if let Some(entry) = state.blacklist.check_user(&user_id) {
        state.audit_record(
            AuditEvent::AuthFailure,
            Some(identity.tenant_id),
            peer_addr,
            &format!(
                "blacklisted user '{}' denied: {}",
                identity.username, entry.reason
            ),
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("user blacklisted: {}", entry.reason),
        });
    }

    // Check IP blacklist.
    if let Some(entry) = state.blacklist.check_ip(peer_addr) {
        state.audit_record(
            AuditEvent::AuthFailure,
            Some(identity.tenant_id),
            peer_addr,
            &format!("blacklisted IP '{peer_addr}' denied: {}", entry.reason),
        );
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("IP blacklisted: {}", entry.reason),
        });
    }

    // Check auth user status (JIT-provisioned users).
    if let Some(status) = state.auth_users.get_status(&user_id) {
        let ctx_status = status;
        if matches!(
            ctx_status,
            crate::control::security::auth_context::AuthStatus::Suspended
                | crate::control::security::auth_context::AuthStatus::Banned
        ) {
            state.audit_record(
                AuditEvent::AuthFailure,
                Some(identity.tenant_id),
                peer_addr,
                &format!(
                    "auth user '{}' denied: account {}",
                    identity.username, ctx_status
                ),
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("account {ctx_status}"),
            });
        }
    }

    // Check org status overrides member status.
    // If any of the user's orgs is suspended/banned, block the user.
    let user_org_ids = state.orgs.orgs_for_user(&user_id);
    for org_id in &user_org_ids {
        if !state.orgs.is_active(org_id) {
            state.audit_record(
                AuditEvent::AuthFailure,
                Some(identity.tenant_id),
                peer_addr,
                &format!(
                    "org '{}' is not active — user '{}' blocked",
                    org_id, identity.username
                ),
            );
            return Err(crate::Error::RejectedAuthz {
                tenant_id: identity.tenant_id,
                resource: format!("organization '{org_id}' is suspended"),
            });
        }
    }

    Ok(())
}

/// Enforce the adaptive-auth risk decision for a request.
///
/// The score itself was computed once, in
/// [`RequestAuthScopeBuilder::build`](crate::control::security::request_scope::RequestAuthScopeBuilder::build),
/// where the transport's real client address was in hand; this guard only
/// turns the stamped `$auth.risk_score` into a refusal. Returns `Ok(())`
/// when scoring is disabled or the score is in the allow band.
///
/// Refusals use [`crate::Error::RejectedAuthz`] — the same authorization
/// rejection the blacklist and account-status guards raise on this path, so
/// clients see one consistent, non-retryable code for "this request is not
/// allowed" rather than a risk-specific status they would have to learn.
/// The reason string distinguishes the three cases (deny, step-up required,
/// unassessed).
pub fn check_risk(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    peer_addr: &str,
) -> crate::Result<()> {
    let Some(refusal) = state.risk_scorer.refusal_for(auth_ctx) else {
        return Ok(());
    };

    state.audit_record(
        AuditEvent::AuthFailure,
        Some(identity.tenant_id),
        peer_addr,
        &format!(
            "risk gate refused user '{}': {}",
            identity.username, refusal.audit_detail
        ),
    );

    // Escalation seam: a refusal here is exactly the repeated-violation
    // signal `EscalationEngine` consumes, and wiring it is one line —
    // `state.escalation.record_violation(&auth_ctx.id);` — placed right
    // here, before the error is returned, so a user who keeps tripping the
    // risk gate escalates to Suspended/Banned like any other violator.

    Err(crate::Error::RejectedAuthz {
        tenant_id: identity.tenant_id,
        resource: refusal.resource,
    })
}

/// Check rate limit for a request.
///
/// Called after identity and blacklist checks, before query execution.
/// Returns `Err(RateLimited)` if the request exceeds the rate limit.
///
/// Tenant and database QPS caps are read from the quota catalog when available.
/// Check order: user → org → tenant → database.
pub fn check_rate_limit(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    auth_ctx: &AuthContext,
    operation: &str,
    database_id: nodedb_types::DatabaseId,
) -> crate::Result<crate::control::security::ratelimit::limiter::RateLimitResult> {
    use crate::control::security::ratelimit::limiter::QuotaCheckParams;

    let plan_tier = auth_ctx.metadata.get("plan").map(|s| s.as_str());

    // Resolve tenant and database QPS caps from the quota catalog if available.
    let quota_params = {
        let catalog = state.credentials.catalog();
        let tenant_max_qps = catalog
            .get_tenant_quota(database_id, identity.tenant_id)
            .ok()
            .flatten()
            .and_then(|r| {
                if r.max_qps > 0 {
                    Some(r.max_qps as u64)
                } else {
                    None
                }
            });

        let database_max_qps = catalog
            .get_database_quota(database_id)
            .ok()
            .flatten()
            .and_then(|r| {
                if r.max_qps > 0 {
                    Some(r.max_qps as u64)
                } else {
                    None
                }
            });

        if tenant_max_qps.is_some() || database_max_qps.is_some() {
            Some(QuotaCheckParams {
                tenant_max_qps,
                database_max_qps,
                tenant_id: identity.tenant_id,
                database_id,
            })
        } else {
            None
        }
    };

    let result = state.rate_limiter.check(
        &identity.user_id.to_string(),
        &auth_ctx.org_ids,
        plan_tier,
        operation,
        quota_params.as_ref(),
    );

    if !result.allowed {
        return Err(crate::Error::RateExceeded {
            gate: operation.to_string(),
            detail: format!("rate limited for user {}", identity.user_id),
            retry_after_ms: result.retry_after_secs.saturating_mul(1000),
        });
    }

    Ok(result)
}
