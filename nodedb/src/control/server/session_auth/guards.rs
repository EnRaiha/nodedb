// SPDX-License-Identifier: BUSL-1.1

//! Post-identity authorization guards: blacklist and rate-limit checks.

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
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: format!("rate limited: retry after {}s", result.retry_after_secs),
        });
    }

    Ok(result)
}
