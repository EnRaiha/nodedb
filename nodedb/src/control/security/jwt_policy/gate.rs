// SPDX-License-Identifier: BUSL-1.1

//! The single post-verification gate for `[auth.jwt]` policy that needs server
//! state, applied on every bearer route after a token has been verified and
//! bound to an identity.
//!
//! Config-only policy (claim remapping, blocked status values) runs earlier,
//! inside the JWKS registry, so it applies to every caller of the registry.
//! What lives here needs the scope definitions and auth-user store that only
//! [`SharedState`] carries, so it hangs off the two call sites that own one:
//! the HTTP bearer path and the native/OIDC bearer path.

use crate::control::security::jwt::JwtClaims;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::{provisioning, scopes};

/// Apply the state-dependent half of the JWT policy to a verified token.
///
/// `tenant_id` is the tenant the *identity* was bound to by its provider —
/// never a tenant asserted by the token's claims.
///
/// A deployment with no JWKS registry has no JWT authentication at all, so
/// there is no policy to apply and the gate is a no-op.
pub fn enforce_stateful_jwt_policy(
    state: &SharedState,
    claims: &JwtClaims,
    tenant_id: TenantId,
) -> crate::Result<()> {
    let Some(registry) = state.jwks_registry.as_ref() else {
        return Ok(());
    };
    let config = registry.jwt_config();

    scopes::enforce_declared_scopes(config.enforce_scopes, &state.scope_defs, claims, tenant_id)?;
    provisioning::provision_and_check_status(
        &state.auth_users,
        Some(&state.orgs),
        config,
        claims,
        tenant_id,
    )
}
