// SPDX-License-Identifier: BUSL-1.1

//! Post-signature-verification claim checks and identity construction for
//! the static-provider path.

use crate::config::auth::JwtProviderConfig;
use crate::control::security::identity::{
    AuthenticatedIdentity, ExternalClaims, ExternalProviderBinding, identity_from_external_claims,
};
use crate::control::security::jwt::{JwtClaims, JwtError};
use crate::types::TenantId;

/// Validate the issuer and audience constraints of a selected static provider.
pub(super) fn validate_provider_claims(
    provider: &JwtProviderConfig,
    claims: &JwtClaims,
) -> Result<(), JwtError> {
    if claims.iss != provider.issuer {
        return Err(JwtError::InvalidIssuer);
    }
    // Exact equality against one element of the token's audience list. Never a
    // substring or prefix test: a token whose audience list carries an
    // unrelated value must not be accepted for this provider's audience.
    if !provider.audience.is_empty()
        && !claims
            .aud
            .iter()
            .any(|audience| audience == &provider.audience)
    {
        return Err(JwtError::InvalidAudience);
    }
    Ok(())
}

/// Build an `AuthenticatedIdentity` from a verified static-provider JWT.
///
/// Static-provider roles are parsed by [`Role::from_str`]. Tenant ownership comes
/// from the provider's server-side binding, never the JWT. The catalog path uses
/// [`crate::control::security::oidc`] instead, which applies stored
/// claim-mapping rules.
pub(super) fn build_identity(claims: &JwtClaims, tenant_id: u64) -> AuthenticatedIdentity {
    identity_from_external_claims(
        ExternalClaims {
            user_id: claims.user_id,
            subject: &claims.sub,
            role_names: &claims.roles,
            asserted_superuser: claims.is_superuser,
        },
        ExternalProviderBinding::default_database(TenantId::new(tenant_id)),
    )
}
