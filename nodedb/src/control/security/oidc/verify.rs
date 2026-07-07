// SPDX-License-Identifier: BUSL-1.1

//! OIDC bearer-token verification entry point.
//!
//! Decodes the token header + payload (without verifying the signature) to
//! read the `iss` claim, resolves the matching OIDC provider from the catalog,
//! delegates signature verification to `JwksRegistry`, applies claim mapping,
//! and constructs an ephemeral `AuthenticatedIdentity`.
//!
//! pgwire does NOT support OIDC bearer tokens (SCRAM-SHA-256 only).
//! Use the native protocol or HTTP for OIDC.

use nodedb_types::id::DatabaseId;
use tracing::debug;

use crate::control::security::identity::database_set::DatabaseSet;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::security::jwt::JwtError;
use crate::control::security::util::base64_url_decode;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::claim_mapping::apply_claim_mapping;

/// Verify an OIDC bearer token and return an ephemeral `AuthenticatedIdentity`.
///
/// Steps:
/// 1. Decode header + payload (no signature) to extract `iss`.
/// 2. Look up provider by `iss` in the catalog (`_system.oidc_providers`).
/// 3. Verify signature via `JwksRegistry::validate_with_provider`.
/// 4. Validate `aud` (if the provider has an expected audience), `exp`, `nbf`.
/// 5. Apply claim-mapping rules to derive `default_database`, `accessible_databases`, `roles`.
/// 6. Construct `AuthenticatedIdentity` with `auth_method = OidcBearer`.
pub async fn verify_bearer_token(
    state: &SharedState,
    token: &str,
) -> crate::Result<AuthenticatedIdentity> {
    // 1. Decode payload (no sig) to read `iss`.
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(jwt_error_to_crate_error(JwtError::MalformedToken));
    }
    let payload_bytes = base64_url_decode(parts[1])
        .ok_or_else(|| jwt_error_to_crate_error(JwtError::DecodingError))?;
    let claims: crate::control::security::jwt::JwtClaims = sonic_rs::from_slice(&payload_bytes)
        .map_err(|_| jwt_error_to_crate_error(JwtError::InvalidClaims))?;

    let iss = &claims.iss;

    // 2. Look up provider by `iss`.
    let catalog = state.credentials.catalog();
    let provider = catalog
        .list_oidc_providers()
        .map_err(|_| jwt_error_to_crate_error(JwtError::InvalidIssuer))?
        .into_iter()
        .find(|p| p.issuer == *iss)
        .ok_or_else(|| crate::Error::OidcUnknownProvider { iss: iss.clone() })?;

    // 3. Verify signature via JwksRegistry using the catalog-provided JWKS URI.
    let jwks = state
        .jwks_registry
        .as_ref()
        .ok_or_else(|| jwt_error_to_crate_error(JwtError::UnsupportedAlgorithm))?;
    let verified_claims = jwks
        .validate_with_catalog_provider(&provider.provider_name, &provider.jwks_uri, token)
        .await
        .map_err(jwt_error_to_crate_error)?;

    // 4. Validate audience if configured.
    if let Some(ref expected_aud) = provider.audience
        && verified_claims.aud != *expected_aud
    {
        return Err(jwt_error_to_crate_error(JwtError::InvalidAudience));
    }

    // 5. Apply claim mapping.
    let mapping = apply_claim_mapping(&verified_claims, &provider.claim_mapping);

    // Build the accessible-database set. The default database MUST be set
    // by a matching claim-mapping rule — there is no silent fallback to
    // `DatabaseId::DEFAULT`. An OIDC user whose claims match no rule that
    // assigns a database is rejected here so operators see the gap instead
    // of silently routing the session to the system default.
    let default_db = mapping
        .default_database
        .map(DatabaseId::new)
        .ok_or_else(|| crate::Error::OidcNoDefaultDatabase {
            sub: verified_claims.sub.clone(),
        })?;

    let mut accessible: smallvec::SmallVec<[DatabaseId; 4]> = smallvec::smallvec![default_db];
    for &db_raw in &mapping.accessible_databases {
        let db = DatabaseId::new(db_raw);
        if !accessible.contains(&db) {
            accessible.push(db);
        }
    }

    // Map role strings to Role enum values. `Role::from_str` is infallible
    // (unknown names land in `Role::Custom`), so destructure the Result
    // without a phantom fallback that the type system says cannot fire.
    let roles: Vec<Role> = mapping
        .roles
        .iter()
        .map(|r| match r.parse::<Role>() {
            Ok(role) => role,
            Err(never) => match never {},
        })
        .collect();

    let username = if verified_claims.sub.is_empty() {
        format!("oidc_{}", verified_claims.user_id)
    } else {
        verified_claims.sub.clone()
    };

    debug!(
        provider = %provider.provider_name,
        sub = %verified_claims.sub,
        iss = %verified_claims.iss,
        default_db = %default_db.as_u64(),
        "OIDC login succeeded"
    );

    Ok(AuthenticatedIdentity {
        // Use a sentinel range for OIDC ephemeral identities to avoid colliding
        // with trust-mode user_id == 0 checks. The real user record, if any,
        // is identified by the `sub` claim's username.
        user_id: verified_claims.user_id,
        username,
        tenant_id: TenantId::new(verified_claims.tenant_id),
        auth_method: AuthMethod::OidcBearer,
        roles,
        is_superuser: false,
        default_database: Some(default_db),
        accessible_databases: DatabaseSet::Some(accessible),
    })
}

/// Map a `JwtError` to a `crate::Error` with the correct semantics for the
/// OIDC bearer-token path:
/// - `Expired` surfaces as `SessionTokenExpired` so callers can distinguish
///   token-expired from auth-rejected.
/// - All other errors become `BadRequest` (client sent an invalid token).
fn jwt_error_to_crate_error(e: JwtError) -> crate::Error {
    match e {
        JwtError::Expired => crate::Error::SessionTokenExpired,
        JwtError::MalformedToken
        | JwtError::InvalidClaims
        | JwtError::DecodingError
        | JwtError::InvalidSignature
        | JwtError::NotYetValid
        | JwtError::InvalidIssuer
        | JwtError::InvalidAudience
        | JwtError::UnsupportedAlgorithm => crate::Error::BadRequest {
            detail: e.to_string(),
        },
    }
}
