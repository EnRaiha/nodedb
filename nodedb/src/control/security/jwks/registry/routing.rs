// SPDX-License-Identifier: BUSL-1.1

//! Provider routing: match a token's issuer and audience against the
//! configured static providers.

use crate::config::auth::JwtProviderConfig;
use crate::control::security::bearer::BearerAttempt;
use crate::control::security::jwt::{JwtClaims, JwtError};
use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::state::JwksRegistry;

impl JwksRegistry {
    /// Record into `attempt` the `sub` a token claims and the provider its `iss` and `aud` route to.
    ///
    /// Nothing here is verified, so the values only label the audit entry.
    /// A token whose payload does not decode leaves `attempt` unchanged.
    pub(crate) fn label_unverified(&self, token: &str, attempt: &mut BearerAttempt<'_>) {
        let mut parts = token.split('.');
        let (Some(_header), Some(payload), Some(_signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let Some(payload_bytes) = base64_url_decode(payload) else {
            return;
        };
        let Ok(claims) = crate::util::bounded_json::from_slice::<JwtClaims>(&payload_bytes) else {
            return;
        };
        attempt.set_claimed_sub(&claims.sub);
        if let Ok(provider) = self.find_provider(&claims.iss, &claims.aud) {
            attempt.set_provider(&provider.name, Some(TenantId::new(provider.tenant_id)));
        }
    }

    /// Find the provider matching a token's issuer and audience.
    ///
    /// Static configuration validation ensures a route is unique. A provider
    /// with an empty audience is a wildcard only when it is the sole provider
    /// for its issuer; validation forbids it from sharing that issuer. There
    /// is no single-provider fallback for a token whose issuer is empty or
    /// does not match a configured provider. For a known issuer with a
    /// mismatched audience, return `InvalidAudience` rather than accepting
    /// the first provider.
    ///
    /// `audience` is the token's full `aud` list (RFC 7519 permits several).
    pub(super) fn find_provider(
        &self,
        issuer: &str,
        audience: &[String],
    ) -> Result<&JwtProviderConfig, JwtError> {
        if issuer.is_empty() {
            return Err(JwtError::InvalidIssuer);
        }

        let mut issuer_matched = false;
        let mut wildcard_provider = None;
        for provider in &self.providers {
            if provider.issuer == issuer {
                issuer_matched = true;
                // Routing matches on exact equality against one element of the
                // token's audience list — never a substring, prefix, or
                // joined-string test, which would route a token issued for an
                // unrelated audience to this provider.
                if audience.iter().any(|value| value == &provider.audience) {
                    return Ok(provider);
                }
                if provider.audience.is_empty() {
                    wildcard_provider = Some(provider);
                }
            }
        }

        match (issuer_matched, wildcard_provider) {
            (_, Some(provider)) => Ok(provider),
            (true, None) => Err(JwtError::InvalidAudience),
            (false, None) => Err(JwtError::InvalidIssuer),
        }
    }
}
