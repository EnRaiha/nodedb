// SPDX-License-Identifier: BUSL-1.1

//! Provider routing: match a token's issuer and audience against the
//! configured static providers.

use crate::config::auth::JwtProviderConfig;
use crate::control::security::jwt::JwtError;

use super::state::JwksRegistry;

impl JwksRegistry {
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
