// SPDX-License-Identifier: BUSL-1.1

//! Verification-key resolution: cache lookup plus rate-limited on-demand
//! re-fetch when a `kid` is unknown, for both static-config and catalog
//! providers.

use tracing::warn;

use crate::config::auth::JwtProviderConfig;
use crate::control::security::jwks::fetch;
use crate::control::security::jwks::key::VerificationKey;
use crate::control::security::jwt::JwtError;

use super::state::{DecodedToken, JwksRegistry};

impl JwksRegistry {
    /// Resolve the verification key for a static-config provider, refetching
    /// from the provider's JWKS URL on cache miss (rate-limited).
    pub(super) async fn resolve_key(
        &self,
        provider: &JwtProviderConfig,
        decoded: &DecodedToken<'_>,
    ) -> Result<VerificationKey, JwtError> {
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let cache_identity = super::cache_identity::static_cache_identity(&provider.name);
        match self.cache.get(&cache_identity, kid) {
            Some(k) => Ok(k),
            None => {
                self.refetch_for_unknown_kid(provider, &cache_identity, kid)
                    .await
            }
        }
    }

    /// On-demand re-fetch for unknown `kid` against a static-config provider.
    async fn refetch_for_unknown_kid(
        &self,
        provider: &JwtProviderConfig,
        cache_identity: &str,
        kid: &str,
    ) -> Result<VerificationKey, JwtError> {
        if !self
            .cache
            .can_refetch(cache_identity, self.config.jwks_min_refetch_secs)
        {
            warn!(
                provider = %provider.name,
                kid = %kid,
                "unknown kid — re-fetch rate-limited"
            );
            return Err(JwtError::InvalidSignature);
        }

        self.cache.mark_refetch_attempted(cache_identity);
        fetch::fetch_and_cache(
            cache_identity,
            &provider.name,
            &provider.jwks_url,
            &self.cache,
            &self.policy,
        )
        .await;

        self.cache
            .get(cache_identity, kid)
            .ok_or(JwtError::InvalidSignature)
    }

    /// On-demand re-fetch for a catalog provider whose JWKS URI is supplied
    /// dynamically (not part of static config).
    pub(super) async fn refetch_catalog_key(
        &self,
        provider_name: &str,
        jwks_uri: &str,
        cache_identity: &str,
        kid: &str,
    ) -> Result<VerificationKey, JwtError> {
        if !self
            .cache
            .can_refetch(cache_identity, self.config.jwks_min_refetch_secs)
        {
            warn!(
                provider = %provider_name,
                kid = %kid,
                "unknown kid — re-fetch rate-limited (catalog provider)"
            );
            return Err(JwtError::InvalidSignature);
        }
        self.cache.mark_refetch_attempted(cache_identity);
        fetch::fetch_and_cache(
            cache_identity,
            provider_name,
            jwks_uri,
            &self.cache,
            &self.policy,
        )
        .await;
        self.cache
            .get(cache_identity, kid)
            .ok_or(JwtError::InvalidSignature)
    }
}
