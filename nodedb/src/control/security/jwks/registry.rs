// SPDX-License-Identifier: BUSL-1.1

//! Multi-provider JWKS registry: routes JWT tokens to the correct provider,
//! fetches keys on demand, and validates signatures.
//!
//! All three public entry points (`validate`, `validate_with_provider`,
//! `validate_with_catalog_provider`) share the same token-decoding,
//! signature-verification, and time-claim-validation pipeline; they differ
//! only in how the verification key is resolved. The shared pipeline lives
//! in [`Self::decode_unverified`] and [`Self::verify_signature_and_time`].

mod cache_identity;
mod header;
mod verified;

use std::sync::Arc;

use cache_identity::{catalog_cache_identity, static_cache_identity};
use header::{JwtHeader, decode_jwt_header};
use tracing::{debug, warn};

pub use verified::VerifiedJwtClaims;

use crate::config::auth::{JwtAuthConfig, JwtProviderConfig};
use crate::control::security::identity::{
    AuthenticatedIdentity, ExternalClaims, ExternalProviderBinding, identity_from_external_claims,
};
use crate::control::security::jwt::{JwtClaims, JwtError, validate_time_claims};
use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::cache::JwksCache;
use super::key::{VerificationKey, verify_signature};

/// Multi-provider JWKS registry.
///
/// Manages providers, caches keys, and validates JWT tokens.
/// Lives on the Control Plane (Send + Sync).
pub struct JwksRegistry {
    providers: Vec<JwtProviderConfig>,
    cache: Arc<JwksCache>,
    config: JwtAuthConfig,
    policy: Arc<super::url::JwksPolicy>,
    /// Background refresh task handle.
    _refresh_handle: Option<tokio::task::JoinHandle<()>>,
}

/// JWT broken into its three base64url-encoded parts plus the decoded
/// header and payload. Produced by [`JwksRegistry::decode_unverified`].
///
/// The `parts` slices borrow from the original token string and are reused
/// when reconstructing the signing input for signature verification — no
/// re-split, no re-decode.
struct DecodedToken<'a> {
    parts: [&'a str; 3],
    header: JwtHeader,
    claims: JwtClaims,
}

impl JwksRegistry {
    /// Create and initialize the registry.
    ///
    /// Fetches JWKS from all providers on startup, loads disk cache as fallback,
    /// and spawns the periodic refresh task.
    pub async fn init(config: JwtAuthConfig) -> crate::Result<Self> {
        // Registry construction is also a public entry point, so it must not
        // rely on the server-config loader to reject unsafe static providers.
        // Validate before creating cache state, fetching remote keys, or
        // spawning a refresh task.
        config.validate()?;
        let policy = Arc::new(config.jwks_policy().map_err(|e| crate::Error::Config {
            detail: format!("auth.jwt allow-list is invalid: {e}"),
        })?);
        let cache = Arc::new(JwksCache::new(config.jwks_cache_path.clone()));

        // Load disk cache first (offline fallback).
        cache.load_from_disk();

        // Fetch from all providers (best-effort — failures use disk cache).
        for provider in &config.providers {
            let cache_identity = static_cache_identity(&provider.name);
            super::fetch::fetch_and_cache(
                &cache_identity,
                &provider.name,
                &provider.jwks_url,
                &cache,
                &policy,
            )
            .await;
        }

        // Spawn periodic refresh.
        let refresh_handle = if !config.providers.is_empty() {
            let pairs: Vec<(String, String, String)> = config
                .providers
                .iter()
                .map(|p| {
                    (
                        static_cache_identity(&p.name),
                        p.name.clone(),
                        p.jwks_url.clone(),
                    )
                })
                .collect();
            Some(super::fetch::spawn_refresh_task(
                pairs,
                cache.clone(),
                config.jwks_refresh_secs,
                policy.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            providers: config.providers.clone(),
            cache,
            config,
            policy,
            _refresh_handle: refresh_handle,
        })
    }

    /// Validate a JWT token using JWKS, routing by the `iss` and `aud` claims.
    ///
    /// Flow:
    /// 1. Decode header + payload (no signature) via [`Self::decode_unverified`].
    /// 2. Match `iss` and `aud` to a configured provider via [`Self::find_provider`].
    /// 3. Resolve the verification key (cache lookup + on-demand re-fetch).
    /// 4. Verify signature, `exp`, `nbf` via [`Self::verify_signature_and_time`].
    /// 5. Validate `iss`, `aud` against the matched provider.
    /// 6. Build and return an `AuthenticatedIdentity` bound to that provider's tenant.
    pub async fn validate(&self, token: &str) -> Result<AuthenticatedIdentity, JwtError> {
        self.validate_with_claims(token)
            .await
            .map(|(identity, _)| identity)
    }

    /// Validate a JWT and retain an opaque proof for rich session claims.
    pub(crate) async fn validate_with_claims(
        &self,
        token: &str,
    ) -> Result<(AuthenticatedIdentity, VerifiedJwtClaims), JwtError> {
        let decoded = self.decode_unverified(token)?;
        let provider = self.find_provider(&decoded.claims.iss, &decoded.claims.aud)?;
        let key = self.resolve_key(provider, &decoded).await?;
        self.verify_signature_and_time(&decoded, &key, &provider.name)?;
        validate_provider_claims(provider, &decoded.claims)?;

        let mut claims = decoded.claims;
        self.apply_claim_policy(&mut claims)?;
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let identity = build_identity(&claims, provider.tenant_id);

        debug!(
            username = %identity.username,
            tenant_id = provider.tenant_id,
            provider = %provider.name,
            kid = %kid,
            "JWKS JWT validated"
        );

        Ok((identity, VerifiedJwtClaims(claims)))
    }

    /// Validate a JWT using a named catalog provider whose JWKS endpoint is
    /// provided dynamically (catalog OIDC providers not in the static config).
    ///
    /// Catalog keysets use a separate cache identity bound to their endpoint,
    /// so they cannot reuse a static provider's keys or a prior endpoint's
    /// keys after a catalog provider is recreated.
    pub async fn validate_with_catalog_provider(
        &self,
        provider_name: &str,
        jwks_uri: &str,
        token: &str,
    ) -> Result<VerifiedJwtClaims, JwtError> {
        let decoded = self.decode_unverified(token)?;
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let cache_identity = catalog_cache_identity(provider_name, jwks_uri);
        let key = match self.cache.get(&cache_identity, kid) {
            Some(k) => k,
            None => {
                self.refetch_catalog_key(provider_name, jwks_uri, &cache_identity, kid)
                    .await?
            }
        };
        self.verify_signature_and_time(&decoded, &key, provider_name)?;

        let mut claims = decoded.claims;
        self.apply_claim_policy(&mut claims)?;

        debug!(
            provider = %provider_name,
            kid = %kid,
            sub = %claims.sub,
            "JWKS JWT validated via catalog provider"
        );
        Ok(VerifiedJwtClaims(claims))
    }

    /// Check if any providers are configured.
    pub fn is_configured(&self) -> bool {
        !self.providers.is_empty()
    }

    /// The `[auth.jwt]` section this registry was built from.
    ///
    /// Exposed so the post-verification gate that needs server state
    /// (`jwt_policy::enforce_stateful_jwt_policy`) reads the same config the
    /// verification pipeline does, instead of a second copy that could drift.
    pub(crate) fn jwt_config(&self) -> &JwtAuthConfig {
        &self.config
    }

    /// Apply the config-only claim policy to a token that has passed
    /// signature, route, and time validation: remap the provider's claim names
    /// onto the fields NodeDB reads, then refuse a token whose status claim
    /// carries a blocked value.
    ///
    /// Both bearer routes converge here — the HTTP static-provider path via
    /// [`Self::validate_with_claims`] and the native/OIDC catalog path via
    /// [`Self::validate_with_catalog_provider`] — so neither can skip it.
    fn apply_claim_policy(&self, claims: &mut JwtClaims) -> Result<(), JwtError> {
        crate::control::security::jwt_policy::remap_claims(&self.config.claims, claims);
        crate::control::security::jwt_policy::check_blocked_status(
            self.config.status_claim.as_deref(),
            &self.config.blocked_statuses,
            claims,
        )
    }

    /// Re-check the `exp` (and the rest of the time-claim envelope) of
    /// previously verified claims against the current clock.
    ///
    /// `exp` is validated once, inside [`Self::verify_signature_and_time`],
    /// at the moment a token is authenticated. A caller that retains a
    /// [`VerifiedJwtClaims`] beyond that single check — e.g. a native
    /// session, which keeps it for the connection's lifetime to re-derive
    /// `$auth.*` enrichment on every request — must call this once per use
    /// so a token that expires mid-connection is caught instead of being
    /// re-applied indefinitely. Reuses [`validate_time_claims`] with this
    /// registry's configured clock-skew tolerance — the exact comparison and
    /// skew allowance the original authentication used.
    pub(crate) fn check_not_expired(&self, verified: &VerifiedJwtClaims) -> Result<(), JwtError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        validate_time_claims(
            verified.claims(),
            now,
            self.config.clock_skew_secs,
            self.config.max_token_lifetime_secs,
        )
    }

    // ── Internal pipeline ───────────────────────────────────────────────

    /// Split the token, decode the header + payload, and check that the
    /// algorithm is non-`none` and on the allow-list. Does NOT verify the
    /// signature, the `iss`, the `aud`, or the time claims.
    fn decode_unverified<'a>(&self, token: &'a str) -> Result<DecodedToken<'a>, JwtError> {
        let raw: Vec<&str> = token.split('.').collect();
        if raw.len() != 3 {
            return Err(JwtError::MalformedToken);
        }
        let parts = [raw[0], raw[1], raw[2]];

        let header = decode_jwt_header(parts[0])?;

        // Check algorithm.
        if header.alg == "none" {
            return Err(JwtError::UnsupportedAlgorithm);
        }
        if !self.config.allowed_algorithms.is_empty()
            && !self
                .config
                .allowed_algorithms
                .iter()
                .any(|a| a == &header.alg)
        {
            return Err(JwtError::UnsupportedAlgorithm);
        }

        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        let claims: JwtClaims = crate::util::bounded_json::from_slice(&payload_bytes)
            .map_err(|_| JwtError::InvalidClaims)?;

        Ok(DecodedToken {
            parts,
            header,
            claims,
        })
    }

    /// Verify signature + `exp` + `nbf`. Assumes the algorithm has already
    /// been allow-listed by [`Self::decode_unverified`]. The `provider_name`
    /// is used only for log context on rejection.
    fn verify_signature_and_time(
        &self,
        decoded: &DecodedToken<'_>,
        key: &VerificationKey,
        provider_name: &str,
    ) -> Result<(), JwtError> {
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        if key.algorithm != decoded.header.alg {
            // HMAC-when-RSA-expected attack prevention.
            warn!(
                expected = %key.algorithm,
                actual = %decoded.header.alg,
                kid = %kid,
                provider = %provider_name,
                "JWT algorithm mismatch — possible algorithm confusion attack"
            );
            return Err(JwtError::UnsupportedAlgorithm);
        }

        let signing_input = format!("{}.{}", decoded.parts[0], decoded.parts[1]);
        let signature = base64_url_decode(decoded.parts[2]).ok_or(JwtError::DecodingError)?;
        if !verify_signature(key, signing_input.as_bytes(), &signature) {
            return Err(JwtError::InvalidSignature);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        validate_time_claims(
            &decoded.claims,
            now,
            self.config.clock_skew_secs,
            self.config.max_token_lifetime_secs,
        )
    }

    /// Resolve the verification key for a static-config provider, refetching
    /// from the provider's JWKS URL on cache miss (rate-limited).
    async fn resolve_key(
        &self,
        provider: &JwtProviderConfig,
        decoded: &DecodedToken<'_>,
    ) -> Result<VerificationKey, JwtError> {
        let kid = decoded.header.kid.as_deref().unwrap_or("");
        let cache_identity = static_cache_identity(&provider.name);
        match self.cache.get(&cache_identity, kid) {
            Some(k) => Ok(k),
            None => {
                self.refetch_for_unknown_kid(provider, &cache_identity, kid)
                    .await
            }
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
    fn find_provider(&self, issuer: &str, audience: &str) -> Result<&JwtProviderConfig, JwtError> {
        if issuer.is_empty() {
            return Err(JwtError::InvalidIssuer);
        }

        let mut issuer_matched = false;
        let mut wildcard_provider = None;
        for provider in &self.providers {
            if provider.issuer == issuer {
                issuer_matched = true;
                if provider.audience == audience {
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
        super::fetch::fetch_and_cache(
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
    async fn refetch_catalog_key(
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
        super::fetch::fetch_and_cache(
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

/// Validate the issuer and audience constraints of a selected static provider.
fn validate_provider_claims(
    provider: &JwtProviderConfig,
    claims: &JwtClaims,
) -> Result<(), JwtError> {
    if claims.iss != provider.issuer {
        return Err(JwtError::InvalidIssuer);
    }
    if !provider.audience.is_empty() && claims.aud != provider.audience {
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
fn build_identity(claims: &JwtClaims, tenant_id: u64) -> AuthenticatedIdentity {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::auth::JwtAuthConfig;

    fn claims(iat: u64, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 999,
            roles: Vec::new(),
            exp,
            nbf: 0,
            iat,
            iss: String::new(),
            aud: String::new(),
            user_id: 1,
            is_superuser: false,
            extra: std::collections::HashMap::new(),
        }
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("test clock must be after epoch")
            .as_secs()
    }

    /// A native session retains `VerifiedJwtClaims` for the connection's
    /// lifetime (see `native::session::request::handle_request`) and must
    /// re-check `exp` on every request via this method, instead of trusting
    /// the one-time check `verify_signature_and_time` ran at authentication.
    /// A session whose stored claims have since expired must be rejected.
    #[tokio::test]
    async fn check_not_expired_rejects_claims_past_exp() {
        let registry = JwksRegistry::init(JwtAuthConfig::default())
            .await
            .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        let expired = VerifiedJwtClaims(claims(now - 2_000, now - 1_000));

        assert_eq!(registry.check_not_expired(&expired), Err(JwtError::Expired));
    }

    /// A session whose stored claims have not expired keeps passing the
    /// check request after request — no regression of the claim-enrichment
    /// path this check now gates.
    #[tokio::test]
    async fn check_not_expired_accepts_claims_before_exp() {
        let registry = JwksRegistry::init(JwtAuthConfig::default())
            .await
            .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        let valid = VerifiedJwtClaims(claims(now - 10, now + 3_600));

        assert_eq!(registry.check_not_expired(&valid), Ok(()));
    }

    /// Parse an `[auth.jwt]` section exactly as the server-config loader does,
    /// so these tests prove the knobs travel from a config file into the
    /// verification pipeline — not merely that a hand-built struct works.
    fn config_from_toml(body: &str) -> JwtAuthConfig {
        let parsed: JwtAuthConfig =
            toml::from_str(body).expect("the [auth.jwt] section must deserialize");
        parsed.validate().expect("the section must be valid");
        parsed
    }

    fn claims_with_extra(pairs: &[(&str, serde_json::Value)]) -> JwtClaims {
        let mut claims = claims(1, 9_999_999_999);
        for (key, value) in pairs {
            claims.extra.insert((*key).to_owned(), value.clone());
        }
        claims
    }

    /// The assertion that would have caught the original defect: a
    /// `status_claim` / `blocked_statuses` pair supplied through server config
    /// must actually reject a token carrying a blocked value.
    #[tokio::test]
    async fn blocked_status_from_server_config_rejects_the_token() {
        let registry = JwksRegistry::init(config_from_toml(
            r#"
            status_claim = "account_status"
            blocked_statuses = ["suspended", "banned"]
            "#,
        ))
        .await
        .expect("registry with no configured providers must still initialize");

        let mut blocked = claims_with_extra(&[("account_status", serde_json::json!("suspended"))]);
        assert_eq!(
            registry.apply_claim_policy(&mut blocked),
            Err(JwtError::BlockedStatus)
        );

        let mut allowed = claims_with_extra(&[("account_status", serde_json::json!("active"))]);
        assert_eq!(registry.apply_claim_policy(&mut allowed), Ok(()));

        // A token that never carries the claim is not blocked.
        let mut absent = claims_with_extra(&[]);
        assert_eq!(registry.apply_claim_policy(&mut absent), Ok(()));
    }

    /// A `[auth.jwt.claims]` table supplied through server config must move a
    /// provider-named claim onto the field NodeDB reads.
    #[tokio::test]
    async fn claim_remap_from_server_config_reaches_the_verification_pipeline() {
        let registry = JwksRegistry::init(config_from_toml(
            r#"
            [claims]
            upn = "email"
            "#,
        ))
        .await
        .expect("registry with no configured providers must still initialize");

        let mut remapped = claims_with_extra(&[("upn", serde_json::json!("alice@example.com"))]);
        assert_eq!(registry.apply_claim_policy(&mut remapped), Ok(()));
        assert_eq!(
            remapped.extra.get("email").and_then(|v| v.as_str()),
            Some("alice@example.com")
        );
    }

    /// Remapping runs before the status check, so an operator may block on a
    /// provider claim after renaming it onto `status`.
    #[tokio::test]
    async fn remap_feeds_the_status_check() {
        let registry = JwksRegistry::init(config_from_toml(
            r#"
            status_claim = "status"
            blocked_statuses = ["deactivated"]

            [claims]
            acct_state = "status"
            "#,
        ))
        .await
        .expect("registry with no configured providers must still initialize");

        let mut claims = claims_with_extra(&[("acct_state", serde_json::json!("deactivated"))]);
        assert_eq!(
            registry.apply_claim_policy(&mut claims),
            Err(JwtError::BlockedStatus)
        );
    }

    /// `check_not_expired` must apply the registry's own configured clock
    /// skew — the same tolerance `verify_signature_and_time` used at
    /// authentication — not a hand-rolled or zero tolerance.
    #[tokio::test]
    async fn check_not_expired_honors_configured_clock_skew() {
        let registry = JwksRegistry::init(JwtAuthConfig {
            clock_skew_secs: 120,
            ..JwtAuthConfig::default()
        })
        .await
        .expect("registry with no configured providers must still initialize");
        let now = now_secs();
        // Expired 60s ago: within the 120s skew tolerance, so still accepted.
        let just_expired = VerifiedJwtClaims(claims(now - 200, now - 60));

        assert_eq!(registry.check_not_expired(&just_expired), Ok(()));
    }
}
