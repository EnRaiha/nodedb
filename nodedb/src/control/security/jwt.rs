// SPDX-License-Identifier: BUSL-1.1

//! JWT (JSON Web Token) bearer token authentication.
//!
//! Validates JWTs presented as `Authorization: Bearer <token>` headers
//! or as `password` in pgwire authentication. Supports:
//!
//! - HS256 (HMAC-SHA256) for shared-secret deployments
//! - RS256 (RSA-SHA256) for public-key deployments
//! - Required, bounded token lifetime (`iat` + `exp` claims)
//! - Tenant isolation (server-owned provider binding)
//! - Role mapping (`roles` claim → NodeDB roles)
//!
//! The JWT secret/public key is configured per cluster. Tokens are
//! stateless — no server-side session storage required.

use std::time::{SystemTime, UNIX_EPOCH};

use tracing::debug;

use crate::control::security::util::base64_url_decode;
use crate::types::TenantId;

use super::identity::{
    AuthenticatedIdentity, ExternalClaims, ExternalProviderBinding, identity_from_external_claims,
};

/// Signature algorithm pinned by a static JWT provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtAlgorithm {
    Hs256,
    Rs256,
}

impl JwtAlgorithm {
    fn header_name(self) -> &'static str {
        match self {
            Self::Hs256 => "HS256",
            Self::Rs256 => "RS256",
        }
    }
}

/// JWT validation configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// The one accepted algorithm for this provider. Required when enabled.
    pub algorithm: Option<JwtAlgorithm>,
    /// HMAC secret for HS256 verification (raw bytes).
    /// If empty, HS256 is disabled.
    pub hmac_secret: Vec<u8>,
    /// RSA public key for RS256 verification (DER-encoded PKCS#8 or PKCS#1).
    /// If empty, RS256 is disabled.
    pub rsa_public_key_der: Vec<u8>,
    /// Expected issuer (`iss` claim). Empty = don't validate.
    pub expected_issuer: String,
    /// Expected audience (`aud` claim). Empty = don't validate.
    pub expected_audience: String,
    /// Clock skew tolerance in seconds for `exp`/`nbf` validation.
    pub clock_skew_seconds: u64,
    /// Server-owned tenant binding for this static JWT provider.
    /// Tokens are rejected when this is absent; `claims.tenant_id` is never authoritative.
    pub tenant_id: Option<u64>,
    /// Maximum accepted `exp - iat` lifetime in seconds.
    pub max_token_lifetime_seconds: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            algorithm: None,
            hmac_secret: Vec::new(),
            rsa_public_key_der: Vec::new(),
            expected_issuer: String::new(),
            expected_audience: String::new(),
            clock_skew_seconds: 60,
            tenant_id: None,
            max_token_lifetime_seconds: 86_400,
        }
    }
}

/// JWT header (the first base64url-encoded segment).
#[derive(Debug, serde::Deserialize)]
struct JwtHeader {
    /// Algorithm: "HS256" or "RS256".
    alg: String,
}

/// Decoded JWT claims (the payload after verification).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct JwtClaims {
    /// Subject: typically user_id or username.
    pub sub: String,
    /// Legacy tenant assertion. Parsed but never authoritative.
    #[serde(default)]
    pub tenant_id: u64,
    /// Roles as string array.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Expiration time (Unix timestamp).
    #[serde(default)]
    pub exp: u64,
    /// Not-before time (Unix timestamp).
    #[serde(default)]
    pub nbf: u64,
    /// Issued-at time.
    #[serde(default)]
    pub iat: u64,
    /// Issuer.
    #[serde(default)]
    pub iss: String,
    /// Audience. RFC 7519 allows either a single string or an array of
    /// strings; both deserialize into this list so an array-audience token
    /// reaches audience matching instead of being rejected as malformed.
    #[serde(default, deserialize_with = "deserialize_audience")]
    pub aud: Vec<String>,
    /// User ID (NodeDB-specific claim).
    #[serde(default)]
    pub user_id: u64,
    /// Legacy external superuser assertion.
    ///
    /// Parsed for compatibility and security telemetry, but never grants
    /// NodeDB superuser authority.
    #[serde(default)]
    pub is_superuser: bool,
    /// Extended claims not covered by the standard fields above.
    ///
    /// Captures provider-specific claims (email, org_id, groups, permissions,
    /// status, metadata) that verified JWT context construction maps to
    /// session variables. Different providers use different claim names — the
    /// `[auth.jwt.claims]` config section renames them onto the fields read
    /// here, applied by `jwt_policy::remap_claims` from the JWKS registry
    /// immediately after signature, route, and time validation.
    ///
    /// Because the payload is flattened, nested provider claims land under
    /// their outermost key — read them through `jwt_policy::resolve_claim`,
    /// which resolves an exact key first and a dotted path second, rather than
    /// indexing this map directly.
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Accept both RFC 7519 shapes of the `aud` claim — `"aud": "x"` and
/// `"aud": ["x", "y"]` — as one list.
///
/// A hand-written visitor rather than an untagged enum: the claim set is
/// deserialized through serde's flatten buffer, and a visitor keeps the
/// accepted shapes explicit instead of depending on untagged fallthrough.
fn deserialize_audience<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct AudienceVisitor;

    impl<'de> serde::de::Visitor<'de> for AudienceVisitor {
        type Value = Vec<String>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JWT audience: a string or an array of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(vec![value.to_owned()])
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(vec![value])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut audiences = Vec::with_capacity(seq.size_hint().unwrap_or(1));
            while let Some(entry) = seq.next_element::<String>()? {
                audiences.push(entry);
            }
            Ok(audiences)
        }
    }

    deserializer.deserialize_any(AudienceVisitor)
}

/// JWT validator.
pub struct JwtValidator {
    config: JwtConfig,
}

impl JwtValidator {
    pub fn new(config: JwtConfig) -> Self {
        Self { config }
    }

    /// Validate a JWT token string and extract the authenticated identity.
    ///
    /// Performs:
    /// 1. Base64 decode header + payload + signature
    /// 2. HMAC-SHA256 signature verification (if configured)
    /// 3. Expiration check (`exp` claim)
    /// 4. Issuer/audience validation (if configured)
    /// 5. Map claims → `AuthenticatedIdentity`
    pub fn validate(&self, token: &str) -> Result<AuthenticatedIdentity, JwtError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::MalformedToken);
        }

        // Decode header to determine algorithm.
        let header_bytes = base64_url_decode(parts[0]).ok_or(JwtError::DecodingError)?;
        let header: JwtHeader = crate::util::bounded_json::from_slice(&header_bytes)
            .map_err(|_| JwtError::InvalidClaims)?;

        // Decode payload (middle part). We verify signature separately.
        let payload_bytes = base64_url_decode(parts[1]).ok_or(JwtError::DecodingError)?;
        let claims: JwtClaims = crate::util::bounded_json::from_slice(&payload_bytes)
            .map_err(|_| JwtError::InvalidClaims)?;

        // Verify signature based on algorithm declared in header.
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let signature_bytes = base64_url_decode(parts[2]).ok_or(JwtError::DecodingError)?;

        let algorithm = self.config.algorithm.ok_or(JwtError::UnboundAlgorithm)?;
        if header.alg != algorithm.header_name() {
            return Err(JwtError::UnsupportedAlgorithm);
        }
        match algorithm {
            JwtAlgorithm::Hs256 => {
                if self.config.hmac_secret.is_empty() {
                    return Err(JwtError::UnsupportedAlgorithm);
                }
                if !verify_hmac_sha256(
                    &self.config.hmac_secret,
                    signing_input.as_bytes(),
                    &signature_bytes,
                ) {
                    return Err(JwtError::InvalidSignature);
                }
            }
            JwtAlgorithm::Rs256 => {
                if self.config.rsa_public_key_der.is_empty() {
                    return Err(JwtError::UnsupportedAlgorithm);
                }
                if !verify_rsa_sha256(
                    &self.config.rsa_public_key_der,
                    signing_input.as_bytes(),
                    &signature_bytes,
                ) {
                    return Err(JwtError::InvalidSignature);
                }
            }
        }

        // Check expiration.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        validate_time_claims(
            &claims,
            now,
            self.config.clock_skew_seconds,
            self.config.max_token_lifetime_seconds,
        )?;

        // Validate issuer.
        if !self.config.expected_issuer.is_empty() && claims.iss != self.config.expected_issuer {
            return Err(JwtError::InvalidIssuer);
        }

        // Validate audience: exact equality against one element of the token's
        // audience list. Never a substring, prefix, or joined-string test — a
        // token issued for an unrelated audience must not authenticate here
        // merely because this provider's audience appears inside one of its
        // values.
        if !self.config.expected_audience.is_empty()
            && !claims
                .aud
                .iter()
                .any(|audience| audience == &self.config.expected_audience)
        {
            return Err(JwtError::InvalidAudience);
        }

        let tenant_id = self.config.tenant_id.ok_or(JwtError::UnboundProvider)?;
        let identity = identity_from_external_claims(
            ExternalClaims {
                user_id: claims.user_id,
                subject: &claims.sub,
                role_names: &claims.roles,
                asserted_superuser: claims.is_superuser,
            },
            ExternalProviderBinding::default_database(TenantId::new(tenant_id)),
        );

        debug!(
            username = %identity.username,
            tenant_id,
            roles = ?identity.roles,
            "JWT validated"
        );

        Ok(identity)
    }

    /// Check if JWT authentication is configured (has a secret or public key).
    pub fn is_configured(&self) -> bool {
        self.config.algorithm.is_some()
            && self.config.tenant_id.is_some()
            && (!self.config.hmac_secret.is_empty() || !self.config.rsa_public_key_der.is_empty())
    }
}

pub(crate) fn validate_time_claims(
    claims: &JwtClaims,
    now: u64,
    clock_skew_seconds: u64,
    max_token_lifetime_seconds: u64,
) -> Result<(), JwtError> {
    if claims.exp == 0 {
        return Err(JwtError::MissingExpiration);
    }
    if claims.iat == 0 {
        return Err(JwtError::MissingIssuedAt);
    }
    if claims.exp < claims.iat {
        return Err(JwtError::InvalidIssuedAt);
    }
    if claims.exp.saturating_sub(claims.iat) > max_token_lifetime_seconds {
        return Err(JwtError::TokenLifetimeExceeded);
    }
    if now > claims.exp.saturating_add(clock_skew_seconds) {
        return Err(JwtError::Expired);
    }
    if claims.iat > now.saturating_add(clock_skew_seconds) {
        return Err(JwtError::InvalidIssuedAt);
    }
    if claims.nbf > 0 && now.saturating_add(clock_skew_seconds) < claims.nbf {
        return Err(JwtError::NotYetValid);
    }
    Ok(())
}

/// JWT validation errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    #[error("malformed JWT token")]
    MalformedToken,
    #[error("invalid JWT claims")]
    InvalidClaims,
    #[error("JWT signature verification failed")]
    InvalidSignature,
    #[error("JWT token expired")]
    Expired,
    #[error("JWT token not yet valid")]
    NotYetValid,
    #[error("JWT issuer mismatch")]
    InvalidIssuer,
    #[error("JWT audience mismatch")]
    InvalidAudience,
    #[error("JWT provider has no server-side tenant binding")]
    UnboundProvider,
    #[error("JWT provider has no pinned signature algorithm")]
    UnboundAlgorithm,
    #[error("JWT token is missing exp")]
    MissingExpiration,
    #[error("JWT token is missing iat")]
    MissingIssuedAt,
    #[error("JWT iat/exp claims are inconsistent")]
    InvalidIssuedAt,
    #[error("JWT token lifetime exceeds provider maximum")]
    TokenLifetimeExceeded,
    #[error("JWT status claim carries a blocked value")]
    BlockedStatus,
    #[error("JWT base64 decoding error")]
    DecodingError,
    #[error("JWT algorithm not supported or not configured")]
    UnsupportedAlgorithm,
}

/// Verify HMAC-SHA256 signature.
fn verify_hmac_sha256(secret: &[u8], message: &[u8], expected_signature: &[u8]) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac = match HmacSha256::new_from_slice(secret) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(message);
    mac.verify_slice(expected_signature).is_ok()
}

/// Verify RSA-SHA256 (RS256) signature using a DER-encoded public key.
///
/// Supports both PKCS#1 (RSAPublicKey) and PKCS#8 (SubjectPublicKeyInfo) formats.
fn verify_rsa_sha256(public_key_der: &[u8], message: &[u8], signature: &[u8]) -> bool {
    use rsa::Pkcs1v15Sign;

    // Try PKCS#8 first, then PKCS#1.
    let rsa_key = if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs8::DecodePublicKey>::from_public_key_der(public_key_der)
    {
        key
    } else if let Ok(key) =
        <rsa::RsaPublicKey as rsa::pkcs1::DecodeRsaPublicKey>::from_pkcs1_der(public_key_der)
    {
        key
    } else {
        return false;
    };

    // Hash the message with SHA-256.
    use sha2::Digest;
    let digest = sha2::Sha256::digest(message);

    // Verify PKCS#1 v1.5 signature.
    let scheme = Pkcs1v15Sign::new::<sha2::Sha256>();
    rsa_key.verify(scheme, &digest, signature).is_ok()
}

/// Load an RSA public key from a PEM file (for JwtConfig initialization).
///
/// Accepts PEM files with either `BEGIN PUBLIC KEY` (PKCS#8) or
/// `BEGIN RSA PUBLIC KEY` (PKCS#1) headers.
pub fn load_rsa_public_key_pem(pem_path: &std::path::Path) -> Result<Vec<u8>, JwtError> {
    let pem_data = std::fs::read(pem_path).map_err(|_| JwtError::DecodingError)?;
    let parsed = pem::parse(&pem_data).map_err(|_| JwtError::DecodingError)?;
    match parsed.tag() {
        "PUBLIC KEY" | "RSA PUBLIC KEY" => Ok(parsed.into_contents()),
        _ => Err(JwtError::DecodingError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::Role;

    #[test]
    fn decode_claims() {
        // A minimal JWT payload (base64url encoded).
        let payload =
            r#"{"sub":"alice","tenant_id":1,"roles":["readwrite"],"exp":9999999999,"user_id":42}"#;
        let claims: JwtClaims = sonic_rs::from_str(payload).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.tenant_id, 1);
        assert_eq!(claims.user_id, 42);
        assert_eq!(claims.roles, vec!["readwrite"]);
    }

    #[test]
    fn malformed_token_rejected() {
        let validator = JwtValidator::new(JwtConfig::default());
        let result = validator.validate("not-a-jwt");
        assert_eq!(result.err(), Some(JwtError::MalformedToken));
    }

    #[test]
    fn base64url_decode_works() {
        let encoded = base64_url_encode(b"hello world");
        let decoded = base64_url_decode(&encoded).unwrap();
        assert_eq!(decoded, b"hello world");
    }

    fn base64_url_encode(data: &[u8]) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data)
    }

    /// Bit length used for RSA keys generated in this test module.
    /// Production validates whatever the operator configures; the
    /// signing/verification logic doesn't care about strength, so we
    /// use 1024 here to keep `RsaPrivateKey::new` from dominating the
    /// test runtime. RSA-1024 keygen is ~10x faster than RSA-2048
    /// without changing what these tests actually exercise.
    const TEST_RSA_BITS: usize = 1024;

    fn validate_rs256_payload(payload_json: &str) -> AuthenticatedIdentity {
        validate_rs256_payload_for_audience(payload_json, "").unwrap()
    }

    fn validate_rs256_payload_for_audience(
        payload_json: &str,
        expected_audience: &str,
    ) -> Result<AuthenticatedIdentity, JwtError> {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        let mut rng = rsa::rand_core::OsRng;
        let private_key = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let public_key = rsa::RsaPublicKey::from(&private_key);
        let pub_der = {
            use rsa::pkcs8::EncodePublicKey;
            public_key.to_public_key_der().unwrap().as_ref().to_vec()
        };
        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = base64_url_encode(payload_json.as_bytes());
        let signing_input = format!("{header}.{payload}");
        let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
        let signature: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            base64_url_encode(&signature.to_bytes())
        );
        let validator = JwtValidator::new(JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            rsa_public_key_der: pub_der,
            expected_audience: expected_audience.to_owned(),
            tenant_id: Some(2),
            max_token_lifetime_seconds: u64::MAX,
            ..Default::default()
        });
        validator.validate(&token)
    }

    /// RFC 7519 allows `aud` to be an array. A token listing the configured
    /// audience alongside others must authenticate.
    #[test]
    fn array_audience_containing_the_expected_value_authenticates() {
        let identity = validate_rs256_payload_for_audience(
            r#"{"sub":"alice","aud":["other","nodedb"],"iat":1,"exp":9999999999,"user_id":5}"#,
            "nodedb",
        )
        .expect("an array audience listing the expected value must authenticate");

        assert_eq!(identity.username, "alice");
    }

    /// The match is exact equality against one element — an array audience
    /// with no matching element is rejected, however its values are shaped.
    #[test]
    fn array_audience_without_the_expected_value_is_rejected() {
        assert_eq!(
            validate_rs256_payload_for_audience(
                r#"{"sub":"alice","aud":["other"],"iat":1,"exp":9999999999,"user_id":5}"#,
                "nodedb",
            )
            .err(),
            Some(JwtError::InvalidAudience)
        );
        // A value that merely contains the expected audience is not a match.
        assert_eq!(
            validate_rs256_payload_for_audience(
                r#"{"sub":"alice","aud":["nodedb-staging"],"iat":1,"exp":9999999999,"user_id":5}"#,
                "nodedb",
            )
            .err(),
            Some(JwtError::InvalidAudience)
        );
    }

    #[test]
    fn string_audience_still_authenticates() {
        let identity = validate_rs256_payload_for_audience(
            r#"{"sub":"alice","aud":"nodedb","iat":1,"exp":9999999999,"user_id":5}"#,
            "nodedb",
        )
        .expect("a single-string audience must keep working");

        assert_eq!(identity.username, "alice");
    }

    #[test]
    fn audience_claim_accepts_both_rfc_shapes() {
        let single: JwtClaims =
            sonic_rs::from_str(r#"{"sub":"alice","aud":"nodedb"}"#).expect("string aud parses");
        assert_eq!(single.aud, vec!["nodedb".to_owned()]);

        let multiple: JwtClaims =
            sonic_rs::from_str(r#"{"sub":"alice","aud":["a","b"]}"#).expect("array aud parses");
        assert_eq!(multiple.aud, vec!["a".to_owned(), "b".to_owned()]);

        let absent: JwtClaims =
            sonic_rs::from_str(r#"{"sub":"alice"}"#).expect("absent aud parses");
        assert!(absent.aud.is_empty());
    }

    #[test]
    fn externally_asserted_superuser_flag_does_not_grant_superuser() {
        let identity = validate_rs256_payload(
            r#"{"sub":"alice","tenant_id":999,"roles":["readwrite"],"is_superuser":true,"iat":1,"exp":9999999999,"user_id":99}"#,
        );

        assert!(!identity.is_superuser());
        assert!(!identity.roles.contains(&Role::Superuser));
        assert!(identity.roles.contains(&Role::ReadWrite));
        assert!(!identity.can_access_database(nodedb_types::id::DatabaseId::new(9_999)));
    }

    #[test]
    fn externally_asserted_superuser_role_does_not_grant_superuser() {
        let identity = validate_rs256_payload(
            r#"{"sub":"alice","tenant_id":999,"roles":["superuser","readonly"],"is_superuser":false,"iat":1,"exp":9999999999,"user_id":99}"#,
        );

        assert!(!identity.is_superuser());
        assert!(!identity.roles.contains(&Role::Superuser));
        assert!(identity.roles.contains(&Role::ReadOnly));
    }

    #[test]
    fn rs256_roundtrip() {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        // Generate a test RSA key pair.
        let mut rng = rsa::rand_core::OsRng;
        let private_key = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let public_key = rsa::RsaPublicKey::from(&private_key);

        // Export public key as DER (PKCS#8).
        let pub_der = {
            use rsa::pkcs8::EncodePublicKey;
            public_key.to_public_key_der().unwrap().as_ref().to_vec()
        };

        // Build JWT manually.
        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload_json = r#"{"sub":"bob","tenant_id":999,"roles":["admin"],"iat":1,"exp":9999999999,"user_id":99}"#;
        let payload = base64_url_encode(payload_json.as_bytes());
        let signing_input = format!("{header}.{payload}");

        // Sign with RSA PKCS#1 v1.5.
        let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
        let sig: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = base64_url_encode(&sig.to_bytes());

        let token = format!("{signing_input}.{sig_b64}");

        // Validate.
        let config = JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            rsa_public_key_der: pub_der,
            tenant_id: Some(2),
            max_token_lifetime_seconds: u64::MAX,
            ..Default::default()
        };
        let validator = JwtValidator::new(config);
        let identity = validator.validate(&token).unwrap();
        assert_eq!(identity.username, "bob");
        assert_eq!(identity.tenant_id, TenantId::new(2));
        assert_eq!(identity.user_id, 99);
    }

    #[test]
    fn rs256_wrong_key_rejected() {
        use rsa::pkcs1v15::SigningKey;
        use rsa::signature::{SignatureEncoding, Signer};

        let mut rng = rsa::rand_core::OsRng;
        let key1 = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let key2 = rsa::RsaPrivateKey::new(&mut rng, TEST_RSA_BITS).unwrap();
        let pub2 = rsa::RsaPublicKey::from(&key2);

        let pub2_der = {
            use rsa::pkcs8::EncodePublicKey;
            pub2.to_public_key_der().unwrap().as_ref().to_vec()
        };

        let header = base64_url_encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","exp":9999999999}"#);
        let signing_input = format!("{header}.{payload}");

        // Sign with key1.
        let signing_key = SigningKey::<sha2::Sha256>::new(key1);
        let sig: rsa::pkcs1v15::Signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = base64_url_encode(&sig.to_bytes());
        let token = format!("{signing_input}.{sig_b64}");

        // Verify with key2 — should fail.
        let config = JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            rsa_public_key_der: pub2_der,
            tenant_id: Some(1),
            max_token_lifetime_seconds: u64::MAX,
            ..Default::default()
        };
        let validator = JwtValidator::new(config);
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::InvalidSignature)
        );
    }

    fn time_claims(iat: u64, exp: u64) -> JwtClaims {
        JwtClaims {
            sub: "alice".into(),
            tenant_id: 999,
            roles: Vec::new(),
            exp,
            nbf: 0,
            iat,
            iss: String::new(),
            aud: Vec::new(),
            user_id: 1,
            is_superuser: false,
            extra: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn time_claims_require_exp_iat_and_bounded_lifetime() {
        assert_eq!(
            validate_time_claims(&time_claims(10, 0), 10, 0, 100),
            Err(JwtError::MissingExpiration)
        );
        assert_eq!(
            validate_time_claims(&time_claims(0, 20), 10, 0, 100),
            Err(JwtError::MissingIssuedAt)
        );
        assert_eq!(
            validate_time_claims(&time_claims(20, 10), 10, 0, 100),
            Err(JwtError::InvalidIssuedAt)
        );
        assert_eq!(
            validate_time_claims(&time_claims(10, 111), 10, 0, 100),
            Err(JwtError::TokenLifetimeExceeded)
        );
        assert_eq!(
            validate_time_claims(&time_claims(20, 30), 10, 0, 100),
            Err(JwtError::InvalidIssuedAt)
        );
        assert!(validate_time_claims(&time_claims(10, 30), 20, 0, 100).is_ok());
    }

    #[test]
    fn unsupported_algorithm_rejected() {
        let header = base64_url_encode(br#"{"alg":"ES256"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","exp":9999999999}"#);
        let sig = base64_url_encode(b"fakesig");
        let token = format!("{header}.{payload}.{sig}");

        let validator = JwtValidator::new(JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            tenant_id: Some(1),
            ..Default::default()
        });
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::UnsupportedAlgorithm)
        );
    }

    #[test]
    fn configured_algorithm_cannot_be_overridden_by_header() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let secret = b"shared-secret";
        let header = base64_url_encode(br#"{"alg":"HS256"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","iat":1,"exp":2}"#);
        let signing_input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("valid HMAC fixture key");
        mac.update(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            base64_url_encode(&mac.finalize().into_bytes())
        );
        let validator = JwtValidator::new(JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            hmac_secret: secret.to_vec(),
            rsa_public_key_der: vec![1],
            tenant_id: Some(1),
            ..Default::default()
        });
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::UnsupportedAlgorithm)
        );
    }

    #[test]
    fn signed_hs256_token_without_expiration_is_rejected() {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        let secret = b"shared-secret";
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock must be after epoch")
            .as_secs();
        let header = base64_url_encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = base64_url_encode(
            format!(
                r#"{{"sub":"alice","iss":"https://issuer.example/","aud":"nodedb-api","iat":{issued_at},"user_id":7}}"#
            )
            .as_bytes(),
        );
        let signing_input = format!("{header}.{payload}");
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("valid HMAC fixture key");
        mac.update(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            base64_url_encode(&mac.finalize().into_bytes())
        );
        let validator = JwtValidator::new(JwtConfig {
            algorithm: Some(JwtAlgorithm::Hs256),
            hmac_secret: secret.to_vec(),
            expected_issuer: "https://issuer.example/".into(),
            expected_audience: "nodedb-api".into(),
            tenant_id: Some(42),
            ..Default::default()
        });

        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::MissingExpiration)
        );
    }

    #[test]
    fn none_algorithm_rejected() {
        let header = base64_url_encode(br#"{"alg":"none"}"#);
        let payload = base64_url_encode(br#"{"sub":"x","iat":1,"exp":2}"#);
        let token = format!("{header}.{payload}.");
        let validator = JwtValidator::new(JwtConfig {
            algorithm: Some(JwtAlgorithm::Rs256),
            tenant_id: Some(1),
            ..Default::default()
        });
        assert_eq!(
            validator.validate(&token).err(),
            Some(JwtError::UnsupportedAlgorithm)
        );
    }
}
