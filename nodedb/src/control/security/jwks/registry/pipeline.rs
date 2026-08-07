// SPDX-License-Identifier: BUSL-1.1

//! The shared token-decode and signature/time-verification pipeline every
//! public entry point in this package runs through.

use tracing::warn;

use crate::control::security::jwks::key::{VerificationKey, verify_signature};
use crate::control::security::jwt::{JwtClaims, JwtError, validate_time_claims};
use crate::control::security::util::base64_url_decode;

use super::header::decode_jwt_header;
use super::state::{DecodedToken, JwksRegistry};

impl JwksRegistry {
    /// Split the token, decode the header + payload, and check that the
    /// algorithm is non-`none` and on the allow-list. Does NOT verify the
    /// signature, the `iss`, the `aud`, or the time claims.
    pub(super) fn decode_unverified<'a>(
        &self,
        token: &'a str,
    ) -> Result<DecodedToken<'a>, JwtError> {
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
    pub(super) fn verify_signature_and_time(
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
}
