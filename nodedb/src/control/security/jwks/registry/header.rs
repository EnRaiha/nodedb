// SPDX-License-Identifier: BUSL-1.1

//! JWT header parsing: the first base64url-encoded segment of a token.

use crate::control::security::jwt::JwtError;
use crate::control::security::util::base64_url_decode;

#[derive(Debug, serde::Deserialize)]
pub(super) struct JwtHeader {
    pub(super) alg: String,
    #[serde(default)]
    pub(super) kid: Option<String>,
}

pub(super) fn decode_jwt_header(encoded: &str) -> Result<JwtHeader, JwtError> {
    let bytes = base64_url_decode(encoded).ok_or(JwtError::DecodingError)?;
    crate::util::bounded_json::from_slice(&bytes).map_err(|_| JwtError::InvalidClaims)
}
