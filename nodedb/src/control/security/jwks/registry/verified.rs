// SPDX-License-Identifier: BUSL-1.1

//! Opaque proof that a token's claims passed JWKS signature, route, and time
//! validation, plus the `[auth.jwt]` claim policy applied on top of them.

use crate::control::security::jwt::JwtClaims;

/// Opaque proof that claims passed JWKS signature, route, and time validation.
pub struct VerifiedJwtClaims(pub(super) JwtClaims);

impl VerifiedJwtClaims {
    pub(crate) fn claims(&self) -> &JwtClaims {
        &self.0
    }

    /// Test-only constructor: wraps already-"verified" claims without going
    /// through JWKS signature verification. Exists so callers elsewhere in
    /// the crate (e.g. `request_scope::builder` tests) can exercise the
    /// verified-JWT construction path without standing up a full JWKS
    /// registry.
    #[cfg(test)]
    pub(crate) fn new_for_test(claims: JwtClaims) -> Self {
        Self(claims)
    }
}

/// Deliberately opaque: the wrapped claims carry the subject, audience, and
/// whatever custom fields the provider issues, so a derived `Debug` would put
/// them into any log line, panic message, or error report that formats a value
/// containing one. `Debug` exists only so `Result<VerifiedJwtClaims, _>` can be
/// unwrapped in tests; it intentionally reveals nothing.
impl std::fmt::Debug for VerifiedJwtClaims {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedJwtClaims").finish_non_exhaustive()
    }
}
