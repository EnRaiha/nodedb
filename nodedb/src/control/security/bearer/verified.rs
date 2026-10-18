// SPDX-License-Identifier: BUSL-1.1

//! A verified bearer token bound to the session id its login recorded.

use crate::control::security::jwks::registry::VerifiedJwtClaims;

/// A verified bearer token and the session id its `AuthSuccess` entry carries.
///
/// The session's `AuthContext` uses the same id, so `$auth.session_id` matches the audit log.
pub struct VerifiedBearer {
    pub claims: VerifiedJwtClaims,
    pub session_id: String,
}
