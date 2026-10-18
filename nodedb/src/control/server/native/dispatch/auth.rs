// SPDX-License-Identifier: BUSL-1.1

//! Authentication and ping handlers.

use nodedb_types::protocol::{AuthMethod as ProtoAuth, NativeResponse};

use crate::control::security::bearer::VerifiedBearer;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Result of a native-protocol authentication attempt.
///
/// `verified_jwt` is `Some` only for the `OidcBearer` method — it carries the
/// opaque proof that the token's claims passed JWKS signature, route, and
/// time validation, so the session can later enrich `$auth.*` (email, org,
/// groups, permissions, metadata) via `AuthContext::from_verified_jwt`
/// without re-deriving authority from the token: authority always comes from
/// `identity`, never from the claims.
/// It also carries the session id the login's `AuthSuccess` entry recorded.
pub(crate) struct NativeAuthOutcome {
    pub(crate) identity: AuthenticatedIdentity,
    /// Non-empty when the account is in a password grace period or
    /// `must_change_password` is set.
    pub(crate) warning: Option<String>,
    pub(crate) verified_jwt: Option<VerifiedBearer>,
}

/// Authenticate a native protocol client.
///
/// `OidcBearer` tokens are validated directly against the OIDC provider catalog
/// (not the `JwksRegistry` provider list), enabling runtime `CREATE OIDC PROVIDER`
/// without a server restart.
pub(crate) async fn handle_auth(
    state: &SharedState,
    auth_mode: &crate::config::auth::AuthMode,
    auth: &ProtoAuth,
    peer_addr: &str,
) -> crate::Result<NativeAuthOutcome> {
    if let ProtoAuth::OidcBearer { token, .. } = auth {
        // The session id exists before verification, so the `AuthSuccess` entry and
        // `$auth.session_id` carry the same id.
        let session_id = crate::control::security::auth_context::generate_session_id()?;
        let (identity, claims) = crate::control::security::oidc::verify_bearer_token(
            state,
            token,
            peer_addr,
            &session_id,
        )
        .await?;
        return Ok(NativeAuthOutcome {
            identity,
            warning: None,
            verified_jwt: Some(VerifiedBearer { claims, session_id }),
        });
    }

    let body = match auth {
        ProtoAuth::Trust { username } => {
            serde_json::json!({ "method": "trust", "username": username })
        }
        ProtoAuth::Password { username, password } => {
            serde_json::json!({ "method": "password", "username": username, "password": password })
        }
        ProtoAuth::ApiKey { token } => {
            serde_json::json!({ "method": "api_key", "token": token })
        }
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "unsupported authentication method".into(),
            });
        }
    };

    let (identity, warning) =
        super::super::super::session_auth::authenticate(state, auth_mode, &body, peer_addr).await?;
    Ok(NativeAuthOutcome {
        identity,
        warning,
        verified_jwt: None,
    })
}

/// Respond to a ping with a pong.
pub(crate) fn handle_ping(seq: u64) -> NativeResponse {
    NativeResponse::status_row(seq, "PONG")
}
