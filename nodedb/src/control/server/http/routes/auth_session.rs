// SPDX-License-Identifier: BUSL-1.1

//! Opaque session handle HTTP endpoint.
//!
//! ```text
//! POST /v1/auth/session
//! Authorization: Bearer <jwt-or-api-key>
//!
//! Response: { "session_id": "nds_...", "expires_in": 3600 }
//! ```
//!
//! The returned `session_id` can be used with pgwire connection poolers:
//! ```text
//! SET LOCAL nodedb.auth_session = 'nds_...';
//! SELECT * FROM orders;  -- Uses the cached AuthContext from the session handle
//! ```

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;

use crate::control::security::session_handle::ClientFingerprint;

use super::super::admission::{admit, identity_database};
use super::super::auth::{ApiError, AppState, resolve_auth};
use super::super::transport::ClientTransport;
use super::super::types::{HttpSessionResponse, HttpStatusOk};

/// `POST /v1/auth/session` — Create an opaque session handle.
///
/// Validates the bearer token (JWT or API key), creates a server-side
/// cached `AuthContext`, and returns a UUID handle the client can use
/// with `SET LOCAL nodedb.auth_session = '<handle>'` on pgwire connections.
///
/// The caller's `(tenant_id, peer IP)` is captured as a `ClientFingerprint`
/// and bound to the handle; subsequent resolves from a different origin are
/// rejected per the configured `FingerprintMode`.
pub async fn create_session(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    transport: ClientTransport,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let peer_str = peer.to_string();
    let (identity, auth_ctx) =
        resolve_auth(&headers, &state, &peer_str, transport.security()).await?;

    // Creating server-side session state on the caller's behalf runs behind
    // the full gate — a blacklisted IP or suspended/banned account must not be
    // able to cache an `AuthContext` for later pgwire reuse, and the rate
    // limit bounds bulk handle creation from one principal.
    let rate_limit_headers = admit(
        &state,
        &identity,
        identity_database(&identity),
        &peer_str,
        "auth_session_create",
    )?;

    let fingerprint = ClientFingerprint::from_peer(identity.tenant_id, &peer);
    let handle = state.shared.session_handles.create(auth_ctx, fingerprint);

    Ok((
        rate_limit_headers,
        axum::Json(HttpSessionResponse {
            session_id: handle,
            expires_in: 3600,
        }),
    ))
}

/// `DELETE /v1/auth/session` — Invalidate a session handle.
///
/// ```text
/// DELETE /v1/auth/session
/// Authorization: Bearer <jwt-or-api-key>
/// X-Session-Id: nds_...
/// ```
///
/// The caller must present a valid bearer token. A session handle may only be
/// invalidated by the authenticated caller — the identity is verified before
/// any session state is read or mutated.
pub async fn delete_session(
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    transport: ClientTransport,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    // Auth check must come first — return 401/403 before touching session state.
    let peer_str = peer.to_string();
    let identity = crate::control::server::http::auth::resolve_identity(
        &headers,
        &state,
        &peer_str,
        transport.security(),
    )
    .await?;
    let rate_limit_headers = admit(
        &state,
        &identity,
        identity_database(&identity),
        &peer_str,
        "auth_session_delete",
    )?;

    let handle = headers
        .get("x-session-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::BadRequest("missing X-Session-Id header".into()))?;

    let found = state.shared.session_handles.invalidate(handle);
    if !found {
        return Err(ApiError::BadRequest("session handle not found".into()));
    }

    Ok((rate_limit_headers, axum::Json(HttpStatusOk::ok())))
}
