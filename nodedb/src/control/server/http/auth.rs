// SPDX-License-Identifier: BUSL-1.1

//! HTTP API authentication via API key bearer tokens.
//!
//! Extracts `AuthenticatedIdentity` from the `Authorization: Bearer ndb_...` header.
//! Falls back to trust mode if configured.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::config::auth::AuthMode;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::session_auth;
use crate::control::state::SharedState;

/// Application state shared across all HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub shared: Arc<SharedState>,
    pub auth_mode: AuthMode,
    /// Shared phased coordinator for graceful shutdown requests.
    pub shutdown_bus: crate::control::shutdown::ShutdownBus,
    /// DataFusion query context for SQL planning (Send + Sync).
    pub query_ctx: Arc<crate::control::planner::context::QueryContext>,
}

/// Try to validate a Bearer token as a JWT via the JWKS registry.
///
/// Returns `Some(identity)` if the token is a valid JWT with 2 dots, the
/// registry verifies the signature, and the token passes the state-dependent
/// `[auth.jwt]` policy (declared scopes, JIT-provisioned account status).
/// Returns `None` otherwise — a policy refusal falls through to the remaining
/// credential kinds and ends as a generic `invalid bearer token`, disclosing
/// nothing about why the token was refused.
///
/// The dot-count check gates entry into the shared two-stage validator
/// ([`authenticate_bearer_jwt`](session_auth::authenticate_bearer_jwt)) rather
/// than being folded into it: API keys (`ndb_...`) never have 2 dots, so this
/// keeps every non-JWT bearer token out of JWKS validation entirely instead of
/// paying for (and logging) a doomed verification attempt on each one.
///
/// Validation is awaited, never blocked on: a JWKS cache miss fetches the
/// provider's key set over the network, and the whole HTTP request path runs on
/// Tokio worker threads, so blocking here would stall a worker at best and
/// abort the request at worst.
async fn try_validate_jwt(
    state: &AppState,
    token: &str,
) -> Option<(
    AuthenticatedIdentity,
    crate::control::security::jwks::registry::VerifiedJwtClaims,
)> {
    if token.matches('.').count() != 2 {
        return None;
    }
    session_auth::authenticate_bearer_jwt(&state.shared, token).await
}

/// Enforce the TLS policy for a resolved HTTP identity.
///
/// HTTP has no connection-scoped session object to hang this off, so it runs
/// at the same place the identity appears: every route resolves its identity
/// through [`resolve_identity`] or [`resolve_auth_parts`], and neither returns
/// one that has not been through here. `transport` is the value the listener
/// captured at accept (see [`transport`](super::transport)).
fn enforce_transport_policy(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    transport: crate::control::security::tls_policy::TransportSecurity,
    peer_addr: &str,
) -> Result<(), ApiError> {
    session_auth::check_transport_security(&state.shared, identity, transport, peer_addr)
        .map_err(ApiError::from)
}

/// Resolve an authenticated identity from HTTP headers, then enforce the TLS
/// policy against the connection it arrived on.
///
/// Authentication order:
/// 1. `Authorization: Bearer eyJ...` — JWT (if JwksRegistry configured)
/// 2. `Authorization: Bearer ndb_...` — API key
/// 3. Trust mode (no header required) — if configured
pub async fn resolve_identity(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
    transport: crate::control::security::tls_policy::TransportSecurity,
) -> Result<AuthenticatedIdentity, ApiError> {
    let identity = resolve_identity_unchecked(headers, state, peer_addr).await?;
    enforce_transport_policy(state, &identity, transport, peer_addr)?;
    Ok(identity)
}

/// The credential half of [`resolve_identity`], without the transport check.
///
/// Private on purpose: an identity that has not been through
/// [`enforce_transport_policy`] must not leave this module.
async fn resolve_identity_unchecked(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
) -> Result<AuthenticatedIdentity, ApiError> {
    if let Some(auth_header) = headers.get("authorization") {
        let auth_str = auth_header
            .to_str()
            .map_err(|_| ApiError::Unauthorized("invalid authorization header encoding".into()))?;

        if let Some(token) = auth_str.strip_prefix("Bearer ") {
            let token = token.trim();

            // Try JWT first (token has 2 dots = JWT format).
            if let Some((identity, _)) = try_validate_jwt(state, token).await {
                return Ok(identity);
            }

            // Try API key.
            if let Some(identity) =
                session_auth::verify_api_key_identity(&state.shared, token, peer_addr, "HTTP")
            {
                return Ok(identity);
            }

            return Err(ApiError::Unauthorized("invalid bearer token".into()));
        }
    }

    if state.auth_mode == AuthMode::Trust {
        return session_auth::configured_trust_identity(&state.shared).ok_or_else(|| {
            ApiError::Unauthorized("configured trust identity is unavailable".into())
        });
    }

    Err(ApiError::Unauthorized(
        "missing Authorization: Bearer <token> header".into(),
    ))
}

/// Require tenant administration authority for an operation in `database_id`.
///
/// Tenant-admin actions are still bound to the identity's selected database;
/// a tenant administrator cannot use an HTTP endpoint to bypass its database
/// access scope.
pub fn require_tenant_admin_for_database(
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
) -> Result<(), ApiError> {
    use crate::control::security::identity::Role;

    if !identity.has_role(&Role::TenantAdmin) {
        return Err(ApiError::Forbidden(
            "WASM uploads require tenant_admin privileges".into(),
        ));
    }
    if !identity.can_access_database(database_id) {
        return Err(ApiError::Forbidden(format!(
            "permission denied for selected database {}",
            database_id.as_u64()
        )));
    }
    Ok(())
}

/// Resolve both authenticated identity and auth context from HTTP headers.
///
/// Uses `AuthContext::from_verified_jwt()` for JWTs so rich claims are retained
/// while authorization fields remain bound to the verified identity. Falls back
/// to `build_auth_context()` for API key / password auth.
pub async fn resolve_auth(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
    transport: crate::control::security::tls_policy::TransportSecurity,
) -> Result<
    (
        AuthenticatedIdentity,
        crate::control::security::auth_context::AuthContext,
    ),
    ApiError,
> {
    use crate::control::security::auth_context::{AuthContext, generate_session_id};

    let (identity, verified_jwt) = resolve_auth_parts(headers, state, peer_addr, transport).await?;
    let auth_ctx = match &verified_jwt {
        Some(claims) => AuthContext::from_verified_jwt(claims, &identity, generate_session_id()),
        None => session_auth::build_auth_context(&identity),
    };
    let auth_ctx = apply_on_deny_header(headers, auth_ctx);
    Ok((identity, auth_ctx))
}

/// Resolve identity plus the raw ingredients a caller needs to build a
/// [`RequestAuthScope`](crate::control::security::request_scope::RequestAuthScope)
/// directly, instead of a pre-assembled `AuthContext`: the verified JWT
/// claims when authentication went through a JWT bearer token (so
/// `RequestAuthScope::builder().with_verified_jwt()` reproduces the exact
/// claim-derived enrichment `resolve_auth` gives `AuthContext::from_verified_jwt`),
/// or `None` for API key / trust / password auth.
pub(crate) async fn resolve_auth_parts(
    headers: &HeaderMap,
    state: &AppState,
    peer_addr: &str,
    transport: crate::control::security::tls_policy::TransportSecurity,
) -> Result<
    (
        AuthenticatedIdentity,
        Option<crate::control::security::jwks::registry::VerifiedJwtClaims>,
    ),
    ApiError,
> {
    if let Some(auth_header) = headers.get("authorization")
        && let Ok(auth_str) = auth_header.to_str()
        && let Some(token) = auth_str.strip_prefix("Bearer ")
    {
        let token = token.trim();
        if let Some((identity, verified_claims)) = try_validate_jwt(state, token).await {
            // The JWT path returns before `resolve_identity`, so it enforces
            // the transport policy itself rather than inheriting it.
            enforce_transport_policy(state, &identity, transport, peer_addr)?;
            return Ok((identity, Some(verified_claims)));
        }
    }

    let identity = resolve_identity(headers, state, peer_addr, transport).await?;
    Ok((identity, None))
}

/// Parse the `X-On-Deny` header into a `DenyMode`, if present and valid.
///
/// HTTP accepts only the trimmed, case-insensitive tokens `SILENT` and `ERROR`.
/// `ERROR` always uses this server-owned generic RLS response; DDL's richer
/// `ON DENY ERROR` syntax must not let an HTTP client control response content.
pub(crate) fn on_deny_header_mode(
    headers: &HeaderMap,
) -> Option<crate::control::security::deny::DenyMode> {
    use crate::control::security::deny::{DenyCodes, DenyError, DenyMode};

    let value = headers
        .get("x-on-deny")
        .and_then(|value| value.to_str().ok())?;

    if value.trim().eq_ignore_ascii_case("SILENT") {
        Some(DenyMode::Silent)
    } else if value.trim().eq_ignore_ascii_case("ERROR") {
        Some(DenyMode::Error(DenyError {
            code: DenyCodes::RLS_READ_DENIED.to_string(),
            message: "Access denied by row-level security policy".to_string(),
            detail: None,
        }))
    } else {
        None
    }
}

/// Build a request-scoped
/// [`ClientRequestScope`](crate::control::security::request_scope::ClientRequestScope)
/// for an HTTP request already bound to `database_id`, from the identity and
/// verified JWT resolved earlier in the same handler via
/// [`resolve_auth_parts`].
///
/// Collapses the construction dance every HTTP query route needs identically:
/// session database, `X-On-Deny` header, the connection's real peer address,
/// and (when present) verified-JWT enrichment.
///
/// `peer_addr` must be the accepted socket's address, from
/// [`PeerAddr`](super::peer::PeerAddr) — the scope stamps `$auth.risk_score`
/// from it, and a scope built without one is treated as unassessed and refused
/// by the request-admission gate whenever risk scoring is enabled. The result
/// keeps that address bound to the scope so the gate cannot be handed a
/// different one.
pub(crate) fn build_request_scope<'a, 'p>(
    identity: &'a AuthenticatedIdentity,
    verified_jwt: Option<&'a crate::control::security::jwks::registry::VerifiedJwtClaims>,
    headers: &HeaderMap,
    state: &'a AppState,
    database_id: crate::types::DatabaseId,
    peer_addr: &'p str,
) -> crate::control::security::request_scope::ClientRequestScope<'a, 'p> {
    crate::control::security::request_scope::RequestAuthScope::builder(
        identity,
        state.shared.auth_stores(),
    )
    .with_session_database(Some(database_id))
    .with_on_deny(on_deny_header_mode(headers))
    .with_optional_verified_jwt(verified_jwt)
    .build_for_client(peer_addr)
}

/// Apply [`on_deny_header_mode`] to an already-built `AuthContext`.
fn apply_on_deny_header(
    headers: &HeaderMap,
    mut auth_ctx: crate::control::security::auth_context::AuthContext,
) -> crate::control::security::auth_context::AuthContext {
    if let Some(mode) = on_deny_header_mode(headers) {
        auth_ctx.on_deny_override = Some(mode);
    }
    auth_ctx
}

/// HTTP API error type.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    Forbidden(String),
    BadRequest(String),
    Internal(String),
    /// 429 Too Many Requests with Retry-After header.
    RateLimited {
        message: String,
        retry_after_secs: u64,
    },
    /// Arbitrary HTTP status from gateway error mapping.
    HttpStatus(u16, String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        use super::types::HttpError;

        match self {
            ApiError::RateLimited {
                message,
                retry_after_secs,
            } => {
                let body = HttpError::new(message);
                let mut resp = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
                if let Ok(val) = retry_after_secs.to_string().parse() {
                    resp.headers_mut().insert("Retry-After", val);
                }
                resp
            }
            other => {
                let (status, message) = match other {
                    ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
                    ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
                    ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
                    ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
                    ApiError::RateLimited { .. } => unreachable!(),
                    ApiError::HttpStatus(code, msg) => (
                        StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                        msg,
                    ),
                };
                let body = HttpError::new(message);
                (status, axum::Json(body)).into_response()
            }
        }
    }
}

/// Axum extractor that resolves and enforces HTTP auth before a handler runs.
///
/// Add this as the first parameter to any handler that performs tenant-scoped
/// or admin work. Handlers that should remain public (health probes, etc.) must
/// NOT include this extractor.
///
/// Produces a 401/403 response and short-circuits the handler if auth fails.
pub struct ResolvedIdentity(pub crate::control::security::identity::AuthenticatedIdentity);

impl ResolvedIdentity {
    /// The resolved tenant ID (convenience accessor).
    pub fn tenant_id(&self) -> crate::types::TenantId {
        self.0.tenant_id
    }
}

impl FromRequestParts<AppState> for ResolvedIdentity {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let peer = super::peer::peer_addr_from_parts(parts)?;
        let transport = super::transport::transport_from_parts(parts)?;
        let identity = resolve_identity(&parts.headers, state, &peer, transport).await?;
        Ok(ResolvedIdentity(identity))
    }
}

/// Like `ResolvedIdentity` but also resolves an `AuthContext` for handlers
/// that need fine-grained RLS / permission checks.
pub struct ResolvedAuth(
    pub crate::control::security::identity::AuthenticatedIdentity,
    pub crate::control::security::auth_context::AuthContext,
);

impl ResolvedAuth {
    pub fn tenant_id(&self) -> crate::types::TenantId {
        self.0.tenant_id
    }
}

impl FromRequestParts<AppState> for ResolvedAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let peer = super::peer::peer_addr_from_parts(parts)?;
        let transport = super::transport::transport_from_parts(parts)?;
        let (identity, auth_ctx) = resolve_auth(&parts.headers, state, &peer, transport).await?;
        Ok(ResolvedAuth(identity, auth_ctx))
    }
}

impl From<crate::Error> for ApiError {
    fn from(e: crate::Error) -> Self {
        match &e {
            crate::Error::RejectedAuthz { .. } => Self::Forbidden(e.to_string()),
            crate::Error::RateExceeded { retry_after_ms, .. } => Self::RateLimited {
                message: e.to_string(),
                retry_after_secs: retry_after_ms.div_ceil(1000).max(1),
            },
            crate::Error::BadRequest { .. }
            | crate::Error::PlanError { .. }
            | crate::Error::Config { .. } => Self::BadRequest(e.to_string()),
            crate::Error::CollectionNotFound { .. } | crate::Error::DocumentNotFound { .. } => {
                Self::BadRequest(e.to_string())
            }
            _ => Self::Internal(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_context::{AuthContext, AuthStatus};
    use crate::control::security::deny::{DenyCodes, DenyMode};
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;

    fn auth_context() -> AuthContext {
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(7),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        let mut context = AuthContext::from_identity(&identity, "s_http_test".into());
        context.email = Some("alice@example.test".into());
        context.org_id = Some("org-7".into());
        context.org_ids = vec!["org-7".into()];
        context.groups = vec!["engineering".into()];
        context.permissions = vec!["documents:read".into()];
        context.status = AuthStatus::Restricted;
        context.metadata.insert("plan".into(), "pro".into());
        context.auth_time = Some(1_700_000_000);
        context.database_id = Some(nodedb_types::id::DatabaseId::new(9));
        context
    }

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-on-deny", value.parse().expect("valid header value"));
        headers
    }

    fn assert_authorization_fields_unchanged(before: &AuthContext, after: &AuthContext) {
        assert_eq!(after.id, before.id);
        assert_eq!(after.username, before.username);
        assert_eq!(after.email, before.email);
        assert_eq!(after.tenant_id, before.tenant_id);
        assert_eq!(after.org_id, before.org_id);
        assert_eq!(after.org_ids, before.org_ids);
        assert_eq!(after.roles, before.roles);
        assert_eq!(after.groups, before.groups);
        assert_eq!(after.permissions, before.permissions);
        assert_eq!(after.status, before.status);
        assert_eq!(after.metadata, before.metadata);
        assert_eq!(after.auth_method, before.auth_method);
        assert_eq!(after.auth_time, before.auth_time);
        assert_eq!(after.session_id, before.session_id);
        assert_eq!(after.database_id, before.database_id);
    }

    #[test]
    fn x_on_deny_silent_accepts_trimmed_case_insensitive_token() {
        let context = auth_context();
        let result = apply_on_deny_header(&headers("  sIlEnT  "), context.clone());

        assert_eq!(result.on_deny_override, Some(DenyMode::Silent));
        assert_authorization_fields_unchanged(&context, &result);
    }

    #[test]
    fn x_on_deny_error_uses_server_owned_generic_rls_response() {
        let context = auth_context();
        let result = apply_on_deny_header(&headers("  ErRoR  "), context.clone());

        match result.on_deny_override {
            Some(DenyMode::Error(ref error)) => {
                assert_eq!(error.code, DenyCodes::RLS_READ_DENIED);
                assert_eq!(error.message, "Access denied by row-level security policy");
                assert_eq!(error.detail, None);
            }
            other => panic!("expected generic error override, got {other:?}"),
        }
        assert_authorization_fields_unchanged(&context, &result);
    }

    #[test]
    fn x_on_deny_parameterized_error_cannot_inject_response_content() {
        let mut context = auth_context();
        context.on_deny_override = Some(DenyMode::Silent);
        let result = apply_on_deny_header(
            &headers("ERROR 'CLIENT_CODE' MESSAGE 'client-controlled message'"),
            context.clone(),
        );

        assert_eq!(result.on_deny_override, context.on_deny_override);
        assert_authorization_fields_unchanged(&context, &result);
    }

    #[test]
    fn x_on_deny_malformed_value_is_ignored() {
        let context = auth_context();
        let result = apply_on_deny_header(&headers("ERROR=custom"), context.clone());

        assert_eq!(result.on_deny_override, None);
        assert_authorization_fields_unchanged(&context, &result);
    }
}
