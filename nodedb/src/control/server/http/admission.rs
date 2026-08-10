// SPDX-License-Identifier: BUSL-1.1

//! The request-admission gate as HTTP routes call it.
//!
//! `session_auth::check_request_admission` and
//! `session_auth::check_blacklist_and_status` are the two composed doors every
//! transport shares. HTTP routes need the same three-line dance around each of
//! them — build a `ClientRequestScope` bound to the route's database and the
//! accepted socket's address, run the door, turn a refusal into an
//! [`ApiError`] — so it lives here once instead of being re-derived (and
//! forgotten) per route.
//!
//! The peer address must be the accepted socket's, from
//! [`PeerAddr`](super::peer::PeerAddr): the IP blacklist and the risk scorer
//! both parse it as an address and silently ignore anything that is not one,
//! so a route that passes a transport label disables those halves of the gate
//! while still appearing to call it.

use axum::http::HeaderMap;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::session_auth;
use crate::types::DatabaseId;

use super::auth::{ApiError, AppState};
use super::rate_limit_headers::rate_limit_headers;

/// The database an admission scope is charged against on a route that does
/// not let the caller select one. The identity's own default is authoritative
/// there — falling straight to `DatabaseId::DEFAULT` would meter and
/// rate-limit every such caller against a database it may not even use.
pub(crate) fn identity_database(identity: &AuthenticatedIdentity) -> DatabaseId {
    identity.default_database.unwrap_or(DatabaseId::DEFAULT)
}

/// Build the scope both doors below are checked against, bound to the
/// accepted socket's address.
fn admission_scope<'a, 'p>(
    state: &'a AppState,
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
    peer_addr: &'p str,
) -> ClientRequestScope<'a, 'p> {
    ClientRequestScope::for_database(identity, state.shared.auth_stores(), database_id, peer_addr)
}

/// Run the full gate — internal-service exemption, blacklist, account status,
/// risk, then rate limit — for a per-request route.
///
/// Returns the `X-RateLimit-*` headers the caller must attach to its success
/// response; a refusal is an `Err` and never reaches the route's work.
pub(crate) fn admit(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    peer_addr: &str,
    operation: &str,
) -> Result<HeaderMap, ApiError> {
    let request = admission_scope(state, identity, database_id, peer_addr);
    let result = session_auth::check_request_admission(&state.shared, &request, operation)?;
    Ok(rate_limit_headers(&result))
}

/// Run the gate without the rate limiter, for doors whose traffic the
/// rate-limiter's per-query cost table does not model: long-lived streams
/// (admitted once at open, then served for hours), bulk ingest, and operator
/// actions that must not be throttled. A blacklisted or suspended/banned
/// account is still refused.
pub(crate) fn admit_without_rate_limit(
    state: &AppState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    peer_addr: &str,
) -> Result<(), ApiError> {
    let request = admission_scope(state, identity, database_id, peer_addr);
    session_auth::check_blacklist_and_status(&state.shared, &request)?;
    Ok(())
}
