// SPDX-License-Identifier: BUSL-1.1

//! The client's socket peer address, for HTTP routes that authenticate,
//! admit, or audit a request.
//!
//! Every guard that takes a `peer_addr` — the IP blacklist, the risk gate,
//! API-key audit records — parses it as an address and silently ignores
//! anything else. A route that hands them a fixed label instead of the real
//! remote address therefore disables those guards for its whole transport
//! while still appearing to call them, so the address is extracted here, once,
//! and fails the request closed when it is unavailable.
//!
//! The address comes from the accepted socket only. Forwarded-for style
//! headers are deliberately not consulted: they are client-supplied, and
//! honouring one unvalidated would let any caller choose the IP the blacklist
//! and risk scorer see.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;

use super::auth::{ApiError, AppState};

/// The peer address of the connection this request arrived on, formatted the
/// way every other transport formats it (`10.1.2.3:54321`, `[::1]:54321`), so
/// `client_ip_from_peer` and the IP blacklist parse it identically.
pub struct PeerAddr(String);

impl PeerAddr {
    /// The peer address as the guards expect it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Read the connection's peer address out of request extensions.
///
/// `Err` when the router was mounted without connect info; the server always
/// serves with `into_make_service_with_connect_info::<SocketAddr>()`, so this
/// is a wiring fault rather than a client error — and a request that cannot be
/// address-checked must be refused, not admitted unchecked.
pub(crate) fn peer_addr_from_parts(parts: &Parts) -> Result<String, ApiError> {
    parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.to_string())
        .ok_or_else(|| ApiError::Internal("client peer address unavailable".into()))
}

impl FromRequestParts<AppState> for PeerAddr {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(PeerAddr(peer_addr_from_parts(parts)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::risk::client_ip_from_peer;

    fn parts_with(addr: Option<SocketAddr>) -> Parts {
        let (mut parts, _) = axum::http::Request::new(()).into_parts();
        if let Some(addr) = addr {
            parts.extensions.insert(ConnectInfo(addr));
        }
        parts
    }

    /// The formatted address must survive `client_ip_from_peer`, which is
    /// what the risk scorer and the IP blacklist key on.
    #[test]
    fn formats_v4_peer_so_the_guards_can_parse_it() {
        let parts = parts_with(Some("10.1.2.3:54321".parse().expect("valid address")));
        let peer = peer_addr_from_parts(&parts).expect("connect info present");

        assert_eq!(peer, "10.1.2.3:54321");
        assert_eq!(client_ip_from_peer(&peer).as_deref(), Some("10.1.2.3"));
    }

    #[test]
    fn formats_v6_peer_so_the_guards_can_parse_it() {
        let parts = parts_with(Some("[::1]:54321".parse().expect("valid address")));
        let peer = peer_addr_from_parts(&parts).expect("connect info present");

        assert_eq!(peer, "[::1]:54321");
        assert_eq!(client_ip_from_peer(&peer).as_deref(), Some("::1"));
    }

    #[test]
    fn missing_connect_info_fails_closed() {
        let parts = parts_with(None);

        assert!(
            matches!(peer_addr_from_parts(&parts), Err(ApiError::Internal(_))),
            "a request with no resolvable peer address must not be admitted"
        );
    }
}
