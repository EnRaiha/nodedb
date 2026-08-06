// SPDX-License-Identifier: BUSL-1.1

//! The transport security of the connection an HTTP request arrived on.
//!
//! HTTP terminates TLS inside `axum-server`'s acceptor, so — unlike the
//! listeners that own a `ConnStream` — nothing downstream can reach the
//! `rustls` session. [`server`](super::server) therefore captures the
//! negotiated version once per accepted connection and injects it as a request
//! extension; this module is where a route reads it back.
//!
//! The value is server-injected only. It is never parsed from a header: a
//! client-supplied `X-Forwarded-Proto` style hint would let any caller claim
//! whatever transport the policy demands.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::control::security::tls_policy::TransportSecurity;

use super::auth::{ApiError, AppState};

/// Read the connection's transport security out of request extensions.
///
/// `Err` when the listener was mounted without the injection layer. That is a
/// wiring fault rather than a client error — and a request whose transport
/// cannot be established must be refused, not admitted as if it were
/// cleartext (which would silently pass a `reject_cleartext = false` policy)
/// or as if it were TLS (which would silently pass a minimum-version policy).
pub(crate) fn transport_from_parts(parts: &Parts) -> Result<TransportSecurity, ApiError> {
    parts
        .extensions
        .get::<TransportSecurity>()
        .copied()
        .ok_or_else(|| ApiError::Internal("connection transport security unavailable".into()))
}

/// Extractor form of [`transport_from_parts`], for routes that resolve their
/// own identity instead of going through the `ResolvedIdentity` /
/// `ResolvedAuth` extractors.
pub struct ClientTransport(TransportSecurity);

impl ClientTransport {
    /// The captured transport security.
    pub fn security(&self) -> TransportSecurity {
        self.0
    }
}

impl FromRequestParts<AppState> for ClientTransport {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(ClientTransport(transport_from_parts(parts)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::tls_policy::TlsVersion;

    fn parts_with(transport: Option<TransportSecurity>) -> Parts {
        let (mut parts, _) = axum::http::Request::new(()).into_parts();
        if let Some(transport) = transport {
            parts.extensions.insert(transport);
        }
        parts
    }

    #[test]
    fn reads_the_injected_transport() {
        let parts = parts_with(Some(TransportSecurity::Tls(TlsVersion::Tls1_3)));
        assert_eq!(
            transport_from_parts(&parts).expect("injected"),
            TransportSecurity::Tls(TlsVersion::Tls1_3)
        );

        let parts = parts_with(Some(TransportSecurity::Cleartext));
        assert_eq!(
            transport_from_parts(&parts).expect("injected"),
            TransportSecurity::Cleartext
        );
    }

    #[test]
    fn a_missing_injection_fails_closed() {
        let parts = parts_with(None);
        assert!(
            matches!(transport_from_parts(&parts), Err(ApiError::Internal(_))),
            "a request whose transport is unknown must not be admitted"
        );
    }
}
