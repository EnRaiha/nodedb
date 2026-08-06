// SPDX-License-Identifier: BUSL-1.1

//! Peer-address normalization for risk scoring.
//!
//! Transports carry a peer address in whatever shape their socket layer
//! produced (`10.1.2.3:5432`, `[::1]:5432`, or a bare address). The scorer
//! keys its known-IP set on the address alone, so the port has to go — and
//! anything that is not an address at all must be rejected rather than
//! coerced. A scorer fed a placeholder would silently mis-score every
//! request behind that transport, which is worse than not scoring at all:
//! the caller leaves `$auth.risk_score` unset and the gate fails closed.

use std::net::{IpAddr, SocketAddr};

/// Extract the client IP from a transport-supplied peer address.
///
/// Returns `None` when `peer` is not an IP address or `address:port` pair.
pub fn client_ip_from_peer(peer: &str) -> Option<String> {
    let peer = peer.trim();
    if let Ok(addr) = peer.parse::<SocketAddr>() {
        return Some(addr.ip().to_string());
    }
    if let Ok(ip) = peer.parse::<IpAddr>() {
        return Some(ip.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_the_port_from_a_socket_address() {
        assert_eq!(
            client_ip_from_peer("10.1.2.3:5432").as_deref(),
            Some("10.1.2.3")
        );
        assert_eq!(client_ip_from_peer("[::1]:5432").as_deref(), Some("::1"));
    }

    #[test]
    fn accepts_a_bare_address() {
        assert_eq!(
            client_ip_from_peer("192.0.2.7").as_deref(),
            Some("192.0.2.7")
        );
    }

    /// A transport label, an empty string, or a socket path is not a remote
    /// address and must never be scored as if it were an IP.
    #[test]
    fn rejects_a_non_address_placeholder() {
        assert_eq!(client_ip_from_peer("http"), None);
        assert_eq!(client_ip_from_peer(""), None);
        assert_eq!(client_ip_from_peer("unix:/tmp/nodedb.sock"), None);
    }
}
