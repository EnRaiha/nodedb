// SPDX-License-Identifier: BUSL-1.1

//! The UDP address a node's SWIM detector binds and advertises.
//!
//! The default is the QUIC listen IP, one port above the QUIC port. Peers
//! never derive it themselves: every node advertises its bound address in
//! its topology entry, so an override on one node needs no change elsewhere.

use std::net::SocketAddr;

use crate::rpc_codec::MacKey;

use super::detector::UdpTransport;
use super::error::SwimError;

/// The SWIM address used when none is configured: the QUIC listen IP with
/// port `quic_port + 1`.
///
/// A QUIC port of `0` asks the OS for a free port, so SWIM does the same.
/// The bound port is what gets advertised.
pub fn default_swim_addr(quic_listen: SocketAddr) -> Result<SocketAddr, SwimError> {
    if quic_listen.port() == 0 {
        return Ok(quic_listen);
    }
    let port = quic_listen
        .port()
        .checked_add(1)
        .ok_or(SwimError::NoDefaultAddr { quic_listen })?;
    Ok(SocketAddr::new(quic_listen.ip(), port))
}

/// Bind the SWIM UDP socket at `configured`, or at [`default_swim_addr`] of
/// `quic_listen` when unset. A bind error is returned as [`SwimError::Bind`],
/// never retried on another port.
pub async fn bind_swim_listener(
    configured: Option<SocketAddr>,
    quic_listen: SocketAddr,
    mac_key: MacKey,
) -> Result<UdpTransport, SwimError> {
    let addr = match configured {
        Some(addr) => addr,
        None => default_swim_addr(quic_listen)?,
    };
    UdpTransport::bind(addr, mac_key).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::detector::Transport;

    #[test]
    fn default_is_the_quic_ip_one_port_up() {
        let quic: SocketAddr = "10.1.2.3:9400".parse().expect("literal address");
        assert_eq!(
            default_swim_addr(quic).expect("derivable"),
            "10.1.2.3:9401"
                .parse::<SocketAddr>()
                .expect("literal address")
        );
        let quic_v6: SocketAddr = "[fd00::7]:7000".parse().expect("literal address");
        assert_eq!(
            default_swim_addr(quic_v6).expect("derivable"),
            "[fd00::7]:7001"
                .parse::<SocketAddr>()
                .expect("literal address")
        );
    }

    #[test]
    fn the_highest_quic_port_has_no_default() {
        let quic: SocketAddr = "10.1.2.3:65535".parse().expect("literal address");
        assert!(matches!(
            default_swim_addr(quic),
            Err(SwimError::NoDefaultAddr { quic_listen }) if quic_listen == quic
        ));
    }

    #[test]
    fn an_os_assigned_quic_port_gives_an_os_assigned_swim_port() {
        let quic: SocketAddr = "127.0.0.1:0".parse().expect("literal address");
        assert_eq!(default_swim_addr(quic).expect("derivable"), quic);
    }

    #[tokio::test]
    async fn a_taken_port_fails_with_the_address() {
        let first = bind_swim_listener(
            Some("127.0.0.1:0".parse().expect("literal address")),
            "127.0.0.1:0".parse().expect("literal address"),
            MacKey::zero(),
        )
        .await
        .expect("bind a free port");
        let taken = first.local_addr();

        match bind_swim_listener(Some(taken), taken, MacKey::zero()).await {
            Err(SwimError::Bind { addr, .. }) => assert_eq!(addr, taken),
            Err(other) => panic!("expected a bind error naming {taken}, got {other}"),
            Ok(_) => panic!("binding a taken port must fail"),
        }
    }
}
