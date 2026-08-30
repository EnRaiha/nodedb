// SPDX-License-Identifier: BUSL-1.1

//! The [`Transport`] trait the detector talks to the network through.

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::swim::error::SwimError;
use crate::swim::wire::SwimMessage;

/// Abstract SWIM transport. Implementations may be unreliable (UDP-like);
/// the detector assumes nothing about ordering or delivery guarantees.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a single SWIM datagram to `to`. Errors indicate the transport
    /// itself is broken, not that the peer is unreachable — an unreachable
    /// peer is modelled as a silent drop.
    async fn send(&self, to: SocketAddr, msg: SwimMessage) -> Result<(), SwimError>;

    /// Block until the next inbound datagram is available. Returns
    /// [`SwimError::TransportClosed`] when the transport is shut down.
    async fn recv(&self) -> Result<(SocketAddr, SwimMessage), SwimError>;

    /// The local bind address — returned so callers can include it in
    /// outgoing messages without plumbing the address through separately.
    fn local_addr(&self) -> SocketAddr;
}
