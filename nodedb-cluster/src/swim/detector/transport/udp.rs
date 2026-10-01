// SPDX-License-Identifier: BUSL-1.1

//! Authenticated UDP transport for SWIM.
//!
//! Binds a single `tokio::net::UdpSocket` and implements [`super::Transport`]
//! by wrapping every outbound [`SwimMessage`] in an authenticated envelope
//! (HMAC-SHA256 MAC + per-peer monotonic sequence) and rejecting inbound
//! datagrams whose MAC fails, whose claimed origin does not match the
//! observed source, or whose sequence replays.
//!
//! Malformed or unauthenticated inbound bytes surface as
//! [`SwimError::Decode`] but do not close the socket — the failure
//! detector's recv loop treats non-`TransportClosed` errors as transient
//! and keeps running.
//!
//! Datagram size is capped by [`RECV_BUF_BYTES`] = 64 KiB (IPv4 UDP
//! maximum). The envelope adds 53 bytes of overhead over the bare
//! zerompk-encoded message.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use super::Transport;
use crate::rpc_codec::MacKey;
use crate::swim::error::SwimError;
use crate::swim::wire::{SwimAuth, SwimMessage, unwrap, wrap};

/// IPv4 UDP maximum datagram size. Authenticated SWIM messages with a
/// 6-entry piggyback fit in well under 2.5 KiB; 64 KiB is abundant.
pub const RECV_BUF_BYTES: usize = 65_536;

/// SWIM datagram transport backed by a real UDP socket.
#[derive(Debug)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    /// Shared MAC key + per-peer sequence state for every packet this
    /// socket sends or receives.
    auth: SwimAuth,
    /// Serializes access to the recv buffer. `recv_from` takes `&self`
    /// on `UdpSocket`, but we need a reusable buffer without allocating
    /// 64 KiB per datagram; the mutex guards that buffer.
    recv_buf: Mutex<Vec<u8>>,
}

impl UdpTransport {
    /// Bind to `addr` with the cluster MAC key. Passing `127.0.0.1:0`
    /// picks an ephemeral port, which [`UdpTransport::local_addr`] then
    /// reports.
    ///
    /// Pass [`MacKey::zero`] explicitly to run the transport in insecure
    /// mode (mirrors the Raft transport's `TransportCredentials::Insecure`
    /// escape hatch — only safe on isolated networks).
    pub async fn bind(addr: SocketAddr, mac_key: MacKey) -> Result<Self, SwimError> {
        let socket = UdpSocket::bind(addr).await.map_err(|e| SwimError::Bind {
            addr,
            detail: e.to_string(),
        })?;
        let local_addr = socket.local_addr().map_err(|e| SwimError::Bind {
            addr,
            detail: format!("local_addr: {e}"),
        })?;
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            auth: SwimAuth::new(mac_key, local_addr),
            recv_buf: Mutex::new(vec![0u8; RECV_BUF_BYTES]),
        })
    }

    /// Send every later datagram in boot `epoch`'s sequence range. A peer
    /// keeps its replay window for this address across this node's restart,
    /// so the node calls it once per boot, before the detector sends.
    pub fn enter_boot_epoch(&self, epoch: u64) {
        self.auth.enter_boot_epoch(epoch);
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn send(&self, to: SocketAddr, msg: SwimMessage) -> Result<(), SwimError> {
        let bytes = wrap(&self.auth, to, &msg)?;
        // UDP send_to is atomic per datagram; partial sends aren't a
        // thing, so we only have to handle the error case.
        self.socket
            .send_to(&bytes, to)
            .await
            .map(|_| ())
            .map_err(|e| SwimError::Encode {
                detail: format!("udp send_to {to}: {e}"),
            })
    }

    async fn recv(&self) -> Result<(SocketAddr, SwimMessage), SwimError> {
        let mut buf = self.recv_buf.lock().await;
        let (n, from) =
            self.socket
                .recv_from(&mut buf[..])
                .await
                .map_err(|e| SwimError::Decode {
                    detail: format!("udp recv_from: {e}"),
                })?;
        let msg = unwrap(&self.auth, from, &buf[..n])?;
        Ok((from, msg))
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::wire::{Ack, Ping, ProbeId};
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn any_loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    fn shared_key() -> MacKey {
        MacKey::from_bytes([0x5Au8; 32])
    }

    #[tokio::test]
    async fn bind_and_report_local_addr() {
        let t = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind");
        assert_eq!(t.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_ne!(t.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn send_and_recv_roundtrip_between_real_sockets() {
        let a = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind a");
        let b = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind b");
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(42),
            from: NodeId::try_new("a").expect("test fixture"),
            incarnation: Incarnation::new(3),
            piggyback: vec![],
        });
        a.send(b.local_addr(), ping.clone()).await.expect("send");
        let (from, msg) = tokio::time::timeout(Duration::from_secs(1), b.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(from, a.local_addr());
        assert_eq!(msg, ping);
    }

    #[tokio::test]
    async fn recv_decodes_ack_variant() {
        let a = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind a");
        let b = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind b");
        let ack = SwimMessage::Ack(Ack {
            probe_id: ProbeId::new(1),
            from: NodeId::try_new("b").expect("test fixture"),
            incarnation: Incarnation::new(7),
            piggyback: vec![],
        });
        a.send(b.local_addr(), ack.clone()).await.expect("send");
        let (_from, msg) = tokio::time::timeout(Duration::from_secs(1), b.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(msg, ack);
    }

    #[tokio::test]
    async fn decode_error_on_garbage_datagram() {
        let victim = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind");
        let sender = tokio::net::UdpSocket::bind(any_loopback())
            .await
            .expect("sender bind");
        sender
            .send_to(&[0xff_u8; 100], victim.local_addr())
            .await
            .expect("send garbage");
        let err = tokio::time::timeout(Duration::from_secs(1), victim.recv())
            .await
            .expect("recv timed out")
            .expect_err("decode should fail");
        assert!(matches!(err, SwimError::Decode { .. }));

        // Follow up with a valid authenticated datagram to prove the
        // socket still works after a bad one.
        let sender_transport = UdpTransport::bind(any_loopback(), shared_key())
            .await
            .expect("bind");
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::try_new("x").expect("test fixture"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        sender_transport
            .send(victim.local_addr(), ping.clone())
            .await
            .expect("send valid");
        let (_from, msg) = tokio::time::timeout(Duration::from_secs(1), victim.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(msg, ping);
    }

    #[tokio::test]
    async fn rejects_mismatched_cluster_key() {
        let k1 = MacKey::from_bytes([0x01u8; 32]);
        let k2 = MacKey::from_bytes([0x02u8; 32]);
        let sender = UdpTransport::bind(any_loopback(), k1)
            .await
            .expect("bind sender");
        let receiver = UdpTransport::bind(any_loopback(), k2)
            .await
            .expect("bind receiver");
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::try_new("attacker").expect("test fixture"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        sender
            .send(receiver.local_addr(), ping)
            .await
            .expect("send");
        let err = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("recv timed out")
            .expect_err("receiver must reject wrong-key MAC");
        assert!(err.to_string().contains("MAC verification failed"));
    }
}
