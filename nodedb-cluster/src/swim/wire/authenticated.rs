// SPDX-License-Identifier: BUSL-1.1

//! Authenticated SWIM datagram wire.
//!
//! SWIM has no TLS layer — UDP datagrams travel in the clear. The
//! authenticated envelope is the only thing standing between the cluster
//! and an off-network attacker who wants to forge or replay SWIM packets.
//!
//! # Envelope
//!
//! Each UDP datagram is an
//! [`auth_envelope`](crate::rpc_codec::auth_envelope) around the zerompk
//! encoding of [`SwimMessage`]:
//!
//! ```text
//! [env_ver | addr_hash | seq | inner_len | msgpack(SwimMessage) | mac]
//! ```
//!
//! - `addr_hash` is a stable 64-bit hash of the **sender's** UDP socket
//!   address. On receive the verifier must assert `addr_hash ==
//!   hash(observed_remote)` — this binds the envelope claim to the
//!   packet's actual origin, rejecting captured frames replayed from a
//!   different IP by an attacker who somehow has the MAC key.
//! - `mac` is HMAC-SHA256 over every byte of the envelope except the MAC.
//! - `seq` is a per-remote-address monotonic counter with a 64-entry
//!   sliding-window replay detector.
//!
//! # Relationship to Raft envelope
//!
//! Same [`auth_envelope`] primitive, same MAC algorithm, same cluster
//! MAC key. The only layering difference is the choice of `from_node_id`
//! field — Raft uses the u64 node id, SWIM uses the hash of the socket
//! address because SWIM's identity model is address-centric. The MAC
//! key is the cluster-wide shared secret so cross-transport replay
//! attacks (captured Raft frame replayed as SWIM, or vice versa) are
//! still detected — the envelope's `env_ver` field is shared and MAC
//! verification will succeed, but the inner bytes are in different
//! formats so deserialisation in the target handler will fail.

use std::net::SocketAddr;

use sha2::{Digest, Sha256};

use crate::rpc_codec::auth_envelope;
use crate::rpc_codec::{MacKey, PeerSeqSender, PeerSeqWindow};
use crate::swim::error::SwimError;

use super::{codec, message::SwimMessage};

/// Deterministic hash of a `SocketAddr` into a `u64`.
///
/// Uses SHA-256 truncated to the first 8 bytes. Deterministic across
/// processes and hosts so that sender and receiver agree on the value.
/// Not a MAC — the envelope MAC is the actual integrity primitive; this
/// hash exists only as a binding between the envelope's claimed origin
/// and the packet's observed source address.
pub fn addr_hash(addr: SocketAddr) -> u64 {
    let mut h = Sha256::new();
    h.update(addr.to_string().as_bytes());
    let digest = h.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("sha256 is 32 bytes"))
}

/// Per-transport auth state: MAC key, per-peer outbound counters, and a
/// per-peer inbound replay-detection window. Owned by the
/// [`UdpTransport`](crate::swim::detector::transport::udp::UdpTransport).
#[derive(Debug)]
pub struct SwimAuth {
    mac_key: MacKey,
    local_addr_hash: u64,
    seq_out: PeerSeqSender,
    seq_in: PeerSeqWindow,
}

impl SwimAuth {
    /// Construct from the cluster MAC key and the bound local address.
    /// Passing [`MacKey::zero`] is the insecure-mode opt-out — MAC and
    /// replay detection are cosmetic in that mode but the wire format
    /// stays identical so mixed-mode misconfiguration fails loudly
    /// instead of succeeding by accident.
    pub fn new(mac_key: MacKey, local_addr: SocketAddr) -> Self {
        Self {
            mac_key,
            local_addr_hash: addr_hash(local_addr),
            seq_out: PeerSeqSender::new(),
            seq_in: PeerSeqWindow::new(),
        }
    }

    /// Move the outbound counter into boot `epoch`'s sequence range (see
    /// [`PeerSeqSender::enter_boot_epoch`]).
    pub fn enter_boot_epoch(&self, epoch: u64) {
        self.seq_out.enter_boot_epoch(epoch);
    }

    /// Hash of the local bound address — used as the envelope's
    /// `from_node_id` on every outbound datagram.
    pub fn local_addr_hash(&self) -> u64 {
        self.local_addr_hash
    }
}

/// Encode `msg` and wrap it in an authenticated envelope destined for
/// `to`. Returns the bytes to hand to `UdpSocket::send_to`.
///
/// `to` is retained in the signature as documentation (callers pass the
/// destination they are about to `send_to`), but the outbound seq is a
/// single sender-global counter — see `PeerSeqSender`.
pub fn wrap(auth: &SwimAuth, _to: SocketAddr, msg: &SwimMessage) -> Result<Vec<u8>, SwimError> {
    let inner = codec::encode(msg)?;
    let seq = auth.seq_out.next();
    let mut out = Vec::with_capacity(auth_envelope::ENVELOPE_OVERHEAD + inner.len());
    auth_envelope::write_envelope(auth.local_addr_hash, seq, &inner, &auth.mac_key, &mut out)
        .map_err(|e| SwimError::Encode {
            detail: format!("swim envelope: {e}"),
        })?;
    Ok(out)
}

/// Parse an inbound datagram: verify MAC, bind envelope's advertised
/// origin to the observed remote address, reject replays, then decode
/// the inner [`SwimMessage`].
pub fn unwrap(auth: &SwimAuth, from: SocketAddr, bytes: &[u8]) -> Result<SwimMessage, SwimError> {
    let (fields, inner_frame) =
        auth_envelope::parse_envelope(bytes, &auth.mac_key).map_err(|e| SwimError::Decode {
            detail: format!("swim envelope: {e}"),
        })?;

    let expected = addr_hash(from);
    if fields.from_node_id != expected {
        return Err(SwimError::Decode {
            detail: format!(
                "swim envelope from {from} claimed addr_hash {}, observed hash {}",
                fields.from_node_id, expected
            ),
        });
    }

    auth.seq_in
        .accept(fields.from_node_id, fields.seq)
        .map_err(|e| SwimError::Decode {
            detail: format!("swim replay: {e}"),
        })?;

    codec::decode(inner_frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::wire::probe::{Ping, ProbeId};
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn sample_msg() -> SwimMessage {
        SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::try_new("a").expect("test fixture"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        })
    }

    #[test]
    fn addr_hash_is_deterministic() {
        // addr_hash is deterministic across processes — same input,
        // same output. This is required for cross-node agreement on
        // the envelope's from_node_id binding.
        assert_eq!(addr_hash(addr(7001)), addr_hash(addr(7001)));
        assert_ne!(addr_hash(addr(7001)), addr_hash(addr(7002)));
    }

    #[test]
    fn roundtrip_across_independent_endpoints() {
        // Two separate SwimAuth instances (as in a real two-node
        // cluster) must agree on the envelope binding because
        // addr_hash is deterministic.
        let key = MacKey::from_bytes([0x11u8; 32]);
        let sender = SwimAuth::new(key.clone(), addr(7001));
        let receiver = SwimAuth::new(key, addr(7002));
        let bytes = wrap(&sender, addr(7002), &sample_msg()).unwrap();
        // Receiver observed the datagram coming from addr(7001).
        let msg = unwrap(&receiver, addr(7001), &bytes).unwrap();
        assert_eq!(msg, sample_msg());
    }

    #[test]
    fn rejects_spoofed_source_address() {
        // Attacker captures a datagram sender A → receiver B, then
        // replays it with their own IP as source. The receiver's
        // addr_hash(observed_source) will differ from the envelope's
        // embedded addr_hash, so binding check fails.
        let key = MacKey::from_bytes([0x33u8; 32]);
        let real_sender = SwimAuth::new(key.clone(), addr(7001));
        let receiver = SwimAuth::new(key, addr(7002));
        let bytes = wrap(&real_sender, addr(7002), &sample_msg()).unwrap();
        let err = unwrap(&receiver, addr(9999), &bytes).unwrap_err();
        assert!(err.to_string().contains("addr_hash"));
    }

    #[test]
    fn rejects_tampered_mac() {
        let key = MacKey::from_bytes([3u8; 32]);
        let sender = SwimAuth::new(key.clone(), addr(7001));
        let receiver = SwimAuth::new(key, addr(7002));
        let mut bytes = wrap(&sender, addr(7002), &sample_msg()).unwrap();
        let mac_start = bytes.len() - 32;
        bytes[mac_start] ^= 0xFF;
        let err = unwrap(&receiver, addr(7001), &bytes).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }

    #[test]
    fn rejects_replay() {
        let key = MacKey::from_bytes([4u8; 32]);
        let sender = SwimAuth::new(key.clone(), addr(7001));
        let receiver = SwimAuth::new(key, addr(7002));
        let bytes = wrap(&sender, addr(7002), &sample_msg()).unwrap();
        unwrap(&receiver, addr(7001), &bytes).unwrap();
        let err = unwrap(&receiver, addr(7001), &bytes).unwrap_err();
        assert!(err.to_string().contains("replayed"));
    }

    #[test]
    fn rejects_wrong_cluster_key() {
        let k1 = MacKey::from_bytes([1u8; 32]);
        let k2 = MacKey::from_bytes([2u8; 32]);
        let sender = SwimAuth::new(k1, addr(7001));
        let receiver = SwimAuth::new(k2, addr(7002));
        let bytes = wrap(&sender, addr(7002), &sample_msg()).unwrap();
        let err = unwrap(&receiver, addr(7001), &bytes).unwrap_err();
        assert!(err.to_string().contains("MAC verification failed"));
    }
}
