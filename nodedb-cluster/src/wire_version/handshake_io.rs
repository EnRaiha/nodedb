// SPDX-License-Identifier: BUSL-1.1

//! Async I/O wrappers for the wire-version handshake exchange.
//!
//! Framing: every message is preceded by a 4-byte big-endian length prefix
//! followed by a zerompk-serialized payload. This is intentionally distinct
//! from the main RPC envelope so the handshake can be parsed before the
//! agreed version is known.
//!
//! # Protocol flow
//!
//! ```text
//! Client                          Server
//!   |-- VersionHandshake -------->|
//!   |<-- VersionHandshakeAck -----|   (or QUIC conn.close(0x01, reason))
//!   |   (agreed version stored)   |
//!   |   normal RPC frames ...     |
//! ```
//!
//! On a range mismatch the server closes the QUIC connection with application
//! error code `0x01` and a UTF-8 reason string before sending an ack.
//! The client observes a `ClusterError::Transport` wrapping the QUIC close.

use super::error::WireVersionError;
use super::negotiation::{VersionHandshake, VersionHandshakeAck, VersionRange, negotiate};
use super::types::WireVersion;
use crate::error::{ClusterError, Result};
use crate::wire::WIRE_VERSION;

/// Maximum byte size for a handshake frame (guards against corrupt / malicious
/// length prefixes before we allocate a receive buffer).
const MAX_HANDSHAKE_BYTES: u32 = 4 * 1024; // 4 KiB — far more than needed

/// The local version range derived from the compile-time constant in
/// `crate::wire::WIRE_VERSION`. Single source of truth for both sides.
///
/// Floor == ceiling: this build advertises only its own exact frame
/// version, never a range of supported older versions — there is no
/// rolling-upgrade window pre-1.0 (see `nodedb_types::wire_version`).
pub fn local_version_range() -> VersionRange {
    VersionRange::new(WireVersion(WIRE_VERSION), WireVersion(WIRE_VERSION))
}

/// This build's exact identity, compared for equality in every handshake.
pub fn local_build_id() -> &'static str {
    nodedb_types::wire_version::WIRE_BUILD_ID
}

/// Compare two build identities for exact equality.
fn check_build_id(local: &str, remote: &str) -> std::result::Result<(), WireVersionError> {
    if local != remote {
        return Err(WireVersionError::BuildIdMismatch {
            local_build_id: local.to_owned(),
            peer_build_id: remote.to_owned(),
        });
    }
    Ok(())
}

/// Write a length-prefixed zerompk message to `send`.
async fn write_framed<T: serde::Serialize + zerompk::ToMessagePack>(
    send: &mut quinn::SendStream,
    msg: &T,
) -> Result<()> {
    let payload = zerompk::to_msgpack_vec(msg).map_err(|e| ClusterError::Codec {
        detail: format!("handshake serialize: {e}"),
    })?;
    let len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("handshake message too large: {} bytes", payload.len()),
    })?;
    send.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("handshake write length: {e}"),
        })?;
    send.write_all(&payload)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("handshake write payload: {e}"),
        })?;
    Ok(())
}

/// Read a length-prefixed zerompk message from `recv`.
async fn read_framed<T: serde::de::DeserializeOwned + for<'a> zerompk::FromMessagePack<'a>>(
    recv: &mut quinn::RecvStream,
) -> Result<T> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("handshake read length: {e}"),
        })?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_HANDSHAKE_BYTES {
        return Err(ClusterError::Codec {
            detail: format!("handshake frame too large: {len} bytes (max {MAX_HANDSHAKE_BYTES})"),
        });
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| ClusterError::Transport {
            detail: format!("handshake read payload: {e}"),
        })?;
    zerompk::from_msgpack(&buf).map_err(|e| ClusterError::Codec {
        detail: format!("handshake deserialize: {e}"),
    })
}

/// Server-side handshake: read a [`VersionHandshake`] from the peer, negotiate
/// the agreed version, and send back a [`VersionHandshakeAck`].
///
/// On a range mismatch the QUIC connection is closed with application error
/// code `0x01` and a descriptive UTF-8 reason before returning
/// `ClusterError::Transport`.
///
/// Returns the negotiated [`WireVersion`] on success.
pub async fn perform_version_handshake_server(
    conn: &quinn::Connection,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<WireVersion> {
    let client_hs: VersionHandshake = read_framed(recv).await?;
    let remote_range = client_hs.to_range();
    let local = local_version_range();

    let agreed = negotiate(local, remote_range).map_err(|e| {
        let reason = e.to_string();
        let reason_bytes = reason.as_bytes();
        conn.close(
            quinn::VarInt::from_u32(0x01),
            &reason_bytes[..reason_bytes.len().min(100)],
        );
        ClusterError::Transport {
            detail: format!("wire version handshake failed (server): {e}"),
        }
    })?;

    if let Err(e) = check_build_id(local_build_id(), &client_hs.build_id) {
        let reason = e.to_string();
        let reason_bytes = reason.as_bytes();
        conn.close(
            quinn::VarInt::from_u32(0x01),
            &reason_bytes[..reason_bytes.len().min(100)],
        );
        return Err(ClusterError::Transport {
            detail: format!("wire version handshake failed (server): {e}"),
        });
    }

    let ack = VersionHandshakeAck::new(agreed, local_build_id().to_owned());
    write_framed(send, &ack).await?;

    Ok(agreed)
}

/// Client-side handshake: send a [`VersionHandshake`] to the server and read
/// the [`VersionHandshakeAck`].
///
/// Returns the negotiated [`WireVersion`] on success. On QUIC connection close
/// or a mismatched ack, returns `ClusterError::Transport`.
pub async fn perform_version_handshake_client(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<WireVersion> {
    let local = local_version_range();
    let hs = VersionHandshake::from_range(local, local_build_id().to_owned());
    write_framed(send, &hs).await?;

    let ack: VersionHandshakeAck = read_framed(recv).await?;
    let agreed = ack.agreed_version();

    // Validate that the server's agreed version falls within our local range.
    // A well-behaved server will only pick something in the intersection, but
    // we must not silently accept a version we cannot speak.
    if !local.contains(agreed) {
        return Err(ClusterError::Transport {
            detail: format!(
                "server returned agreed version {} outside our supported range {}..={}",
                agreed, local.min, local.max
            ),
        });
    }

    // Belt-and-suspenders: the server already refuses a build mismatch
    // before sending an ack, but a misbehaving server must not be trusted
    // silently either.
    check_build_id(local_build_id(), &ack.build_id).map_err(|e| ClusterError::Transport {
        detail: format!("wire version handshake failed (client): {e}"),
    })?;

    Ok(agreed)
}

/// Map a [`WireVersionError`] to a `ClusterError::Transport` with the QUIC
/// connection closed. Helper shared by callers that have already negotiated
/// but encounter a per-frame version mismatch.
pub fn close_on_version_error(conn: &quinn::Connection, e: WireVersionError) -> ClusterError {
    let reason = e.to_string();
    let reason_bytes = reason.as_bytes();
    conn.close(
        quinn::VarInt::from_u32(0x01),
        &reason_bytes[..reason_bytes.len().min(100)],
    );
    ClusterError::Transport {
        detail: format!("wire version mismatch: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire_version::negotiation::VersionRange;
    use crate::wire_version::types::WireVersion;

    fn v(n: u16) -> WireVersion {
        WireVersion(n)
    }

    fn range(min: u16, max: u16) -> VersionRange {
        VersionRange::new(v(min), v(max))
    }

    /// Verify that `local_version_range()` is consistent with the header
    /// constants and is a valid range.
    #[test]
    fn local_range_is_valid() {
        let r = local_version_range();
        assert!(
            r.min <= r.max,
            "local range min ({}) must be <= max ({})",
            r.min,
            r.max
        );
        assert_eq!(r.min, v(WIRE_VERSION));
        assert_eq!(r.max, v(WIRE_VERSION));
    }

    /// Verify in-range happy path negotiation without I/O.
    #[test]
    fn negotiate_in_range_succeeds() {
        let local = local_version_range();
        // A remote that exactly matches the local range must succeed.
        let result = negotiate(local, local);
        assert!(
            result.is_ok(),
            "identical ranges must negotiate: {result:?}"
        );
        assert_eq!(result.unwrap(), local.max);
    }

    /// Verify that disjoint ranges surface a NegotiationFailed error.
    #[test]
    fn negotiate_disjoint_range_fails() {
        // Force a range that cannot overlap with any valid local range.
        // local_version_range() has max == WIRE_VERSION_MAX (u8 cast to u16).
        // Pick a remote whose min = u16::MAX — definitely disjoint.
        let local = range(1, 2);
        let remote = range(100, 200);
        let err = negotiate(local, remote).unwrap_err();
        assert!(
            matches!(err, WireVersionError::NegotiationFailed { .. }),
            "expected NegotiationFailed, got: {err}"
        );
    }

    /// Verify capabilities field survives a serialize/deserialize roundtrip.
    #[test]
    fn handshake_capabilities_roundtrip() {
        let caps = 0xABCD_1234_5678_EF01_u64;
        let hs = VersionHandshake {
            range: (1, 3),
            capabilities: caps,
            build_id: "test-build".to_owned(),
        };
        let bytes = zerompk::to_msgpack_vec(&hs).unwrap();
        let decoded: VersionHandshake = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.capabilities, caps);
        assert_eq!(decoded.range, (1, 3));
    }

    /// Verify capabilities field in ack survives a serialize/deserialize roundtrip.
    #[test]
    fn ack_capabilities_roundtrip() {
        let caps = 0xFEDC_BA98_7654_3210_u64;
        let ack = VersionHandshakeAck {
            agreed: 2,
            capabilities: caps,
            build_id: "test-build".to_owned(),
        };
        let bytes = zerompk::to_msgpack_vec(&ack).unwrap();
        let decoded: VersionHandshakeAck = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.agreed_version(), v(2));
        assert_eq!(decoded.capabilities, caps);
    }

    /// A build-id mismatch is refused with the operator-facing message.
    #[test]
    fn build_id_mismatch_is_refused_with_message() {
        let err = check_build_id("build-a", "build-b").unwrap_err();
        assert!(matches!(err, WireVersionError::BuildIdMismatch { .. }));
        let msg = err.to_string();
        assert!(msg.contains("all nodes must run one build before 1.0"));
        assert!(msg.contains("build-a"));
        assert!(msg.contains("build-b"));
    }

    /// Matching build identities are accepted.
    #[test]
    fn matching_build_id_is_accepted() {
        assert!(check_build_id("same-build", "same-build").is_ok());
    }
}
