// SPDX-License-Identifier: BUSL-1.1

//! RPC frame header layout and framing helpers.
//!
//! # Current layout (18 bytes)
//!
//! ```text
//! ┌─────────┬──────────┬────────────┬──────────┬───────────────┬─────────────────────┐
//! │ version │ rpc_type │ payload_len│ crc32c   │ cluster_epoch │ rkyv payload bytes  │
//! │  1 byte │  1 byte  │  4 bytes   │ 4 bytes  │   8 bytes LE  │  payload_len bytes  │
//! └─────────┴──────────┴────────────┴──────────┴───────────────┴─────────────────────┘
//! ```
//!
//! Every frame is emitted with `RPC_FRAME_VERSION` in the version byte.
//! Frames with any other version byte are rejected immediately.

use crate::cluster_epoch::{
    current_local_cluster_epoch, observe_peer_cluster_epoch, validate_peer_cluster_epoch,
};
use crate::error::{ClusterError, Result};

/// Header size in bytes: version(1) + rpc_type(1) + payload_len(4) + crc32c(4) + cluster_epoch(8).
pub const HEADER_SIZE: usize = 18;

/// Wire-version byte stamped on every outbound frame and required on every inbound frame.
const RPC_FRAME_VERSION: u8 = 3;

/// Maximum RPC message payload size (64 MiB). Distinct from WAL's MAX_RPC_PAYLOAD_SIZE.
///
/// Prevents degenerate allocations from corrupt frames.
pub const MAX_RPC_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Write a framed v3 header + payload into `out`.
///
/// `rpc_type` is the discriminant byte; `payload` is the already-serialized
/// body. The current cluster epoch (read from
/// [`current_local_cluster_epoch`]) is stamped into the header.
pub fn write_frame(rpc_type: u8, payload: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let payload_len: u32 = payload.len().try_into().map_err(|_| ClusterError::Codec {
        detail: format!("payload too large: {} bytes", payload.len()),
    })?;
    let crc = crc32c::crc32c(payload);
    let epoch = current_local_cluster_epoch();
    out.push(RPC_FRAME_VERSION);
    out.push(rpc_type);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&epoch.to_le_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Validate the CRC32C of an inbound frame and return the payload slice.
///
/// `data` must start at byte 0 (version byte). Returns `(rpc_type, payload)`.
///
/// Side effect: observes the peer's cluster epoch via
/// [`observe_peer_cluster_epoch`] (monotonic max).
pub fn parse_frame(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.is_empty() {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: 0 bytes, need {HEADER_SIZE}"),
        });
    }

    let version = data[0];
    if version != RPC_FRAME_VERSION {
        return Err(ClusterError::UnsupportedWireVersion {
            got: version,
            supported_min: RPC_FRAME_VERSION,
            supported_max: RPC_FRAME_VERSION,
        });
    }

    if data.len() < HEADER_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("frame too short: {} bytes, need {HEADER_SIZE}", data.len()),
        });
    }

    let rpc_type = data[1];
    let payload_len = u32::from_le_bytes([data[2], data[3], data[4], data[5]]);
    let expected_crc = u32::from_le_bytes([data[6], data[7], data[8], data[9]]);
    let peer_epoch = u64::from_le_bytes([
        data[10], data[11], data[12], data[13], data[14], data[15], data[16], data[17],
    ]);

    // Enforce the cluster-epoch fence. Join handshake + ping/pong frames are
    // exempt (see cluster_epoch::EPOCH_EXEMPT_RPC_TYPES); everything else from
    // a peer on a strictly older epoch is dropped here, before any dispatch.
    validate_peer_cluster_epoch(rpc_type, peer_epoch)?;

    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }

    let expected_total = HEADER_SIZE + payload_len as usize;
    if data.len() < expected_total {
        return Err(ClusterError::Codec {
            detail: format!(
                "frame truncated: got {} bytes, expected {expected_total}",
                data.len()
            ),
        });
    }

    let payload = &data[HEADER_SIZE..expected_total];
    let actual_crc = crc32c::crc32c(payload);
    if actual_crc != expected_crc {
        return Err(ClusterError::Codec {
            detail: format!(
                "CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
            ),
        });
    }

    if peer_epoch > 0 {
        observe_peer_cluster_epoch(peer_epoch);
    }

    Ok((rpc_type, payload))
}

/// Return the total frame size for a buffer that starts with a valid header.
///
/// The buffer must be at least [`HEADER_SIZE`] bytes.
pub fn frame_size(header: &[u8; HEADER_SIZE]) -> Result<usize> {
    let payload_len = u32::from_le_bytes([header[2], header[3], header[4], header[5]]);
    if payload_len > MAX_RPC_PAYLOAD_SIZE {
        return Err(ClusterError::Codec {
            detail: format!("payload length {payload_len} exceeds maximum {MAX_RPC_PAYLOAD_SIZE}"),
        });
    }
    Ok(HEADER_SIZE + payload_len as usize)
}

// rkyv_deserialize and rkyv_serialize are macros in each sub-module because
// rkyv's generic bounds for Serialize and Deserialize are cumbersome to
// express generically across all types. Each sub-module calls rkyv directly.

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a frame with the given version byte and a 10-byte (old short) header shape.
    /// Used to test rejection of non-current version bytes.
    fn make_short_frame(version: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(version);
        out.push(0xAB); // arbitrary rpc_type
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// Build a correct current-version frame (18-byte header).
    fn make_frame(rpc_type: u8, payload: &[u8], epoch: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + payload.len());
        let payload_len = payload.len() as u32;
        let crc = crc32c::crc32c(payload);
        out.push(RPC_FRAME_VERSION);
        out.push(rpc_type);
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn parse_frame_accepts_current_version() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        let _g = crate::cluster_epoch::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // Local epoch 0: any stamp (including 0) passes the fence.
        set_local_cluster_epoch(0);
        let payload = b"world";
        let frame = make_frame(0xAB, payload, 0);
        let (_t, body) = parse_frame(&frame).expect("current version must be accepted");
        assert_eq!(body, payload);
    }

    #[test]
    fn parse_frame_rejects_n_plus_one() {
        let payload = b"future";
        let frame = make_short_frame(RPC_FRAME_VERSION.saturating_add(1), payload);
        let err = parse_frame(&frame).expect_err("N+1 must be rejected");
        match err {
            ClusterError::UnsupportedWireVersion {
                got,
                supported_min,
                supported_max,
            } => {
                assert_eq!(got, RPC_FRAME_VERSION.saturating_add(1));
                assert_eq!(supported_min, RPC_FRAME_VERSION);
                assert_eq!(supported_max, RPC_FRAME_VERSION);
            }
            other => panic!("expected UnsupportedWireVersion, got {other:?}"),
        }
    }

    #[test]
    fn parse_frame_rejects_v1_v2() {
        for version in [1u8, 2u8] {
            let payload = b"old";
            let frame = make_short_frame(version, payload);
            let err = parse_frame(&frame).expect_err(&format!("v{version} must be rejected"));
            assert!(
                matches!(
                    err,
                    ClusterError::UnsupportedWireVersion { got, .. } if got == version
                ),
                "expected UnsupportedWireVersion for v{version}, got {err:?}"
            );
        }
    }

    #[test]
    fn parse_frame_rejects_version_zero() {
        let payload = b"zero";
        let frame = make_short_frame(0, payload);
        let err = parse_frame(&frame).expect_err("version 0 must be rejected");
        assert!(matches!(
            err,
            ClusterError::UnsupportedWireVersion { got: 0, .. }
        ));
    }

    #[test]
    fn v3_frame_round_trips_with_epoch() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        let _g = crate::cluster_epoch::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        set_local_cluster_epoch(0);
        set_local_cluster_epoch(7);
        let payload = b"epoch-bound";
        let mut buf = Vec::new();
        write_frame(0xAB, payload, &mut buf).unwrap();
        set_local_cluster_epoch(0);
        let (rpc_type, body) = parse_frame(&buf).unwrap();
        assert_eq!(rpc_type, 0xAB);
        assert_eq!(body, payload);
        assert_eq!(crate::cluster_epoch::current_local_cluster_epoch(), 7);
        set_local_cluster_epoch(0);
    }

    #[test]
    fn frame_size_current_version() {
        let frame = make_frame(0xAB, b"abcde", 1);
        let mut hdr = [0u8; HEADER_SIZE];
        hdr.copy_from_slice(&frame[..HEADER_SIZE]);
        assert_eq!(frame_size(&hdr).unwrap(), HEADER_SIZE + 5);
    }

    #[test]
    fn parse_frame_rejects_stale_epoch() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        // Serialise against the cluster_epoch tests on the shared global.
        let _g = crate::cluster_epoch::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        set_local_cluster_epoch(7);
        let frame = make_frame(0xAB, b"stale", 5);
        let err = parse_frame(&frame).expect_err("stale epoch must be rejected");
        assert!(matches!(
            err,
            ClusterError::StalePeerEpoch {
                peer_epoch: 5,
                local_epoch: 7,
            }
        ));
        // Rejection happens before observe, so the local mark is untouched.
        assert_eq!(crate::cluster_epoch::current_local_cluster_epoch(), 7);
        set_local_cluster_epoch(0);
    }

    #[test]
    fn parse_frame_accepts_newer_epoch_and_observes() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        let _g = crate::cluster_epoch::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        set_local_cluster_epoch(3);
        let frame = make_frame(0xAB, b"newer", 9);
        let (rpc_type, body) = parse_frame(&frame).expect("newer epoch must be accepted");
        assert_eq!(rpc_type, 0xAB);
        assert_eq!(body, b"newer");
        assert_eq!(crate::cluster_epoch::current_local_cluster_epoch(), 9);
        set_local_cluster_epoch(0);
    }

    #[test]
    fn parse_frame_join_and_ping_exempt() {
        use crate::cluster_epoch::set_local_cluster_epoch;
        use crate::rpc_codec::discriminants::{RPC_JOIN_REQ, RPC_PING};
        let _g = crate::cluster_epoch::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        set_local_cluster_epoch(9);
        let join = make_frame(RPC_JOIN_REQ, b"join", 0);
        parse_frame(&join).expect("join req must be exempt from the epoch fence");
        let ping = make_frame(RPC_PING, b"ping", 0);
        parse_frame(&ping).expect("ping must be exempt from the epoch fence");
        assert_eq!(crate::cluster_epoch::current_local_cluster_epoch(), 9);
        set_local_cluster_epoch(0);
    }
}
