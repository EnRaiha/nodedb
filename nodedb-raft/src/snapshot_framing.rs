// SPDX-License-Identifier: BUSL-1.1

//! Snapshot chunk framing for `InstallSnapshot` RPCs.
//!
//! # Wire layout (per chunk)
//!
//! ```text
//! [magic 4B][version u16 BE][engine_id u16 BE][crc32c u32 BE][payload]
//! ```
//!
//! - **magic**: `NDSN` (`0x4E 0x44 0x53 0x4E`) — identifies NodeDB snapshot frames.
//! - **version**: `SNAPSHOT_FORMAT_VERSION` (currently `1`). Unknown future versions
//!   are rejected. There is no v0-fallback path for snapshot data — the format is
//!   always framed.
//! - **engine_id**: discriminant from [`SnapshotEngineId`] (u16 big-endian).
//! - **crc32c**: CRC-32C computed over `engine_id bytes (2) ++ payload bytes`.
//!   Validates both the engine tag and the payload together.
//! - **payload**: engine-specific snapshot bytes.
//!
//! # Empty-payload bootstrap stub
//!
//! The current `tick.rs` sends `InstallSnapshot` with `data: vec![]` as a
//! positional marker (no real engine data yet). The receive side in
//! `handle_rpc.rs` therefore skips framing validation when `data` is
//! empty. When a real engine starts shipping snapshot data it must call
//! [`encode_snapshot_chunk`] on the sender side; the receiver enforces
//! framing for all non-empty payloads automatically.
//!
//! # Adding a new engine
//!
//! 1. Add a variant to [`SnapshotEngineId`] with a unique discriminant.
//! 2. Call [`encode_snapshot_chunk`] when building the chunk.
//! 3. The receiver's `decode_snapshot_chunk` requires no changes.

use thiserror::Error;

/// Four-byte magic that identifies a NodeDB snapshot frame.
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"NDSN";

/// Format version embedded in every snapshot frame header.
///
/// A single version shared across all engines. Per-engine versioning
/// (if ever needed) belongs inside the payload, not the frame header.
pub const SNAPSHOT_FORMAT_VERSION: u16 = 1;

/// Fixed byte length of the frame header (magic + version + engine_id + crc32c).
const HEADER_LEN: usize = 4 + 2 + 2 + 4;

/// Engine identifier embedded in every snapshot frame.
///
/// The discriminant is stored as a `u16` big-endian in the wire format.
/// Gaps in the numbering are intentional — they leave room for future
/// engines without renumbering existing ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SnapshotEngineId {
    Vector = 1,
    Graph = 2,
    DocumentSchemaless = 3,
    DocumentStrict = 4,
    Columnar = 5,
    KeyValue = 6,
    Fts = 7,
    Spatial = 8,
    Crdt = 9,
    /// Whole-tenant DataPlane snapshot spanning every engine (a serialized
    /// `TenantDataSnapshot`), as shipped by the cluster InstallSnapshot path.
    /// Not a single storage engine — tags the composite payload so the frame
    /// header carries a meaningful engine id instead of borrowing another
    /// engine's discriminant.
    Composite = 10,
}

impl SnapshotEngineId {
    /// Parse a raw `u16` discriminant from the wire.
    pub fn from_u16(v: u16) -> Result<Self, SnapshotFramingError> {
        match v {
            1 => Ok(Self::Vector),
            2 => Ok(Self::Graph),
            3 => Ok(Self::DocumentSchemaless),
            4 => Ok(Self::DocumentStrict),
            5 => Ok(Self::Columnar),
            6 => Ok(Self::KeyValue),
            7 => Ok(Self::Fts),
            8 => Ok(Self::Spatial),
            9 => Ok(Self::Crdt),
            10 => Ok(Self::Composite),
            other => Err(SnapshotFramingError::UnknownEngineId(other)),
        }
    }
}

/// Errors produced by snapshot frame encoding and decoding.
#[derive(Debug, Error, Clone)]
pub enum SnapshotFramingError {
    #[error("snapshot frame magic mismatch: expected {SNAPSHOT_MAGIC:?}, got {0:?}")]
    MagicMismatch([u8; 4]),

    #[error("snapshot frame version mismatch: expected {SNAPSHOT_FORMAT_VERSION}, got {0}")]
    VersionMismatch(u16),

    #[error("snapshot frame CRC mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { stored: u32, computed: u32 },

    #[error("unknown snapshot engine id: {0}")]
    UnknownEngineId(u16),

    #[error("snapshot frame truncated: need at least {HEADER_LEN} bytes, got {0}")]
    Truncated(usize),
}

impl From<SnapshotFramingError> for crate::error::RaftError {
    fn from(e: SnapshotFramingError) -> Self {
        crate::error::RaftError::SnapshotFormat {
            detail: e.to_string(),
        }
    }
}

/// Encode a single snapshot chunk into the framed wire format.
///
/// The CRC-32C is computed over the two `engine_id` bytes followed by
/// all `payload` bytes, then the result is prefixed with the fixed header.
pub fn encode_snapshot_chunk(engine_id: SnapshotEngineId, payload: &[u8]) -> Vec<u8> {
    let engine_bytes = (engine_id as u16).to_be_bytes();
    let crc = {
        let mut h = crc32c::crc32c(&engine_bytes);
        h = crc32c::crc32c_append(h, payload);
        h
    };

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&SNAPSHOT_MAGIC);
    out.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_be_bytes());
    out.extend_from_slice(&engine_bytes);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a framed snapshot chunk, returning the engine id and a slice
/// into the original buffer pointing at the payload (zero-copy).
///
/// Validates magic, version, and CRC in that order; returns the first
/// error encountered.
pub fn decode_snapshot_chunk(
    data: &[u8],
) -> Result<(SnapshotEngineId, &[u8]), SnapshotFramingError> {
    if data.len() < HEADER_LEN {
        return Err(SnapshotFramingError::Truncated(data.len()));
    }

    // Magic — safe: we verified data.len() >= HEADER_LEN (12) above.
    let magic: [u8; 4] = [data[0], data[1], data[2], data[3]];
    if magic != SNAPSHOT_MAGIC {
        return Err(SnapshotFramingError::MagicMismatch(magic));
    }

    // Version.
    let version = u16::from_be_bytes([data[4], data[5]]);
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotFramingError::VersionMismatch(version));
    }

    // Engine id.
    let engine_id_raw = u16::from_be_bytes([data[6], data[7]]);
    let engine_id = SnapshotEngineId::from_u16(engine_id_raw)?;

    // CRC (over engine_id bytes + payload).
    let stored_crc = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    let payload = &data[HEADER_LEN..];
    let computed_crc = {
        let mut h = crc32c::crc32c(&data[6..8]); // engine_id bytes
        h = crc32c::crc32c_append(h, payload);
        h
    };
    if stored_crc != computed_crc {
        return Err(SnapshotFramingError::CrcMismatch {
            stored: stored_crc,
            computed: computed_crc,
        });
    }

    Ok((engine_id, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_ENGINES: &[SnapshotEngineId] = &[
        SnapshotEngineId::Vector,
        SnapshotEngineId::Graph,
        SnapshotEngineId::DocumentSchemaless,
        SnapshotEngineId::DocumentStrict,
        SnapshotEngineId::Columnar,
        SnapshotEngineId::KeyValue,
        SnapshotEngineId::Fts,
        SnapshotEngineId::Spatial,
        SnapshotEngineId::Crdt,
        SnapshotEngineId::Composite,
    ];

    #[test]
    fn roundtrip_all_engine_ids() {
        for &engine_id in ALL_ENGINES {
            let payload = b"test snapshot payload";
            let framed = encode_snapshot_chunk(engine_id, payload);
            let (decoded_id, decoded_payload) = decode_snapshot_chunk(&framed).unwrap();
            assert_eq!(decoded_id, engine_id);
            assert_eq!(decoded_payload, payload);
        }
    }

    #[test]
    fn roundtrip_empty_payload() {
        let framed = encode_snapshot_chunk(SnapshotEngineId::KeyValue, &[]);
        let (id, payload) = decode_snapshot_chunk(&framed).unwrap();
        assert_eq!(id, SnapshotEngineId::KeyValue);
        assert!(payload.is_empty());
    }

    #[test]
    fn tamper_magic_returns_magic_mismatch() {
        let mut framed = encode_snapshot_chunk(SnapshotEngineId::Vector, b"data");
        framed[0] ^= 0xFF;
        let err = decode_snapshot_chunk(&framed).unwrap_err();
        assert!(
            matches!(err, SnapshotFramingError::MagicMismatch(_)),
            "{err}"
        );
    }

    #[test]
    fn tamper_version_returns_version_mismatch() {
        let mut framed = encode_snapshot_chunk(SnapshotEngineId::Graph, b"data");
        // Flip the version bytes to something != SNAPSHOT_FORMAT_VERSION.
        let bad_version = SNAPSHOT_FORMAT_VERSION.wrapping_add(1).to_be_bytes();
        framed[4] = bad_version[0];
        framed[5] = bad_version[1];
        let err = decode_snapshot_chunk(&framed).unwrap_err();
        assert!(
            matches!(err, SnapshotFramingError::VersionMismatch(_)),
            "{err}"
        );
    }

    #[test]
    fn tamper_crc_returns_crc_mismatch() {
        let mut framed = encode_snapshot_chunk(SnapshotEngineId::Fts, b"important data");
        // Flip a bit in the CRC field (bytes 8..12).
        framed[9] ^= 0x01;
        let err = decode_snapshot_chunk(&framed).unwrap_err();
        assert!(
            matches!(err, SnapshotFramingError::CrcMismatch { .. }),
            "{err}"
        );
    }

    #[test]
    fn reject_unknown_engine_id() {
        // Manually construct a frame with a valid header but unknown engine_id = 99.
        let engine_id_raw: u16 = 99;
        let engine_bytes = engine_id_raw.to_be_bytes();
        let payload = b"payload";
        let crc = {
            let mut h = crc32c::crc32c(&engine_bytes);
            h = crc32c::crc32c_append(h, payload);
            h
        };
        let mut frame = Vec::new();
        frame.extend_from_slice(&SNAPSHOT_MAGIC);
        frame.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_be_bytes());
        frame.extend_from_slice(&engine_bytes);
        frame.extend_from_slice(&crc.to_be_bytes());
        frame.extend_from_slice(payload);

        let err = decode_snapshot_chunk(&frame).unwrap_err();
        assert!(
            matches!(err, SnapshotFramingError::UnknownEngineId(99)),
            "{err}"
        );
    }

    #[test]
    fn truncated_frame_returns_truncated_error() {
        let framed = encode_snapshot_chunk(SnapshotEngineId::Crdt, b"data");
        // Feed only 5 bytes — not enough for the full header.
        let err = decode_snapshot_chunk(&framed[..5]).unwrap_err();
        assert!(matches!(err, SnapshotFramingError::Truncated(5)), "{err}");
    }

    #[test]
    fn from_u16_roundtrip_all_discriminants() {
        for &engine_id in ALL_ENGINES {
            let raw = engine_id as u16;
            let decoded = SnapshotEngineId::from_u16(raw).unwrap();
            assert_eq!(decoded, engine_id);
        }
    }

    #[test]
    fn from_u16_unknown_returns_error() {
        let err = SnapshotEngineId::from_u16(0).unwrap_err();
        assert!(matches!(err, SnapshotFramingError::UnknownEngineId(0)));

        let err = SnapshotEngineId::from_u16(255).unwrap_err();
        assert!(matches!(err, SnapshotFramingError::UnknownEngineId(255)));
    }

    /// Asserts `NDSN` magic at [0..4], SNAPSHOT_FORMAT_VERSION == 1 BE at [4..6],
    /// and that the CRC at [8..12] covers the engine_id bytes plus payload.
    #[test]
    fn golden_raft_snapshot_frame_format() {
        let payload = b"golden-payload";
        let framed = encode_snapshot_chunk(SnapshotEngineId::KeyValue, payload);

        // Magic at [0..4].
        assert_eq!(&framed[0..4], b"NDSN", "magic mismatch");

        // Format version at [4..6] BE.
        let version = u16::from_be_bytes([framed[4], framed[5]]);
        assert_eq!(version, SNAPSHOT_FORMAT_VERSION, "version mismatch");
        assert_eq!(version, 1u16, "expected SNAPSHOT_FORMAT_VERSION == 1");

        // Engine ID at [6..8].
        let engine_id_raw = u16::from_be_bytes([framed[6], framed[7]]);
        assert_eq!(engine_id_raw, SnapshotEngineId::KeyValue as u16);

        // CRC at [8..12] BE covers engine_id bytes + payload.
        let stored_crc = u32::from_be_bytes([framed[8], framed[9], framed[10], framed[11]]);
        let engine_bytes = (SnapshotEngineId::KeyValue as u16).to_be_bytes();
        let mut h = crc32c::crc32c(&engine_bytes);
        h = crc32c::crc32c_append(h, payload);
        assert_eq!(stored_crc, h, "CRC mismatch");

        // Payload follows immediately after the 12-byte header (magic+version+engine_id+crc).
        assert_eq!(&framed[12..], payload);
    }
}
