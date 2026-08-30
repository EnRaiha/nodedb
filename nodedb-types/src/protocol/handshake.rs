// SPDX-License-Identifier: Apache-2.0

//! Handshake frames and per-operation limits for the native binary protocol.

use serde::{Deserialize, Serialize};

// ─── Protocol Constants ─────────────────────────────────────────────

/// Maximum frame payload size (16 MiB).
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Length of the frame header (4-byte big-endian u32 payload length).
pub const FRAME_HEADER_LEN: usize = 4;

/// Default server port for the native protocol.
pub const DEFAULT_NATIVE_PORT: u16 = 6433;

/// Current native protocol version advertised in `HelloFrame`.
pub const PROTO_VERSION: u16 = 1;

/// Minimum protocol version this server accepts from clients.
pub const PROTO_VERSION_MIN: u16 = 1;

/// Maximum protocol version this server can speak.
pub const PROTO_VERSION_MAX: u16 = PROTO_VERSION;

// ─── Capability Bits ────────────────────────────────────────────────

/// Capability bit: server supports streaming (partial-response chunking).
pub const CAP_STREAMING: u64 = 1 << 0;
/// Capability bit: server supports GraphRAG fusion (`GraphRagFusion` opcode).
pub const CAP_GRAPHRAG: u64 = 1 << 1;
/// Capability bit: server supports full-text search opcodes.
pub const CAP_FTS: u64 = 1 << 2;
/// Capability bit: server supports CRDT sync.
pub const CAP_CRDT: u64 = 1 << 3;
/// Capability bit: server supports spatial operations.
pub const CAP_SPATIAL: u64 = 1 << 4;
/// Capability bit: server supports timeseries operations.
pub const CAP_TIMESERIES: u64 = 1 << 5;
/// Capability bit: server supports columnar scan.
pub const CAP_COLUMNAR: u64 = 1 << 6;

/// Capability bit: connection uses MessagePack framing (always set for native protocol).
pub const CAP_MSGPACK: u64 = 1 << 7;

// ─── Per-Operation Limits ───────────────────────────────────────────

/// Per-operation capability limits negotiated during the connection handshake.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_vector_dim: Option<u32>,
    pub max_top_k: Option<u32>,
    pub max_scan_limit: Option<u32>,
    pub max_batch_size: Option<u32>,
    pub max_crdt_delta_bytes: Option<u32>,
    pub max_query_text_bytes: Option<u32>,
    pub max_graph_depth: Option<u32>,
}

// ─── HelloFrame ─────────────────────────────────────────────────────

/// First frame sent by the client after TCP connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloFrame {
    pub proto_min: u16,
    pub proto_max: u16,
    pub capabilities: u64,
}

/// Magic bytes for `HelloFrame`: b"NDBH".
pub const HELLO_MAGIC: u32 = 0x4E44_4248;

impl HelloFrame {
    pub const WIRE_SIZE: usize = 16;

    /// Build a `HelloFrame` advertising the current protocol range and all capabilities.
    pub fn current() -> Self {
        Self {
            proto_min: PROTO_VERSION_MIN,
            proto_max: PROTO_VERSION_MAX,
            capabilities: CAP_STREAMING
                | CAP_GRAPHRAG
                | CAP_FTS
                | CAP_CRDT
                | CAP_SPATIAL
                | CAP_TIMESERIES
                | CAP_COLUMNAR
                | CAP_MSGPACK,
        }
    }

    pub fn encode(&self) -> [u8; Self::WIRE_SIZE] {
        let mut buf = [0u8; Self::WIRE_SIZE];
        buf[0..4].copy_from_slice(&HELLO_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.proto_min.to_be_bytes());
        buf[6..8].copy_from_slice(&self.proto_max.to_be_bytes());
        buf[8..16].copy_from_slice(&self.capabilities.to_be_bytes());
        buf
    }

    pub fn decode(buf: &[u8; Self::WIRE_SIZE]) -> Option<Self> {
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != HELLO_MAGIC {
            return None;
        }
        let proto_min = u16::from_be_bytes([buf[4], buf[5]]);
        let proto_max = u16::from_be_bytes([buf[6], buf[7]]);
        let capabilities = u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Some(Self {
            proto_min,
            proto_max,
            capabilities,
        })
    }
}

// ─── HelloAckFrame ──────────────────────────────────────────────────

/// Server's response to a `HelloFrame`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAckFrame {
    pub proto_version: u16,
    pub capabilities: u64,
    pub server_version: String,
    pub limits: Limits,
}

/// Magic bytes for `HelloAckFrame`: b"NDBA".
pub const HELLO_ACK_MAGIC: u32 = 0x4E44_4241;

impl HelloAckFrame {
    pub fn encode(&self) -> Vec<u8> {
        let sv = self.server_version.as_bytes();
        let sv_len = sv.len().min(255) as u8;
        let mut buf = Vec::with_capacity(15 + sv_len as usize + 1 + 7 * 5);
        buf.extend_from_slice(&HELLO_ACK_MAGIC.to_be_bytes());
        buf.extend_from_slice(&self.proto_version.to_be_bytes());
        buf.extend_from_slice(&self.capabilities.to_be_bytes());
        buf.push(sv_len);
        buf.extend_from_slice(&sv[..sv_len as usize]);
        buf.push(1u8);
        encode_limit_field(&mut buf, self.limits.max_vector_dim);
        encode_limit_field(&mut buf, self.limits.max_top_k);
        encode_limit_field(&mut buf, self.limits.max_scan_limit);
        encode_limit_field(&mut buf, self.limits.max_batch_size);
        encode_limit_field(&mut buf, self.limits.max_crdt_delta_bytes);
        encode_limit_field(&mut buf, self.limits.max_query_text_bytes);
        encode_limit_field(&mut buf, self.limits.max_graph_depth);
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 15 {
            return None;
        }
        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != HELLO_ACK_MAGIC {
            return None;
        }
        let proto_version = u16::from_be_bytes([data[4], data[5]]);
        let capabilities = u64::from_be_bytes([
            data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13],
        ]);
        let sv_len = data[14] as usize;
        let sv_end = 15 + sv_len;
        if data.len() < sv_end {
            return None;
        }
        let server_version = String::from_utf8_lossy(&data[15..sv_end]).into_owned();
        let mut limits = Limits::default();
        if data.len() > sv_end && data[sv_end] == 1 {
            let mut pos = sv_end + 1;
            limits.max_vector_dim = decode_limit_field(data, &mut pos);
            limits.max_top_k = decode_limit_field(data, &mut pos);
            limits.max_scan_limit = decode_limit_field(data, &mut pos);
            limits.max_batch_size = decode_limit_field(data, &mut pos);
            limits.max_crdt_delta_bytes = decode_limit_field(data, &mut pos);
            limits.max_query_text_bytes = decode_limit_field(data, &mut pos);
            limits.max_graph_depth = decode_limit_field(data, &mut pos);
        }
        Some(Self {
            proto_version,
            capabilities,
            server_version,
            limits,
        })
    }
}

// ─── HelloErrorFrame ─────────────────────────────────────────────────

/// Error code sent in a `HelloErrorFrame` when the server rejects a handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloErrorCode {
    /// The client sent a frame with an unrecognised magic number.
    BadMagic,
    /// The client's version range does not overlap with the server's.
    VersionMismatch,
    /// The frame was otherwise malformed (truncated, invalid field values, etc.)
    Malformed,
}

impl std::fmt::Display for HelloErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HelloErrorCode::BadMagic => write!(f, "BadMagic"),
            HelloErrorCode::VersionMismatch => write!(f, "VersionMismatch"),
            HelloErrorCode::Malformed => write!(f, "Malformed"),
        }
    }
}

/// Frame sent by the server when it rejects a `HelloFrame`.
///
/// Wire format (all big-endian):
/// - magic: `b"NDBE"` (4 bytes)
/// - code:  u8  (0=BadMagic, 1=VersionMismatch, 2=Malformed)
/// - msg_len: u8
/// - message: UTF-8 bytes (up to 255)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloErrorFrame {
    pub code: HelloErrorCode,
    pub message: String,
}

/// Magic bytes for `HelloErrorFrame`: `b"NDBE"`.
pub const HELLO_ERROR_MAGIC: &[u8; 4] = b"NDBE";

/// Magic for `HelloErrorFrame` as a `u32` (big-endian: 0x4E44_4245).
pub const HELLO_ERROR_MAGIC_U32: u32 = 0x4E44_4245;

impl HelloErrorFrame {
    pub fn encode(&self) -> Vec<u8> {
        let msg = self.message.as_bytes();
        let msg_len = msg.len().min(255) as u8;
        let code_byte = match self.code {
            HelloErrorCode::BadMagic => 0u8,
            HelloErrorCode::VersionMismatch => 1u8,
            HelloErrorCode::Malformed => 2u8,
        };
        let mut buf = Vec::with_capacity(6 + msg_len as usize);
        buf.extend_from_slice(HELLO_ERROR_MAGIC);
        buf.push(code_byte);
        buf.push(msg_len);
        buf.extend_from_slice(&msg[..msg_len as usize]);
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 6 {
            return None;
        }
        if &data[0..4] != HELLO_ERROR_MAGIC {
            return None;
        }
        let code = match data[4] {
            0 => HelloErrorCode::BadMagic,
            1 => HelloErrorCode::VersionMismatch,
            2 => HelloErrorCode::Malformed,
            _ => return None,
        };
        let msg_len = data[5] as usize;
        if data.len() < 6 + msg_len {
            return None;
        }
        let message = String::from_utf8_lossy(&data[6..6 + msg_len]).into_owned();
        Some(Self { code, message })
    }
}

fn encode_limit_field(buf: &mut Vec<u8>, val: Option<u32>) {
    match val {
        Some(v) => {
            buf.push(1u8);
            buf.extend_from_slice(&v.to_be_bytes());
        }
        None => {
            buf.push(0u8);
            buf.extend_from_slice(&0u32.to_be_bytes());
        }
    }
}

fn decode_limit_field(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 5 > data.len() {
        return None;
    }
    let present = data[*pos];
    let value = u32::from_be_bytes([
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
    ]);
    *pos += 5;
    if present == 1 { Some(value) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_frame_roundtrip() {
        let frame = HelloFrame {
            proto_min: 1,
            proto_max: 3,
            capabilities: CAP_STREAMING | CAP_GRAPHRAG | CAP_FTS,
        };
        let buf = frame.encode();
        let decoded = HelloFrame::decode(&buf).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_frame_bad_magic() {
        let mut buf = HelloFrame {
            proto_min: 1,
            proto_max: 1,
            capabilities: 0,
        }
        .encode();
        buf[0] = 0xFF;
        assert!(HelloFrame::decode(&buf).is_none());
    }

    #[test]
    fn hello_ack_frame_roundtrip_all_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING | CAP_CRDT,
            server_version: "0.1.0-dev".into(),
            limits: Limits {
                max_vector_dim: Some(1536),
                max_top_k: Some(1000),
                max_scan_limit: Some(10_000),
                max_batch_size: Some(512),
                max_crdt_delta_bytes: Some(1 << 20),
                max_query_text_bytes: Some(4096),
                max_graph_depth: Some(16),
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_some_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "1.0.0".into(),
            limits: Limits {
                max_vector_dim: Some(768),
                max_top_k: None,
                max_scan_limit: None,
                max_batch_size: None,
                max_crdt_delta_bytes: None,
                max_query_text_bytes: None,
                max_graph_depth: None,
            },
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_frame_roundtrip_no_limits() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: CAP_STREAMING,
            server_version: "0.2.0".into(),
            limits: Limits::default(),
        };
        let enc = frame.encode();
        let decoded = HelloAckFrame::decode(&enc).expect("decode failed");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_ack_bad_magic() {
        let frame = HelloAckFrame {
            proto_version: 1,
            capabilities: 0,
            server_version: "x".into(),
            limits: Limits::default(),
        };
        let mut enc = frame.encode();
        enc[0] = 0xFF;
        assert!(HelloAckFrame::decode(&enc).is_none());
    }

    #[test]
    fn cap_bits_non_overlapping() {
        let all = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;
        assert_eq!(all.count_ones(), 7);
    }
}
