// SPDX-License-Identifier: Apache-2.0

//! W3C-compatible 128-bit trace identifiers and 64-bit span identifiers.
//!
//! `TraceId` is a 16-byte value matching the W3C `traceparent` wire format:
//! `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`.
//!
//! Both types are copy-cheap value types safe to pass by value anywhere.

use std::fmt;
use std::str::FromStr;

use rand::Rng;
use serde::{Deserialize, Serialize};

// ── TraceId ──────────────────────────────────────────────────────────────────

/// A 128-bit distributed trace identifier (W3C traceparent compatible).
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    /// The all-zero sentinel used when no trace is active.
    pub const ZERO: Self = Self([0u8; 16]);

    /// Generate a random, non-zero trace ID.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 16];
        rand::rng().fill_bytes(&mut bytes);
        // Extremely unlikely, but guarantee non-zero.
        if bytes == [0u8; 16] {
            bytes[15] = 1;
        }
        Self(bytes)
    }

    /// Parse a W3C `traceparent` header value.
    ///
    /// Expected format: `00-<32 hex>-<16 hex>-<2 hex>`
    ///
    /// Returns `(trace_id, parent_span_id, flags)` on success, `None` on any
    /// malformed input (wrong version, wrong lengths, non-hex chars, wrong
    /// field count).
    pub fn from_traceparent(s: &str) -> Option<(TraceId, SpanId, u8)> {
        let parts: Vec<&str> = s.splitn(4, '-').collect();
        if parts.len() != 4 {
            return None;
        }
        // Version must be "00".
        if parts[0] != "00" {
            return None;
        }
        // trace-id: 32 hex chars → 16 bytes. parent-id: 16 hex chars → 8
        // bytes. flags: 2 hex chars. A wrong length fails the decode.
        let mut trace_bytes = [0u8; 16];
        hex::decode_to_slice(parts[1], &mut trace_bytes).ok()?;
        let mut span_bytes = [0u8; 8];
        hex::decode_to_slice(parts[2], &mut span_bytes).ok()?;
        let mut flags = [0u8; 1];
        hex::decode_to_slice(parts[3], &mut flags).ok()?;

        Some((TraceId(trace_bytes), SpanId(span_bytes), flags[0]))
    }

    /// Render as a W3C `traceparent` header value.
    ///
    /// Format: `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`
    pub fn to_traceparent_header(&self, span_id: SpanId, flags: u8) -> String {
        format!("00-{self}-{span_id}-{flags:02x}")
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({self})")
    }
}

/// Parse error for `TraceId::from_str`.
#[derive(Debug, thiserror::Error)]
#[error("invalid TraceId: expected 32 lowercase hex chars, got {0:?}")]
pub struct TraceIdParseError(String);

impl FromStr for TraceId {
    type Err = TraceIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 16];
        hex::decode_to_slice(s, &mut bytes).map_err(|_| TraceIdParseError(s.to_owned()))?;
        Ok(TraceId(bytes))
    }
}

// ── SpanId ───────────────────────────────────────────────────────────────────

/// A 64-bit span identifier (W3C traceparent parent-id field).
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpanId(pub [u8; 8]);

impl SpanId {
    /// The all-zero sentinel.
    pub const ZERO: Self = Self([0u8; 8]);

    /// Generate a random, non-zero span ID.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 8];
        rand::rng().fill_bytes(&mut bytes);
        if bytes == [0u8; 8] {
            bytes[7] = 1;
        }
        Self(bytes)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId({self})")
    }
}

/// Parse error for `SpanId::from_str`.
#[derive(Debug, thiserror::Error)]
#[error("invalid SpanId: expected 16 lowercase hex chars, got {0:?}")]
pub struct SpanIdParseError(String);

impl FromStr for SpanId {
    type Err = SpanIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0u8; 8];
        hex::decode_to_slice(s, &mut bytes).map_err(|_| SpanIdParseError(s.to_owned()))?;
        Ok(SpanId(bytes))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_nonzero_ids() {
        let id = TraceId::generate();
        assert_ne!(id, TraceId::ZERO);
    }

    #[test]
    fn two_successive_generate_calls_differ() {
        let a = TraceId::generate();
        let b = TraceId::generate();
        // Astronomically unlikely to collide.
        assert_ne!(a, b);
    }

    #[test]
    fn display_produces_32_lowercase_hex_chars() {
        let id = TraceId::generate();
        let s = id.to_string();
        assert_eq!(s.len(), 32);
        assert!(
            s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }

    #[test]
    fn from_str_roundtrips_through_display() {
        let id = TraceId::generate();
        let s = id.to_string();
        let parsed: TraceId = s.parse().expect("parse must succeed");
        assert_eq!(id, parsed);
    }

    #[test]
    fn from_traceparent_extracts_correct_values() {
        let s = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let (tid, sid, flags) = TraceId::from_traceparent(s).expect("valid traceparent");
        // Verify trace-id bytes.
        let expected_trace = hex::decode("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        assert_eq!(tid.0.as_slice(), expected_trace.as_slice());
        // Verify span-id bytes.
        let expected_span = hex::decode("00f067aa0ba902b7").unwrap();
        assert_eq!(sid.0.as_slice(), expected_span.as_slice());
        assert_eq!(flags, 0x01);
    }

    #[test]
    fn from_traceparent_rejects_wrong_version() {
        assert!(
            TraceId::from_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn from_traceparent_rejects_wrong_trace_length() {
        // 30 hex chars instead of 32.
        assert!(
            TraceId::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e47-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn from_traceparent_rejects_wrong_field_count() {
        assert!(
            TraceId::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7")
                .is_none()
        );
    }

    #[test]
    fn from_traceparent_rejects_non_hex_chars() {
        assert!(
            TraceId::from_traceparent("00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn to_traceparent_header_roundtrips_with_from_traceparent() {
        let tid = TraceId::generate();
        let sid = SpanId::generate();
        let header = tid.to_traceparent_header(sid, 0x01);
        let (parsed_tid, parsed_sid, flags) = TraceId::from_traceparent(&header).expect("valid");
        assert_eq!(parsed_tid, tid);
        assert_eq!(parsed_sid, sid);
        assert_eq!(flags, 0x01);
    }

    #[test]
    fn span_id_generate_nonzero() {
        let sid = SpanId::generate();
        assert_ne!(sid, SpanId::ZERO);
    }

    #[test]
    fn span_id_display_16_lowercase_hex() {
        let sid = SpanId::generate();
        let s = sid.to_string();
        assert_eq!(s.len(), 16);
        assert!(
            s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }
}
