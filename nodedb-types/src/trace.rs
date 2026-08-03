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
        // trace-id: 32 lowercase hex chars → 16 bytes.
        let trace_hex = parts[1];
        if trace_hex.len() != 32 {
            return None;
        }
        let mut trace_bytes = [0u8; 16];
        for (i, chunk) in trace_hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_val(chunk[0])?;
            let lo = hex_val(chunk[1])?;
            trace_bytes[i] = (hi << 4) | lo;
        }
        // parent-id: 16 lowercase hex chars → 8 bytes.
        let span_hex = parts[2];
        if span_hex.len() != 16 {
            return None;
        }
        let mut span_bytes = [0u8; 8];
        for (i, chunk) in span_hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_val(chunk[0])?;
            let lo = hex_val(chunk[1])?;
            span_bytes[i] = (hi << 4) | lo;
        }
        // flags: exactly 2 hex chars.
        let flags_hex = parts[3];
        if flags_hex.len() != 2 {
            return None;
        }
        let fhi = hex_val(flags_hex.as_bytes()[0])?;
        let flo = hex_val(flags_hex.as_bytes()[1])?;
        let flags = (fhi << 4) | flo;

        Some((TraceId(trace_bytes), SpanId(span_bytes), flags))
    }

    /// Render as a W3C `traceparent` header value.
    ///
    /// Format: `00-<32 lowercase hex>-<16 lowercase hex>-<2 hex flags>`
    pub fn to_traceparent_header(&self, span_id: SpanId, flags: u8) -> String {
        format!("00-{self}-{span_id}-{flags:02x}")
    }
}

/// Decode a single ASCII hex nibble; returns `None` for non-hex characters.
#[inline]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
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
        if s.len() != 32 {
            return Err(TraceIdParseError(s.to_owned()));
        }
        let mut bytes = [0u8; 16];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_val(chunk[0]).ok_or_else(|| TraceIdParseError(s.to_owned()))?;
            let lo = hex_val(chunk[1]).ok_or_else(|| TraceIdParseError(s.to_owned()))?;
            bytes[i] = (hi << 4) | lo;
        }
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
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
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
        if s.len() != 16 {
            return Err(SpanIdParseError(s.to_owned()));
        }
        let mut bytes = [0u8; 8];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = hex_val(chunk[0]).ok_or_else(|| SpanIdParseError(s.to_owned()))?;
            let lo = hex_val(chunk[1]).ok_or_else(|| SpanIdParseError(s.to_owned()))?;
            bytes[i] = (hi << 4) | lo;
        }
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
        let expected_trace = hex_decode_16("4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(tid.0, expected_trace);
        // Verify span-id bytes.
        let expected_span = hex_decode_8("00f067aa0ba902b7");
        assert_eq!(sid.0, expected_span);
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

    // ── helpers ──────────────────────────────────────────────────────────────

    fn hex_decode_16(s: &str) -> [u8; 16] {
        let mut out = [0u8; 16];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
        }
        out
    }

    fn hex_decode_8(s: &str) -> [u8; 8] {
        let mut out = [0u8; 8];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            out[i] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
        }
        out
    }
}
