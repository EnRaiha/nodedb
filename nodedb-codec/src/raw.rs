// SPDX-License-Identifier: Apache-2.0

//! Raw (identity) codec — no compression.
//!
//! Passes data through unchanged. Used for symbol columns (already
//! dictionary-encoded as 4-byte u32 IDs) or pre-compressed data.
//!
//! Wire format:
//! ```text
//! [4 bytes] data length (LE u32)
//! [N bytes] raw data
//! ```
//!
//! The length header is included for consistency with other codecs
//! (allows the decoder to validate data integrity).

use crate::error::CodecError;

/// Encode raw bytes (identity codec with length header).
pub fn encode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + data.len());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(data);
    out
}

/// Decode raw bytes (validates length header).
pub fn decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    }

    let expected_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let payload = &data[4..];

    if payload.len() < expected_len {
        return Err(CodecError::Truncated {
            expected: expected_len.saturating_add(4),
            actual: data.len(),
        });
    }

    Ok(payload[..expected_len].to_vec())
}

/// Return a reference to the raw data without copying (zero-copy decode).
///
/// Useful when the caller can work with a borrowed slice.
pub fn decode_ref(data: &[u8]) -> Result<&[u8], CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    }

    let expected_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let payload = &data[4..];

    if payload.len() < expected_len {
        return Err(CodecError::Truncated {
            expected: expected_len.saturating_add(4),
            actual: data.len(),
        });
    }

    Ok(&payload[..expected_len])
}

// ---------------------------------------------------------------------------
// Streaming encoder / decoder types (trivial wrappers)
// ---------------------------------------------------------------------------

/// Streaming Raw encoder.
pub struct RawEncoder {
    buf: Vec<u8>,
}

impl RawEncoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn finish(self) -> Vec<u8> {
        encode(&self.buf)
    }
}

impl Default for RawEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw decoder wrapper.
pub struct RawDecoder;

impl RawDecoder {
    pub fn decode_all(data: &[u8]) -> Result<Vec<u8>, CodecError> {
        decode(data)
    }

    pub fn decode_ref(data: &[u8]) -> Result<&[u8], CodecError> {
        decode_ref(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn data_roundtrip() {
        let data = b"hello world";
        let encoded = encode(data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
        // 4 header + 11 data = 15
        assert_eq!(encoded.len(), 15);
    }

    #[test]
    fn zero_copy_decode() {
        let data = b"test data";
        let encoded = encode(data);
        let slice = decode_ref(&encoded).unwrap();
        assert_eq!(slice, data.as_ref());
    }

    #[test]
    fn binary_data() {
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn u32_symbol_ids() {
        // Typical symbol column: array of u32 IDs as LE bytes.
        let ids: Vec<u32> = (0..1000).collect();
        let raw: Vec<u8> = ids.iter().flat_map(|id| id.to_le_bytes()).collect();
        let encoded = encode(&raw);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn streaming_encoder() {
        let mut enc = RawEncoder::new();
        enc.push(b"hello ");
        enc.push(b"world");
        let encoded = enc.finish();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, b"hello world");
    }

    #[test]
    fn truncated_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[10, 0, 0, 0, 1, 2]).is_err()); // claims 10 bytes, only 2
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn maximal_length_header_returns_decode_error() {
        let error = decode(&u32::MAX.to_le_bytes()).expect_err("truncated input must fail");
        assert!(matches!(
            error,
            CodecError::Truncated {
                expected: usize::MAX,
                actual: 4
            }
        ));
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn maximal_length_header_returns_decode_ref_error() {
        let error = decode_ref(&u32::MAX.to_le_bytes()).expect_err("truncated input must fail");
        assert!(matches!(
            error,
            CodecError::Truncated {
                expected: usize::MAX,
                actual: 4
            }
        ));
    }
}
