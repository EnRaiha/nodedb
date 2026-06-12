// SPDX-License-Identifier: Apache-2.0

//! Zstd compression codec for cold/archived partitions.
//!
//! Higher compression ratio than LZ4 (~5-10x for structured data), slower
//! decompression. Best for sealed partitions that are read infrequently.
//!
//! Platform strategy:
//! - Native: `zstd` crate (C libzstd, fastest)
//! - WASM: `ruzstd` crate (pure Rust decoder, no C dependency)
//!
//! Wire format:
//! ```text
//! [4 bytes] uncompressed size (LE u32)
//! [1 byte]  compression level used
//! [N bytes] Zstd frame (standard format, decodable by any Zstd implementation)
//! ```
//!
//! The 5-byte header prepended to the standard Zstd frame allows us to
//! pre-allocate the output buffer on decode and store the level for metadata.

use crate::error::CodecError;

/// Default Zstd compression level (3 = good balance of speed and ratio).
pub const DEFAULT_LEVEL: i32 = 3;

/// High compression level for cold storage (19 = near-maximum ratio).
pub const HIGH_LEVEL: i32 = 19;

/// Header size: 4 bytes uncompressed size + 1 byte level.
const HEADER_SIZE: usize = 5;

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Compress raw bytes using Zstd at the default level (3).
pub fn encode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    encode_with_level(data, DEFAULT_LEVEL)
}

/// Compress raw bytes using Zstd at a specific level (1-22).
pub fn encode_with_level(data: &[u8], level: i32) -> Result<Vec<u8>, CodecError> {
    let level = level.clamp(1, 22);

    let compressed = compress_native(data, level)?;

    let mut out = Vec::with_capacity(HEADER_SIZE + compressed.len());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.push(level as u8);
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress Zstd-compressed bytes.
pub fn decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    if data.len() < HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }

    let uncompressed_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    // Byte 4 is level — informational only, not needed for decompression.
    let frame = &data[HEADER_SIZE..];

    decompress_native(frame, uncompressed_size)
}

/// Get the uncompressed size from the header without decompressing.
pub fn uncompressed_size(data: &[u8]) -> Result<usize, CodecError> {
    if data.len() < HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }
    Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize)
}

/// Get the compression level from the header.
pub fn compression_level(data: &[u8]) -> Result<i32, CodecError> {
    if data.len() < HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: HEADER_SIZE,
            actual: data.len(),
        });
    }
    Ok(data[4] as i32)
}

// ---------------------------------------------------------------------------
// Platform-specific compression / decompression
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
fn compress_native(data: &[u8], level: i32) -> Result<Vec<u8>, CodecError> {
    zstd::encode_all(std::io::Cursor::new(data), level).map_err(|e| CodecError::CompressFailed {
        detail: format!("zstd compress: {e}"),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn decompress_native(frame: &[u8], expected_size: usize) -> Result<Vec<u8>, CodecError> {
    let mut output = Vec::with_capacity(expected_size);
    let mut decoder = zstd::Decoder::new(std::io::Cursor::new(frame)).map_err(|e| {
        CodecError::DecompressFailed {
            detail: format!("zstd decoder init: {e}"),
        }
    })?;
    std::io::copy(&mut decoder, &mut output).map_err(|e| CodecError::DecompressFailed {
        detail: format!("zstd decompress: {e}"),
    })?;

    if output.len() != expected_size {
        return Err(CodecError::Corrupt {
            detail: format!(
                "zstd size mismatch: expected {expected_size}, got {}",
                output.len()
            ),
        });
    }

    Ok(output)
}

// WASM: use ruzstd for decompression. Compression on WASM uses a simple
// fallback (ruzstd is decode-only; if full Zstd encoding is needed on WASM,
// we'd need the zstd crate compiled to WASM via C-to-WASM toolchain).
// For Pattern C (Lite-local), cold compression happens infrequently, so
// we fall back to LZ4 encoding on WASM and only support Zstd decoding.

#[cfg(target_arch = "wasm32")]
fn compress_native(_data: &[u8], _level: i32) -> Result<Vec<u8>, CodecError> {
    // ruzstd is decode-only. On WASM, we encode using a minimal Zstd frame.
    // For production WASM builds that need Zstd encoding, compile the C zstd
    // library to WASM. For now, return an error directing callers to use LZ4.
    Err(CodecError::CompressFailed {
        detail: "Zstd encoding not available on WASM — use LZ4 codec instead".into(),
    })
}

#[cfg(target_arch = "wasm32")]
fn decompress_native(frame: &[u8], expected_size: usize) -> Result<Vec<u8>, CodecError> {
    use ruzstd::StreamingDecoder;
    use std::io::Read;

    let mut decoder = StreamingDecoder::new(std::io::Cursor::new(frame)).map_err(|e| {
        CodecError::DecompressFailed {
            detail: format!("ruzstd decoder init: {e}"),
        }
    })?;

    let mut output = Vec::with_capacity(expected_size);
    decoder
        .read_to_end(&mut output)
        .map_err(|e| CodecError::DecompressFailed {
            detail: format!("ruzstd decompress: {e}"),
        })?;

    if output.len() != expected_size {
        return Err(CodecError::Corrupt {
            detail: format!(
                "zstd size mismatch: expected {expected_size}, got {}",
                output.len()
            ),
        });
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Streaming encoder / decoder types
// ---------------------------------------------------------------------------

/// Streaming Zstd encoder. Accumulates data and compresses on `finish()`.
pub struct ZstdEncoder {
    buf: Vec<u8>,
    level: i32,
}

impl ZstdEncoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            level: DEFAULT_LEVEL,
        }
    }

    pub fn with_level(level: i32) -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            level: level.clamp(1, 22),
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

    pub fn finish(self) -> Result<Vec<u8>, CodecError> {
        encode_with_level(&self.buf, self.level)
    }
}

impl Default for ZstdEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Zstd decoder wrapper.
pub struct ZstdDecoder;

impl ZstdDecoder {
    pub fn decode_all(data: &[u8]) -> Result<Vec<u8>, CodecError> {
        decode(data)
    }

    pub fn uncompressed_size(data: &[u8]) -> Result<usize, CodecError> {
        uncompressed_size(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data() {
        let encoded = encode(&[]).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn small_data_roundtrip() {
        let data = b"hello world, zstd compression test";
        let encoded = encode(data).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn large_data_roundtrip() {
        let line = "2024-01-15 ERROR database connection timeout host=db-prod-01 retry=3\n";
        let data: Vec<u8> = line.as_bytes().repeat(1000);
        let encoded = encode(&data).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 5.0,
            "repetitive logs should compress >5x with zstd, got {ratio:.1}x"
        );
    }

    #[test]
    fn high_compression_level() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let default_encoded = encode(&data).unwrap();
        let high_encoded = encode_with_level(&data, HIGH_LEVEL).unwrap();

        // High level should produce smaller output (or equal).
        assert!(high_encoded.len() <= default_encoded.len() + 10);

        // Both should roundtrip correctly.
        assert_eq!(decode(&default_encoded).unwrap(), data);
        assert_eq!(decode(&high_encoded).unwrap(), data);
    }

    #[test]
    fn header_metadata() {
        let data = vec![42u8; 1000];
        let encoded = encode_with_level(&data, 7).unwrap();

        assert_eq!(uncompressed_size(&encoded).unwrap(), 1000);
        assert_eq!(compression_level(&encoded).unwrap(), 7);
    }

    #[test]
    fn better_ratio_than_lz4() {
        // Structured data where Zstd should beat LZ4.
        let mut data = Vec::new();
        for i in 0..5000 {
            let line = format!(
                "{{\"timestamp\":{},\"level\":\"INFO\",\"msg\":\"request handled\",\"duration\":{}}}",
                1700000000 + i,
                i % 100
            );
            data.extend_from_slice(line.as_bytes());
            data.push(b'\n');
        }

        let zstd_encoded = encode(&data).unwrap();
        let lz4_encoded = crate::lz4::encode(&data);

        // Zstd should compress better than LZ4.
        assert!(
            zstd_encoded.len() < lz4_encoded.len(),
            "zstd ({}) should be smaller than lz4 ({})",
            zstd_encoded.len(),
            lz4_encoded.len()
        );

        // Both roundtrip correctly.
        assert_eq!(decode(&zstd_encoded).unwrap(), data);
        assert_eq!(crate::lz4::decode(&lz4_encoded).unwrap(), data);
    }

    #[test]
    fn streaming_encoder() {
        let parts: Vec<&[u8]> = vec![b"part one ", b"part two ", b"part three"];
        let full: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();

        let mut enc = ZstdEncoder::new();
        for part in &parts {
            enc.push(part);
        }
        let encoded = enc.finish().unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, full);
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0, 0, 0, 0]).is_err()); // header too short
    }

    #[test]
    fn level_clamping() {
        let data = b"test data for clamping";
        // Level 0 → clamped to 1, level 99 → clamped to 22.
        let encoded_low = encode_with_level(data, 0).unwrap();
        let encoded_high = encode_with_level(data, 99).unwrap();
        assert_eq!(decode(&encoded_low).unwrap(), data);
        assert_eq!(decode(&encoded_high).unwrap(), data);
    }
}
