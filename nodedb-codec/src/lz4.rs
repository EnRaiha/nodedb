// SPDX-License-Identifier: Apache-2.0

//! Bounded LZ4 block compression for byte columns.
//!
//! Frames contain a declared total, block size, block count, a length table,
//! and independently compressed blocks. Decoding validates all framing before
//! allocation and each LZ4 block before extending the result.

use crate::bounds::{
    checked_add, checked_capacity, checked_mul, checked_range, decoded_len, encode_input_len,
    encode_u32_len, u32_to_usize,
};
use crate::error::CodecError;

/// Default block size for LZ4 compression (4 KiB).
const DEFAULT_BLOCK_SIZE: usize = 4096;
const HEADER_SIZE: usize = 12;
const MIN_BLOCK_SIZE: usize = 64;

/// Compress raw bytes using LZ4 block compression.
pub fn encode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    encode_with_block_size(data, DEFAULT_BLOCK_SIZE)
}

/// Compress with a custom block size (useful for testing or tuning).
///
/// Inputs and framing fields are rejected with typed errors when they exceed
/// the application or wire-format limits.
pub fn encode_with_block_size(data: &[u8], block_size: usize) -> Result<Vec<u8>, CodecError> {
    let total_len = encode_input_len(data.len(), "LZ4")?;
    let block_size = block_size.max(MIN_BLOCK_SIZE);
    decoded_len(block_size, "LZ4 block")?;
    let block_size_u32 = encode_u32_len(block_size, "LZ4 block size")?;
    let block_count = if data.is_empty() {
        0
    } else {
        data.len().div_ceil(block_size)
    };
    let block_count_u32 = encode_u32_len(block_count, "LZ4 block count")?;
    let table_len = checked_mul(block_count, 4, "LZ4 block-length table")?;
    let initial_capacity = checked_add(
        checked_add(HEADER_SIZE, table_len, "LZ4 output header")?,
        data.len(),
        "LZ4 output capacity",
    )?;
    let mut out = Vec::with_capacity(initial_capacity);
    out.extend_from_slice(&total_len.to_le_bytes());
    out.extend_from_slice(&block_size_u32.to_le_bytes());
    out.extend_from_slice(&block_count_u32.to_le_bytes());
    let lengths_offset = out.len();
    out.resize(lengths_offset + table_len, 0);

    for (index, chunk) in data.chunks(block_size).enumerate() {
        let compressed = lz4_flex::compress_prepend_size(chunk);
        let compressed_len = encode_u32_len(compressed.len(), "LZ4 compressed block")?;
        let table_pos = lengths_offset + index * 4;
        out[table_pos..table_pos + 4].copy_from_slice(&compressed_len.to_le_bytes());
        out.extend_from_slice(&compressed);
    }
    Ok(out)
}

/// Decompress LZ4 block-compressed bytes back to raw data.
pub fn decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    let header = read_header(data)?;
    let result_capacity = checked_capacity(header.uncompressed_size, 1, "LZ4 decoded bytes")?;
    let mut result = Vec::with_capacity(result_capacity);
    let mut block_offset = header.data_offset;

    for (index, &compressed_len) in header.block_lengths.iter().enumerate() {
        let block = checked_range(data, block_offset, compressed_len, "LZ4 block range")?;
        let expected_block_len = expected_block_len(&header, index)?;
        validate_prepended_size(block, expected_block_len, index)?;
        let decompressed = lz4_flex::decompress_size_prepended(block).map_err(|error| {
            CodecError::DecompressFailed {
                detail: format!("LZ4 block {index}: {error}"),
            }
        })?;
        if decompressed.len() != expected_block_len {
            return Err(CodecError::Corrupt {
                detail: format!(
                    "LZ4 block {index} decoded {} bytes, expected {expected_block_len}",
                    decompressed.len()
                ),
            });
        }
        result.extend_from_slice(&decompressed);
        block_offset = checked_add(block_offset, compressed_len, "LZ4 block offset")?;
    }
    if result.len() != header.uncompressed_size {
        return Err(CodecError::Corrupt {
            detail: format!(
                "LZ4 uncompressed size mismatch: header says {}, got {}",
                header.uncompressed_size,
                result.len()
            ),
        });
    }
    Ok(result)
}

/// Decompress a single block by index (for random access).
pub fn decode_block(data: &[u8], block_idx: usize) -> Result<Vec<u8>, CodecError> {
    let header = read_header(data)?;
    let compressed_len =
        *header
            .block_lengths
            .get(block_idx)
            .ok_or_else(|| CodecError::Corrupt {
                detail: format!("LZ4 block index {block_idx} out of range"),
            })?;
    let mut block_offset = header.data_offset;
    for &len in &header.block_lengths[..block_idx] {
        block_offset = checked_add(block_offset, len, "LZ4 preceding block offset")?;
    }
    let block = checked_range(data, block_offset, compressed_len, "LZ4 block range")?;
    let expected_len = expected_block_len(&header, block_idx)?;
    validate_prepended_size(block, expected_len, block_idx)?;
    let decoded = lz4_flex::decompress_size_prepended(block).map_err(|error| {
        CodecError::DecompressFailed {
            detail: format!("LZ4 block {block_idx}: {error}"),
        }
    })?;
    if decoded.len() != expected_len {
        return Err(CodecError::Corrupt {
            detail: format!(
                "LZ4 block {block_idx} decoded {} bytes, expected {expected_len}",
                decoded.len()
            ),
        });
    }
    Ok(decoded)
}

struct Lz4Header {
    uncompressed_size: usize,
    block_size: usize,
    block_lengths: Vec<usize>,
    data_offset: usize,
}

fn read_header(data: &[u8]) -> Result<Lz4Header, CodecError> {
    checked_range(data, 0, HEADER_SIZE, "LZ4 header")?;
    let uncompressed_size = decoded_len(
        u32_to_usize(read_u32(data, 0, "LZ4 size")?, "LZ4 size")?,
        "LZ4",
    )?;
    let block_size = u32_to_usize(read_u32(data, 4, "LZ4 block size")?, "LZ4 block size")?;
    let block_count = u32_to_usize(read_u32(data, 8, "LZ4 block count")?, "LZ4 block count")?;
    if !(MIN_BLOCK_SIZE..=crate::bounds::MAX_DECODED_BYTES).contains(&block_size) {
        return Err(CodecError::Corrupt {
            detail: "invalid LZ4 block size".into(),
        });
    }
    let expected_count = if uncompressed_size == 0 {
        0
    } else {
        uncompressed_size.div_ceil(block_size)
    };
    if block_count != expected_count {
        return Err(CodecError::Corrupt {
            detail: "LZ4 block count does not match declared output".into(),
        });
    }
    let table_len = checked_mul(block_count, 4, "LZ4 block-length table")?;
    let data_offset = checked_add(HEADER_SIZE, table_len, "LZ4 data offset")?;
    let table = checked_range(data, HEADER_SIZE, table_len, "LZ4 block-length table")?;
    let block_length_capacity = checked_capacity(
        block_count,
        std::mem::size_of::<usize>(),
        "LZ4 block lengths",
    )?;
    let mut block_lengths = Vec::with_capacity(block_length_capacity);
    let mut total_compressed = 0usize;
    for bytes in table.chunks_exact(4) {
        let len = u32_to_usize(
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            "LZ4 block length",
        )?;
        total_compressed = checked_add(total_compressed, len, "LZ4 compressed payload")?;
        block_lengths.push(len);
    }
    checked_range(
        data,
        data_offset,
        total_compressed,
        "LZ4 compressed payload",
    )?;
    if checked_add(data_offset, total_compressed, "LZ4 frame end")? != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after LZ4 frame".into(),
        });
    }
    Ok(Lz4Header {
        uncompressed_size,
        block_size,
        block_lengths,
        data_offset,
    })
}

fn read_u32(data: &[u8], offset: usize, context: &str) -> Result<u32, CodecError> {
    let bytes = checked_range(data, offset, 4, context)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn expected_block_len(header: &Lz4Header, index: usize) -> Result<usize, CodecError> {
    let position = checked_mul(index, header.block_size, "LZ4 block position")?;
    let remaining = header
        .uncompressed_size
        .checked_sub(position)
        .ok_or_else(|| CodecError::Corrupt {
            detail: "LZ4 block position exceeds declared output".into(),
        })?;
    Ok(remaining.min(header.block_size))
}

fn validate_prepended_size(block: &[u8], expected: usize, index: usize) -> Result<(), CodecError> {
    let size = u32_to_usize(
        read_u32(block, 0, "LZ4 block size prefix")?,
        "LZ4 prepended output size",
    )?;
    if size != expected {
        return Err(CodecError::Corrupt {
            detail: format!("LZ4 block {index} declares {size} bytes, expected {expected}"),
        });
    }
    Ok(())
}

/// Streaming LZ4 encoder. Accumulates data and compresses on `finish()`.
pub struct Lz4Encoder {
    buf: Vec<u8>,
    block_size: usize,
}
impl Lz4Encoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
    pub fn with_block_size(block_size: usize) -> Result<Self, CodecError> {
        let block_size = block_size.max(MIN_BLOCK_SIZE);
        decoded_len(block_size, "LZ4 block")?;
        Ok(Self {
            buf: Vec::new(),
            block_size,
        })
    }
    pub fn push(&mut self, data: &[u8]) -> Result<(), CodecError> {
        let next_len = checked_add(self.buf.len(), data.len(), "LZ4 streaming input")?;
        decoded_len(next_len, "LZ4 streaming input")?;
        self.buf.extend_from_slice(data);
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    pub fn finish(self) -> Result<Vec<u8>, CodecError> {
        encode_with_block_size(&self.buf, self.block_size)
    }
}
impl Default for Lz4Encoder {
    fn default() -> Self {
        Self::new()
    }
}
pub struct Lz4Decoder;
impl Lz4Decoder {
    pub fn decode_all(data: &[u8]) -> Result<Vec<u8>, CodecError> {
        decode(data)
    }
    pub fn decode_block(data: &[u8], block_idx: usize) -> Result<Vec<u8>, CodecError> {
        decode_block(data, block_idx)
    }
    pub fn block_count(data: &[u8]) -> Result<usize, CodecError> {
        Ok(read_header(data)?.block_lengths.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounds::MAX_DECODED_BYTES;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]).expect("encode");
        assert!(decode(&encoded).expect("decode").is_empty());
    }

    #[test]
    fn small_data_roundtrip() {
        let data = b"hello world, this is a log message";
        let encoded = encode(data).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), data);
    }

    #[test]
    fn large_data_multiple_blocks() {
        let mut data = Vec::new();
        for i in 0..1000 {
            let line = format!(
                "2024-01-15T10:30:{:02}.000Z INFO request_id={} status=200 duration_ms={}\n",
                i % 60,
                10000 + i,
                i * 3 + 1
            );
            data.extend_from_slice(line.as_bytes());
        }
        let encoded = encode(&data).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), data);
        assert!(data.len() as f64 / encoded.len() as f64 > 2.0);
    }

    #[test]
    fn random_access_block() {
        let data: Vec<u8> = (0..20_000).map(|i| (i % 256) as u8).collect();
        let block_size = 4096;
        let encoded = encode_with_block_size(&data, block_size).expect("encode");
        let block_count = Lz4Decoder::block_count(&encoded).expect("count");
        assert_eq!(block_count, data.len().div_ceil(block_size));
        let mut reassembled = Vec::new();
        for index in 0..block_count {
            reassembled.extend_from_slice(&decode_block(&encoded, index).expect("block"));
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn out_of_range_block_index() {
        let encoded = encode(b"some data here").expect("encode");
        assert!(decode_block(&encoded, 999).is_err());
    }

    #[test]
    fn compressible_log_data_exceeds_three_to_one() {
        let line = "2024-01-15 ERROR database connection timeout host=db-prod-01 retry=3\n";
        let data = line.as_bytes().repeat(500);
        let encoded = encode(&data).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), data);
        assert!(data.len() as f64 / encoded.len() as f64 > 3.0);
    }

    #[test]
    fn incompressible_data_roundtrip() {
        let mut data = vec![0u8; 10_000];
        let mut rng: u64 = 9999;
        for byte in &mut data {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (rng >> 33) as u8;
        }
        let encoded = encode(&data).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), data);
    }

    #[test]
    fn streaming_encoder() {
        let mut encoder = Lz4Encoder::new();
        encoder.push(b"hello ").expect("push");
        encoder.push(b"world").expect("push");
        let encoded = encoder.finish().expect("finish");
        assert_eq!(decode(&encoded).expect("decode"), b"hello world");
    }

    #[test]
    fn custom_block_size() {
        let data = vec![42u8; 10_000];
        let encoded = encode_with_block_size(&data, 1024).expect("encode");
        assert_eq!(decode(&encoded).expect("decode"), data);
        assert_eq!(Lz4Decoder::block_count(&encoded).expect("count"), 10);
    }

    #[test]
    fn rejects_huge_count_and_output_before_allocation() {
        let mut frame = vec![0; HEADER_SIZE];
        frame[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        frame[4..8].copy_from_slice(&64u32.to_le_bytes());
        frame[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&frame),
            Err(CodecError::ResourceLimit { .. })
        ));
        assert!(decoded_len(MAX_DECODED_BYTES + 1, "test").is_err());
    }

    #[test]
    fn rejects_truncated_table_payload_and_oversized_block_output() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0; 8]).is_err());
        let mut frame = vec![0; HEADER_SIZE];
        frame[0..4].copy_from_slice(&64u32.to_le_bytes());
        frame[4..8].copy_from_slice(&64u32.to_le_bytes());
        frame[8..12].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(decode(&frame), Err(CodecError::Truncated { .. })));
        frame.extend_from_slice(&4u32.to_le_bytes());
        frame.extend_from_slice(&65u32.to_le_bytes());
        assert!(matches!(decode(&frame), Err(CodecError::Corrupt { .. })));
    }

    #[test]
    fn random_access_rejects_short_nonfinal_block() {
        let encoded = encode_with_block_size(&[7; 128], 64).expect("encode");
        let first_len = read_u32(&encoded, HEADER_SIZE, "first length").expect("length") as usize;
        let second_start = HEADER_SIZE + 8 + first_len;
        let empty = lz4_flex::compress_prepend_size(&[]);
        let mut malformed = encoded[..HEADER_SIZE + 8].to_vec();
        malformed[HEADER_SIZE..HEADER_SIZE + 4]
            .copy_from_slice(&(empty.len() as u32).to_le_bytes());
        malformed.extend_from_slice(&empty);
        malformed.extend_from_slice(&encoded[second_start..]);
        assert!(matches!(
            decode_block(&malformed, 0),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn encoder_constructor_and_stream_reject_oversize_before_allocation() {
        assert!(matches!(
            Lz4Encoder::with_block_size(usize::MAX),
            Err(CodecError::ResourceLimit { .. })
        ));
        let mut encoder = Lz4Encoder {
            buf: vec![0; MAX_DECODED_BYTES],
            block_size: DEFAULT_BLOCK_SIZE,
        };
        assert!(matches!(
            encoder.push(&[1]),
            Err(CodecError::ResourceLimit { .. })
        ));
        assert_eq!(encoder.len(), MAX_DECODED_BYTES);
    }
}
