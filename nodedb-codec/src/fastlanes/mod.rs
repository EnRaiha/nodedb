// SPDX-License-Identifier: Apache-2.0

//! FastLanes-inspired FOR + bit-packing codec for integer columns.
//!
//! Frame-of-Reference (FOR): subtract the minimum value from all values,
//! reducing them to small unsigned residuals. Then bit-pack the residuals
//! using the minimum number of bits.
//!
//! The bit-packing loop is written as simple scalar operations on contiguous
//! arrays, which LLVM auto-vectorizes to AVX2/AVX-512/NEON/WASM-SIMD without
//! explicit intrinsics. This is the FastLanes insight: structured scalar code
//! that the compiler vectorizes, portable across all targets.
//!
//! Wire format:
//! ```text
//! [4 bytes] total value count (LE u32)
//! [2 bytes] block count (LE u16)
//! For each block:
//!   [2 bytes] values in this block (LE u16, max 1024)
//!   [1 byte]  bit width (0-64)
//!   [8 bytes] min value / reference (LE i64)
//!   [N bytes] bit-packed residuals
//! ```
//!
//! Block size: 1024 values. Last block may be smaller.

mod bits;
mod block;

use std::mem::size_of;

pub use block::bit_width_for_range;

use crate::bounds::{
    checked_add, checked_capacity, checked_mul, checked_range, decoded_len, encode_input_len,
    u32_to_usize,
};
use crate::error::CodecError;
use block::{decode_block, encode_block, skip_block};

/// Block size for FastLanes processing. 1024 values aligns with SIMD
/// register widths across all targets (16 × 64-bit lanes on AVX-512,
/// 8 × 128-bit WASM v128 operations to cover 1024 elements).
const BLOCK_SIZE: usize = 1024;

/// Header: 4 bytes count + 2 bytes block_count.
const GLOBAL_HEADER_SIZE: usize = 6;

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a slice of i64 values using FOR + bit-packing.
pub fn encode(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    let total_bytes = checked_mul(values.len(), size_of::<i64>(), "FastLanes input bytes")?;
    decoded_len(total_bytes, "FastLanes input")?;
    let total_count = encode_input_len(values.len(), "FastLanes value count")?;
    let block_count = values.len().div_ceil(BLOCK_SIZE);
    let block_count = u16::try_from(block_count).map_err(|_| CodecError::ResourceLimit {
        resource: "FastLanes block count".into(),
        requested: block_count,
        limit: u16::MAX as usize,
    })?;
    let estimated = checked_add(
        GLOBAL_HEADER_SIZE,
        checked_mul(values.len(), 5, "FastLanes output estimate")?,
        "FastLanes output estimate",
    )?;
    let mut out = Vec::with_capacity(estimated);
    out.extend_from_slice(&total_count.to_le_bytes());
    out.extend_from_slice(&block_count.to_le_bytes());
    for chunk in values.chunks(BLOCK_SIZE) {
        encode_block(chunk, &mut out)?;
    }
    Ok(out)
}

/// Decode FOR + bit-packed bytes back to i64 values.
pub fn decode(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }

    let (total_count, block_count) = parse_header(data)?;
    if total_count == 0 {
        if data.len() != GLOBAL_HEADER_SIZE {
            return Err(CodecError::Corrupt {
                detail: "trailing bytes after empty FastLanes frame".into(),
            });
        }
        return Ok(Vec::new());
    }

    let value_capacity = checked_capacity(total_count, size_of::<i64>(), "FastLanes values")?;
    let mut values = Vec::with_capacity(value_capacity);
    let mut offset = GLOBAL_HEADER_SIZE;
    for block_idx in 0..block_count {
        offset = decode_block(
            data,
            offset,
            &mut values,
            block_idx,
            expected_block_count(total_count, block_idx)?,
        )?;
    }
    if offset != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after FastLanes frame".into(),
        });
    }
    if values.len() != total_count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "value count mismatch: header says {total_count}, decoded {}",
                values.len()
            ),
        });
    }

    Ok(values)
}

/// Compute byte offsets for each block in an encoded stream.
///
/// Returns a Vec of byte offsets — `offsets[i]` is the start position of
/// block `i` within `data`. O(num_blocks) header scan, no decompression.
pub fn block_byte_offsets(data: &[u8]) -> Result<Vec<usize>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let (total_count, num_blocks) = parse_header(data)?;
    let offset_capacity = checked_capacity(num_blocks, size_of::<usize>(), "FastLanes offsets")?;
    let mut offsets = Vec::with_capacity(offset_capacity);
    let mut pos = GLOBAL_HEADER_SIZE;
    for i in 0..num_blocks {
        offsets.push(pos);
        pos = skip_block(data, pos, i, expected_block_count(total_count, i)?)?;
    }
    if pos != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after FastLanes frame".into(),
        });
    }
    Ok(offsets)
}

/// Decode a range of blocks [start_block..end_block) from encoded data.
///
/// More efficient than calling `decode_single_block` repeatedly — scans
/// headers once to find start_block, then decodes contiguously.
pub fn decode_block_range(
    data: &[u8],
    start_block: usize,
    end_block: usize,
) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let (total_count, num_blocks) = parse_header(data)?;
    validate_frame(data, total_count, num_blocks)?;
    if start_block >= num_blocks || end_block > num_blocks || start_block >= end_block {
        return Ok(Vec::new());
    }

    // Skip to start_block.
    let mut offset = GLOBAL_HEADER_SIZE;
    for i in 0..start_block {
        offset = skip_block(data, offset, i, expected_block_count(total_count, i)?)?;
    }

    let selected_count = (start_block..end_block).try_fold(0usize, |count, index| {
        checked_add(
            count,
            expected_block_count(total_count, index)?,
            "FastLanes range count",
        )
    })?;
    let selected_bytes = checked_mul(selected_count, size_of::<i64>(), "FastLanes range bytes")?;
    decoded_len(selected_bytes, "FastLanes range")?;
    let selected_capacity = checked_capacity(selected_count, size_of::<i64>(), "FastLanes range")?;
    let mut values = Vec::with_capacity(selected_capacity);
    for i in start_block..end_block {
        offset = decode_block(
            data,
            offset,
            &mut values,
            i,
            expected_block_count(total_count, i)?,
        )?;
    }
    Ok(values)
}

/// Number of blocks in an encoded FastLanes stream.
pub fn block_count(data: &[u8]) -> Result<usize, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    Ok(parse_header(data)?.1)
}

/// Decode a single block by index without decoding the entire stream.
///
/// Iterates block headers to reach `block_idx`, then decodes only that
/// block. For sequential block-at-a-time processing, prefer
/// [`BlockIterator`] which tracks byte offsets without re-scanning.
pub fn decode_single_block(data: &[u8], block_idx: usize) -> Result<Vec<i64>, CodecError> {
    if data.len() < GLOBAL_HEADER_SIZE {
        return Err(CodecError::Truncated {
            expected: GLOBAL_HEADER_SIZE,
            actual: data.len(),
        });
    }
    let (total_count, num_blocks) = parse_header(data)?;
    validate_frame(data, total_count, num_blocks)?;
    if block_idx >= num_blocks {
        return Err(CodecError::Corrupt {
            detail: format!("block_idx {block_idx} >= block_count {num_blocks}"),
        });
    }

    // Skip to the target block by iterating headers.
    let mut offset = GLOBAL_HEADER_SIZE;
    for i in 0..block_idx {
        offset = skip_block(data, offset, i, expected_block_count(total_count, i)?)?;
    }

    let expected_count = expected_block_count(total_count, block_idx)?;
    let value_capacity = checked_capacity(expected_count, size_of::<i64>(), "FastLanes block")?;
    let mut values = Vec::with_capacity(value_capacity);
    decode_block(data, offset, &mut values, block_idx, expected_count)?;
    Ok(values)
}

/// Iterator that decodes one 1024-row block at a time, tracking byte
/// offsets internally. Avoids re-scanning headers for sequential access.
pub struct BlockIterator<'a> {
    data: &'a [u8],
    offset: usize,
    total_count: usize,
    blocks_remaining: usize,
    current_block: usize,
}

impl<'a> BlockIterator<'a> {
    /// Create a block iterator over encoded FastLanes data.
    pub fn new(data: &'a [u8]) -> Result<Self, CodecError> {
        if data.len() < GLOBAL_HEADER_SIZE {
            return Err(CodecError::Truncated {
                expected: GLOBAL_HEADER_SIZE,
                actual: data.len(),
            });
        }
        let (total_count, num_blocks) = parse_header(data)?;
        validate_frame(data, total_count, num_blocks)?;
        Ok(Self {
            data,
            offset: GLOBAL_HEADER_SIZE,
            total_count,
            blocks_remaining: num_blocks,
            current_block: 0,
        })
    }

    /// Skip the next block without decoding it.
    pub fn skip_block(&mut self) -> Result<(), CodecError> {
        if self.blocks_remaining == 0 {
            return Ok(());
        }
        self.offset = skip_block(
            self.data,
            self.offset,
            self.current_block,
            expected_block_count(self.total_count, self.current_block)?,
        )?;
        self.current_block += 1;
        self.blocks_remaining -= 1;
        Ok(())
    }
}

fn parse_header(data: &[u8]) -> Result<(usize, usize), CodecError> {
    let header = checked_range(data, 0, GLOBAL_HEADER_SIZE, "FastLanes header")?;
    let total_count = u32_to_usize(
        u32::from_le_bytes([header[0], header[1], header[2], header[3]]),
        "FastLanes value count",
    )?;
    let decoded_bytes = checked_mul(total_count, size_of::<i64>(), "FastLanes decoded bytes")?;
    decoded_len(decoded_bytes, "FastLanes")?;
    let block_count = usize::from(u16::from_le_bytes([header[4], header[5]]));
    let expected_blocks = total_count.div_ceil(BLOCK_SIZE);
    if block_count != expected_blocks {
        return Err(CodecError::Corrupt {
            detail: format!(
                "FastLanes block count {block_count} does not match value count {total_count}"
            ),
        });
    }
    Ok((total_count, block_count))
}

fn validate_frame(data: &[u8], total_count: usize, block_count: usize) -> Result<(), CodecError> {
    let mut offset = GLOBAL_HEADER_SIZE;
    for block_idx in 0..block_count {
        offset = skip_block(
            data,
            offset,
            block_idx,
            expected_block_count(total_count, block_idx)?,
        )?;
    }
    if offset != data.len() {
        return Err(CodecError::Corrupt {
            detail: "trailing bytes after FastLanes frame".into(),
        });
    }
    Ok(())
}

fn expected_block_count(total_count: usize, block_idx: usize) -> Result<usize, CodecError> {
    let start = checked_mul(block_idx, BLOCK_SIZE, "FastLanes block start")?;
    let remaining = total_count
        .checked_sub(start)
        .ok_or_else(|| CodecError::Corrupt {
            detail: "FastLanes block index exceeds declared count".into(),
        })?;
    Ok(remaining.min(BLOCK_SIZE))
}

impl Iterator for BlockIterator<'_> {
    type Item = Result<Vec<i64>, CodecError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.blocks_remaining == 0 {
            return None;
        }
        let expected_count = match expected_block_count(self.total_count, self.current_block) {
            Ok(count) => count,
            Err(error) => return Some(Err(error)),
        };
        let mut values = Vec::with_capacity(expected_count);
        match decode_block(
            self.data,
            self.offset,
            &mut values,
            self.current_block,
            expected_count,
        ) {
            Ok(new_offset) => {
                self.offset = new_offset;
                self.current_block += 1;
                self.blocks_remaining -= 1;
                Some(Ok(values))
            }
            Err(error) => {
                self.blocks_remaining = 0;
                Some(Err(error))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.blocks_remaining, Some(self.blocks_remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(values: &[i64]) -> Vec<u8> {
        super::encode(values).expect("test FastLanes encode")
    }

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_value() {
        let encoded = encode(&[42i64]);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, vec![42i64]);
    }

    #[test]
    fn identical_values_zero_bits() {
        let values = vec![999i64; 1024];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // All identical → bit_width=0 → only headers, no packed data.
        // Global header(6) + block header(11) = 17 bytes for 1024 values.
        assert_eq!(encoded.len(), 17);
    }

    #[test]
    fn small_range_values() {
        // Values in range [100, 107] → 3 bits per value.
        let values: Vec<i64> = (0..1024).map(|i| 100 + (i % 8)).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // 1024 values × 3 bits = 384 bytes packed + headers.
        let expected_packed = (1024usize * 3).div_ceil(8); // 384 bytes
        let expected_total = GLOBAL_HEADER_SIZE + block::BLOCK_HEADER_SIZE + expected_packed;
        assert_eq!(encoded.len(), expected_total);
    }

    #[test]
    fn constant_rate_timestamps() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 4.0,
            "timestamps should pack to <4 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn pre_delta_timestamps() {
        let deltas: Vec<i64> = vec![10_000i64; 10_000];
        let encoded = encode(&deltas);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, deltas);

        let bytes_per_sample = encoded.len() as f64 / deltas.len() as f64;
        assert!(
            bytes_per_sample < 0.2,
            "constant deltas should pack to near-zero, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn pre_delta_timestamps_with_jitter() {
        let mut deltas = Vec::with_capacity(10_000);
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng >> 33) as i64 % 101) - 50;
            deltas.push(10_000 + jitter);
        }
        let encoded = encode(&deltas);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, deltas);

        let bytes_per_sample = encoded.len() as f64 / deltas.len() as f64;
        assert!(
            bytes_per_sample < 1.5,
            "jittered deltas should pack to <1.5 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn negative_values() {
        let values: Vec<i64> = (-500..500).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn boundary_values() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn multiple_blocks() {
        let values: Vec<i64> = (0..3000).map(|i| i * 7 + 100).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn partial_last_block() {
        let values: Vec<i64> = (0..1025).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn compression_vs_raw() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode(&values);
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(ratio > 2.0, "expected >2x compression, got {ratio:.1}x");
    }

    #[test]
    fn bit_width_calculation() {
        assert_eq!(bit_width_for_range(0, 0), 0);
        assert_eq!(bit_width_for_range(100, 100), 0);
        assert_eq!(bit_width_for_range(0, 1), 1);
        assert_eq!(bit_width_for_range(0, 7), 3);
        assert_eq!(bit_width_for_range(0, 8), 4);
        assert_eq!(bit_width_for_range(0, 255), 8);
        assert_eq!(bit_width_for_range(0, 256), 9);
        assert_eq!(bit_width_for_range(i64::MIN, i64::MAX), 64);
    }

    #[test]
    fn pack_unpack_roundtrip() {
        for bw in 1..=64u8 {
            let max_val: u64 = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
            let test_vals = [0u64, 1, max_val / 2, max_val];
            for &val in &test_vals {
                let mut packed = vec![0u8; 16];
                bits::pack_bits(&mut packed, 0, val, bw);
                let unpacked = bits::unpack_bits(&packed, 0, bw);
                let mask = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
                assert_eq!(
                    unpacked & mask,
                    val & mask,
                    "pack/unpack failed for bw={bw}, val={val}"
                );
            }
        }
    }

    #[test]
    fn pack_unpack_at_offsets() {
        let mut packed = vec![0u8; 32];
        bits::pack_bits(&mut packed, 0, 0b101, 3);
        bits::pack_bits(&mut packed, 3, 0b110, 3);
        bits::pack_bits(&mut packed, 6, 0b011, 3);

        assert_eq!(bits::unpack_bits(&packed, 0, 3), 0b101);
        assert_eq!(bits::unpack_bits(&packed, 3, 3), 0b110);
        assert_eq!(bits::unpack_bits(&packed, 6, 3), 0b011);
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0, 1, 0]).is_err()); // count=1, blocks=1, no block data
    }

    #[test]
    fn large_dataset_roundtrip() {
        let mut values = Vec::with_capacity(100_000);
        let mut rng: u64 = 12345;
        for _ in 0..100_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            values.push((rng >> 1) as i64);
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn decode_single_block_correctness() {
        let values: Vec<i64> = (0..3000).collect();
        let encoded = encode(&values);
        assert_eq!(block_count(&encoded).unwrap(), 3);

        let b0 = decode_single_block(&encoded, 0).unwrap();
        assert_eq!(b0.len(), 1024);
        assert_eq!(b0, &values[..1024]);

        let b1 = decode_single_block(&encoded, 1).unwrap();
        assert_eq!(b1.len(), 1024);
        assert_eq!(b1, &values[1024..2048]);

        let b2 = decode_single_block(&encoded, 2).unwrap();
        assert_eq!(b2.len(), 952);
        assert_eq!(b2, &values[2048..]);
    }

    #[test]
    fn block_iterator_matches_full_decode() {
        let values: Vec<i64> = (0..5000).map(|i| i * 7 - 2000).collect();
        let encoded = encode(&values);

        let mut all = Vec::new();
        let iter = BlockIterator::new(&encoded).unwrap();
        for blk in iter {
            all.extend(blk.unwrap());
        }
        assert_eq!(all, values);
    }

    #[test]
    fn block_iterator_skip() {
        let values: Vec<i64> = (0..3000).collect();
        let encoded = encode(&values);

        let mut iter = BlockIterator::new(&encoded).unwrap();
        iter.skip_block().unwrap(); // skip block 0
        let b1 = iter.next().unwrap().unwrap();
        assert_eq!(b1, &values[1024..2048]);
    }

    #[test]
    fn hostile_counts_and_block_shapes_fail_before_allocation_or_looping() {
        let mut huge = vec![0; GLOBAL_HEADER_SIZE];
        huge[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        huge[4..].copy_from_slice(&u16::MAX.to_le_bytes());
        assert!(matches!(
            decode(&huge),
            Err(CodecError::ResourceLimit { .. })
        ));

        let mut mismatched = vec![0; GLOBAL_HEADER_SIZE];
        mismatched[..4].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(
            decode(&mismatched),
            Err(CodecError::Corrupt { .. })
        ));

        let mut bad_block = encode(&[1, 2]);
        bad_block[GLOBAL_HEADER_SIZE..GLOBAL_HEADER_SIZE + 2].copy_from_slice(&1u16.to_le_bytes());
        assert!(matches!(
            decode(&bad_block),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn nonzero_final_padding_is_rejected_by_all_offset_scans() {
        let mut encoded = encode(&[1, 2]);
        *encoded.last_mut().expect("packed byte") |= 0x80;
        assert!(matches!(decode(&encoded), Err(CodecError::Corrupt { .. })));
        assert!(matches!(
            block_byte_offsets(&encoded),
            Err(CodecError::Corrupt { .. })
        ));
        assert!(matches!(
            decode_block_range(&encoded, 0, 1),
            Err(CodecError::Corrupt { .. })
        ));
        assert!(matches!(
            decode_single_block(&encoded, 0),
            Err(CodecError::Corrupt { .. })
        ));
        assert!(matches!(
            BlockIterator::new(&encoded),
            Err(CodecError::Corrupt { .. })
        ));
    }

    #[test]
    fn truncated_packed_block_is_rejected_by_all_offset_scans() {
        let mut encoded = encode(&[1, 2]);
        encoded.pop();
        assert!(matches!(
            decode(&encoded),
            Err(CodecError::Truncated { .. })
        ));
        assert!(matches!(
            block_byte_offsets(&encoded),
            Err(CodecError::Truncated { .. })
        ));
        assert!(matches!(
            decode_single_block(&encoded, 0),
            Err(CodecError::Truncated { .. })
        ));
    }
}
