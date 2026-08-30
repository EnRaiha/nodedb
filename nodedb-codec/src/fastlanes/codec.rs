// SPDX-License-Identifier: Apache-2.0

//! Whole-stream encode / decode for the FastLanes FOR + bit-packing codec.

use std::mem::size_of;

use crate::bounds::{checked_add, checked_capacity, checked_mul, decoded_len, encode_input_len};
use crate::error::CodecError;

use super::block::{decode_block, encode_block};
use super::header::{GLOBAL_HEADER_SIZE, expected_block_count, parse_header};

/// Encode a slice of i64 values using FOR + bit-packing.
pub fn encode(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    let total_bytes = checked_mul(values.len(), size_of::<i64>(), "FastLanes input bytes")?;
    decoded_len(total_bytes, "FastLanes input")?;
    let total_count = encode_input_len(values.len(), "FastLanes value count")?;
    let block_count = values.len().div_ceil(super::header::BLOCK_SIZE);
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
    for chunk in values.chunks(super::header::BLOCK_SIZE) {
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

#[cfg(test)]
mod tests {
    use super::super::bits;
    use super::super::block;
    use super::super::iterator::BlockIterator;
    use super::super::range::{
        block_byte_offsets, block_count, decode_block_range, decode_single_block,
    };
    use super::*;
    use crate::fastlanes::bit_width_for_range;

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
