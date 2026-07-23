// SPDX-License-Identifier: Apache-2.0

//! Block-level encode and decode for the FastLanes FOR + bit-packing codec.

use super::BLOCK_SIZE;
use super::bits::{low_mask_u8, pack_bits, unpack_bits};
use crate::bounds::{checked_add, checked_mul, checked_range, encode_u32_len};
use crate::error::CodecError;

/// Per-block header: 2 bytes count + 1 byte bit_width + 8 bytes min_value.
pub(super) const BLOCK_HEADER_SIZE: usize = 11;

/// Encode a single nonempty block (up to 1024 values).
pub(super) fn encode_block(values: &[i64], out: &mut Vec<u8>) -> Result<(), CodecError> {
    if values.is_empty() || values.len() > BLOCK_SIZE {
        return Err(CodecError::Corrupt {
            detail: "FastLanes encoder received an invalid block size".into(),
        });
    }
    let count_u16 = u16::try_from(values.len()).map_err(|_| CodecError::Corrupt {
        detail: "FastLanes block count does not fit u16".into(),
    })?;

    let mut min_val = values[0];
    let mut max_val = values[0];
    for &value in &values[1..] {
        min_val = min_val.min(value);
        max_val = max_val.max(value);
    }

    let range = (max_val as u128).wrapping_sub(min_val as u128) as u64;
    let bit_width = if range == 0 {
        0
    } else {
        64 - range.leading_zeros() as u8
    };
    let count = usize::from(count_u16);
    let packed_bits = checked_mul(count, usize::from(bit_width), "FastLanes packed bits")?;
    let packed_bytes = packed_bits.div_ceil(8);
    let header_end = checked_add(out.len(), BLOCK_HEADER_SIZE, "FastLanes block header")?;
    let output_end = checked_add(header_end, packed_bytes, "FastLanes block output")?;
    encode_u32_len(output_end, "FastLanes encoded output")?;

    out.extend_from_slice(&count_u16.to_le_bytes());
    out.push(bit_width);
    out.extend_from_slice(&min_val.to_le_bytes());
    if bit_width == 0 {
        return Ok(());
    }

    out.resize(output_end, 0);
    let packed = &mut out[header_end..output_end];
    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut bit_offset = 0usize;
    for &value in values {
        let residual = (value.wrapping_sub(min_val) as u64) & mask;
        pack_bits(packed, bit_offset, residual, bit_width);
        bit_offset = checked_add(bit_offset, usize::from(bit_width), "FastLanes bit offset")?;
    }
    Ok(())
}

/// Decode a single block from the byte stream and return the next offset.
pub(super) fn decode_block(
    data: &[u8],
    offset: usize,
    values: &mut Vec<i64>,
    block_idx: usize,
    expected_count: usize,
) -> Result<usize, CodecError> {
    let header = checked_range(data, offset, BLOCK_HEADER_SIZE, "FastLanes block header")?;
    let count = usize::from(u16::from_le_bytes([header[0], header[1]]));
    let bit_width = header[2];
    let min_val = i64::from_le_bytes([
        header[3], header[4], header[5], header[6], header[7], header[8], header[9], header[10],
    ]);
    validate_block_shape(count, bit_width, block_idx, expected_count)?;
    let packed_bytes = packed_byte_len(count, bit_width)?;
    let packed_start = checked_add(offset, BLOCK_HEADER_SIZE, "FastLanes packed start")?;
    let packed = checked_range(data, packed_start, packed_bytes, "FastLanes packed data")?;
    validate_padding(packed, count, bit_width, block_idx)?;
    let next_offset = checked_add(packed_start, packed_bytes, "FastLanes block end")?;

    if bit_width == 0 {
        values.extend(std::iter::repeat_n(min_val, count));
        return Ok(next_offset);
    }

    let mask = if bit_width == 64 {
        u64::MAX
    } else {
        (1u64 << bit_width) - 1
    };
    let mut bit_offset = 0usize;
    for _ in 0..count {
        let residual = unpack_bits(packed, bit_offset, bit_width) & mask;
        values.push(min_val.wrapping_add(residual as i64));
        bit_offset = checked_add(bit_offset, usize::from(bit_width), "FastLanes bit offset")?;
    }
    Ok(next_offset)
}

/// Skip a block without decoding, returning the next byte offset.
pub(super) fn skip_block(
    data: &[u8],
    offset: usize,
    block_idx: usize,
    expected_count: usize,
) -> Result<usize, CodecError> {
    let header = checked_range(data, offset, BLOCK_HEADER_SIZE, "FastLanes block header")?;
    let count = usize::from(u16::from_le_bytes([header[0], header[1]]));
    let bit_width = header[2];
    validate_block_shape(count, bit_width, block_idx, expected_count)?;
    let packed_bytes = packed_byte_len(count, bit_width)?;
    let packed_start = checked_add(offset, BLOCK_HEADER_SIZE, "FastLanes packed start")?;
    let packed = checked_range(data, packed_start, packed_bytes, "FastLanes packed data")?;
    validate_padding(packed, count, bit_width, block_idx)?;
    checked_add(packed_start, packed_bytes, "FastLanes block end")
}

fn validate_block_shape(
    count: usize,
    bit_width: u8,
    block_idx: usize,
    expected_count: usize,
) -> Result<(), CodecError> {
    if count != expected_count || count == 0 || count > BLOCK_SIZE {
        return Err(CodecError::Corrupt {
            detail: format!(
                "block {block_idx}: count {count} does not match expected {expected_count}"
            ),
        });
    }
    if bit_width > 64 {
        return Err(CodecError::Corrupt {
            detail: format!("block {block_idx}: invalid bit_width {bit_width}"),
        });
    }
    Ok(())
}

fn packed_byte_len(count: usize, bit_width: u8) -> Result<usize, CodecError> {
    checked_mul(count, usize::from(bit_width), "FastLanes packed bits").map(|bits| bits.div_ceil(8))
}

fn validate_padding(
    packed: &[u8],
    count: usize,
    bit_width: u8,
    block_idx: usize,
) -> Result<(), CodecError> {
    let meaningful_bits = checked_mul(count, usize::from(bit_width), "FastLanes packed bits")?;
    let final_bits = meaningful_bits % 8;
    if final_bits != 0
        && packed
            .last()
            .is_some_and(|byte| byte & !low_mask_u8(final_bits) != 0)
    {
        return Err(CodecError::Corrupt {
            detail: format!("block {block_idx}: non-zero FastLanes padding bits"),
        });
    }
    Ok(())
}

/// Compute the minimum number of bits needed to represent the range of values.
pub fn bit_width_for_range(min: i64, max: i64) -> u8 {
    let range = (max as u128).wrapping_sub(min as u128) as u64;
    if range == 0 {
        0
    } else {
        64 - range.leading_zeros() as u8
    }
}
