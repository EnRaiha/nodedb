// SPDX-License-Identifier: Apache-2.0

//! Random-access and range operations over an encoded FastLanes frame.

use std::mem::size_of;

use crate::bounds::{checked_add, checked_capacity, checked_mul, decoded_len};
use crate::error::CodecError;

use super::block::{decode_block, skip_block};
use super::header::{GLOBAL_HEADER_SIZE, expected_block_count, parse_header, validate_frame};

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
/// [`super::iterator::BlockIterator`] which tracks byte offsets without
/// re-scanning.
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
