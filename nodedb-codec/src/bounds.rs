// SPDX-License-Identifier: Apache-2.0

//! Shared hard limits and checked framing helpers for codec decoders.

use crate::error::CodecError;

/// Maximum decoded payload accepted from one codec frame (64 MiB).
///
/// This is deliberately the same ceiling as a WAL payload: it permits normal
/// column blocks while preventing a small hostile frame from reserving process
/// memory proportional to an attacker-controlled u32 length.
pub(crate) const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

/// Maximum declared expansion relative to compressed bytes for Zstd frames.
/// Zstd frames may legitimately compress repeated telemetry strongly; 4096:1
/// permits that workload while rejecting extreme decompression bombs.
pub(crate) const MAX_DECOMPRESSION_RATIO: usize = 4096;

pub(crate) fn u32_to_usize(value: u32, context: &str) -> Result<usize, CodecError> {
    usize::try_from(value).map_err(|_| CodecError::Corrupt {
        detail: format!("{context} does not fit platform usize"),
    })
}

pub(crate) fn checked_add(left: usize, right: usize, context: &str) -> Result<usize, CodecError> {
    left.checked_add(right).ok_or_else(|| CodecError::Corrupt {
        detail: format!("{context} overflows usize"),
    })
}

pub(crate) fn checked_mul(left: usize, right: usize, context: &str) -> Result<usize, CodecError> {
    left.checked_mul(right).ok_or_else(|| CodecError::Corrupt {
        detail: format!("{context} overflows usize"),
    })
}

pub(crate) fn checked_range<'a>(
    data: &'a [u8],
    start: usize,
    len: usize,
    context: &str,
) -> Result<&'a [u8], CodecError> {
    let end = checked_add(start, len, context)?;
    data.get(start..end).ok_or(CodecError::Truncated {
        expected: end,
        actual: data.len(),
    })
}

pub(crate) fn decoded_len(value: usize, codec: &str) -> Result<usize, CodecError> {
    if value > MAX_DECODED_BYTES {
        return Err(CodecError::ResourceLimit {
            resource: format!("{codec} decoded bytes"),
            requested: value,
            limit: MAX_DECODED_BYTES,
        });
    }
    Ok(value)
}

pub(crate) fn encode_u32_len(value: usize, context: &str) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::ResourceLimit {
        resource: format!("{context} encoded length"),
        requested: value,
        limit: u32::MAX as usize,
    })
}

pub(crate) fn encode_input_len(value: usize, codec: &str) -> Result<u32, CodecError> {
    decoded_len(value, codec)?;
    encode_u32_len(value, codec)
}
