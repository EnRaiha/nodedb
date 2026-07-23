// SPDX-License-Identifier: BUSL-1.1

//! Bounded MessagePack decoding for bytes that crossed a trust boundary.

use std::fmt;
use std::io::Cursor;

/// Largest accepted MessagePack document.
pub const MAX_MSGPACK_BYTES: usize = 64 * 1024 * 1024;
/// Maximum permitted nesting depth.
pub const MAX_MSGPACK_DEPTH: usize = 64;
/// Maximum number of decoded values, including container entries.
pub const MAX_MSGPACK_VALUES: usize = 1_000_000;

// rmpv decrements its recursion budget once for each value and again for each
// container/string/bin/ext body. Preflight owns the public structural limit;
// this larger internal budget prevents rmpv from imposing a lower one.
const RMPV_INTERNAL_MAX_DEPTH: usize = MAX_MSGPACK_DEPTH * 2 + 3;

/// Failure while validating or decoding an untrusted MessagePack document.
#[derive(Debug)]
pub struct BoundedMsgpackError {
    detail: &'static str,
}

impl BoundedMsgpackError {
    fn new(detail: &'static str) -> Self {
        Self { detail }
    }
}

impl fmt::Display for BoundedMsgpackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "bounded MessagePack decode failed: {}",
            self.detail
        )
    }
}

impl std::error::Error for BoundedMsgpackError {}

/// Validate and decode exactly one bounded MessagePack value.
pub fn read_value(bytes: &[u8]) -> Result<rmpv::Value, BoundedMsgpackError> {
    if bytes.len() > MAX_MSGPACK_BYTES {
        return Err(BoundedMsgpackError::new("input exceeds byte limit"));
    }

    let mut cursor = 0;
    let mut values = 0;
    preflight_value(bytes, &mut cursor, 0, &mut values)?;
    if cursor != bytes.len() {
        return Err(BoundedMsgpackError::new("trailing bytes after value"));
    }

    let mut reader = Cursor::new(bytes);
    let value = rmpv::decode::read_value_with_max_depth(&mut reader, RMPV_INTERNAL_MAX_DEPTH)
        .map_err(|_| BoundedMsgpackError::new("invalid MessagePack value"))?;
    if reader.position() != bytes.len() as u64 {
        return Err(BoundedMsgpackError::new("trailing bytes after value"));
    }
    Ok(value)
}

fn preflight_value(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    values: &mut usize,
) -> Result<(), BoundedMsgpackError> {
    if depth > MAX_MSGPACK_DEPTH {
        return Err(BoundedMsgpackError::new("nesting exceeds depth limit"));
    }
    *values = values
        .checked_add(1)
        .ok_or_else(|| BoundedMsgpackError::new("value count overflow"))?;
    if *values > MAX_MSGPACK_VALUES {
        return Err(BoundedMsgpackError::new("value count exceeds limit"));
    }

    let marker = take_u8(bytes, cursor)?;
    match marker {
        0x00..=0x7f
        | 0x80..=0x8f
        | 0x90..=0x9f
        | 0xa0..=0xbf
        | 0xc0
        | 0xc2
        | 0xc3
        | 0xe0..=0xff => match marker {
            0x80..=0x8f => {
                preflight_container(bytes, cursor, depth, values, usize::from(marker & 0x0f) * 2)
            }
            0x90..=0x9f => {
                preflight_container(bytes, cursor, depth, values, usize::from(marker & 0x0f))
            }
            0xa0..=0xbf => take(bytes, cursor, usize::from(marker & 0x1f)).map(|_| ()),
            _ => Ok(()),
        },
        0xc1 => Err(BoundedMsgpackError::new("reserved MessagePack marker")),
        0xc4 => take_sized(bytes, cursor, 1, 0),
        0xc5 => take_sized(bytes, cursor, 2, 0),
        0xc6 => take_sized(bytes, cursor, 4, 0),
        0xc7 => take_sized(bytes, cursor, 1, 1),
        0xc8 => take_sized(bytes, cursor, 2, 1),
        0xc9 => take_sized(bytes, cursor, 4, 1),
        0xca | 0xce | 0xd2 => take(bytes, cursor, 4).map(|_| ()),
        0xcb | 0xcf | 0xd3 => take(bytes, cursor, 8).map(|_| ()),
        0xcc | 0xd0 => take(bytes, cursor, 1).map(|_| ()),
        0xcd | 0xd1 => take(bytes, cursor, 2).map(|_| ()),
        0xd4 => take(bytes, cursor, 2).map(|_| ()),
        0xd5 => take(bytes, cursor, 3).map(|_| ()),
        0xd6 => take(bytes, cursor, 5).map(|_| ()),
        0xd7 => take(bytes, cursor, 9).map(|_| ()),
        0xd8 => take(bytes, cursor, 17).map(|_| ()),
        0xd9 => take_sized(bytes, cursor, 1, 0),
        0xda => take_sized(bytes, cursor, 2, 0),
        0xdb => take_sized(bytes, cursor, 4, 0),
        0xdc => preflight_sized_container(bytes, cursor, depth, values, 2, 1),
        0xdd => preflight_sized_container(bytes, cursor, depth, values, 4, 1),
        0xde => preflight_sized_container(bytes, cursor, depth, values, 2, 2),
        0xdf => preflight_sized_container(bytes, cursor, depth, values, 4, 2),
    }
}

fn preflight_sized_container(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    values: &mut usize,
    width: usize,
    multiplier: usize,
) -> Result<(), BoundedMsgpackError> {
    let entries = read_len(bytes, cursor, width)?
        .checked_mul(multiplier)
        .ok_or_else(|| BoundedMsgpackError::new("container entry count overflow"))?;
    preflight_container(bytes, cursor, depth, values, entries)
}

fn preflight_container(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
    values: &mut usize,
    entries: usize,
) -> Result<(), BoundedMsgpackError> {
    if depth >= MAX_MSGPACK_DEPTH {
        return Err(BoundedMsgpackError::new("nesting exceeds depth limit"));
    }
    if entries > MAX_MSGPACK_VALUES.saturating_sub(*values) {
        return Err(BoundedMsgpackError::new(
            "container entries exceed value limit",
        ));
    }
    for _ in 0..entries {
        preflight_value(bytes, cursor, depth + 1, values)?;
    }
    Ok(())
}

fn take_sized(
    bytes: &[u8],
    cursor: &mut usize,
    width: usize,
    extra: usize,
) -> Result<(), BoundedMsgpackError> {
    let length = read_len(bytes, cursor, width)?;
    let required = length
        .checked_add(extra)
        .ok_or_else(|| BoundedMsgpackError::new("declared length overflow"))?;
    take(bytes, cursor, required).map(|_| ())
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, BoundedMsgpackError> {
    Ok(take(bytes, cursor, 1)?[0])
}

fn read_len(bytes: &[u8], cursor: &mut usize, width: usize) -> Result<usize, BoundedMsgpackError> {
    let raw = take(bytes, cursor, width)?;
    let length = match width {
        1 => usize::from(raw[0]),
        2 => usize::from(u16::from_be_bytes([raw[0], raw[1]])),
        4 => usize::try_from(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
            .map_err(|_| BoundedMsgpackError::new("declared length does not fit usize"))?,
        _ => return Err(BoundedMsgpackError::new("invalid length width")),
    };
    Ok(length)
}

fn take<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], BoundedMsgpackError> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| BoundedMsgpackError::new("input offset overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| BoundedMsgpackError::new("truncated declared payload"))?;
    *cursor = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_nested_value_roundtrips() {
        let bytes = [
            0x82, 0xa1, b'a', 0x92, 1, 2, 0xa1, b'b', 0x81, 0xa1, b'c', 0xc3,
        ];
        assert!(matches!(read_value(&bytes), Ok(rmpv::Value::Map(_))));
    }

    #[test]
    fn maximum_nesting_is_accepted_for_nested_value_kinds() {
        for leaf in [vec![0xc0], vec![0xa1, b'x'], vec![0xd4, 1, 2]] {
            let mut bytes = vec![0x91; MAX_MSGPACK_DEPTH];
            bytes.extend_from_slice(&leaf);
            assert!(read_value(&bytes).is_ok());
        }
    }

    #[test]
    fn nesting_above_maximum_is_rejected() {
        let mut bytes = vec![0x91; MAX_MSGPACK_DEPTH + 1];
        bytes.push(0xc0);
        assert!(read_value(&bytes).is_err());
    }

    #[test]
    fn declared_lengths_cannot_allocate_from_tiny_input() {
        for bytes in [
            vec![0xdc, 0xff, 0xff],
            vec![0xdd, 0xff, 0xff, 0xff, 0xff],
            vec![0xde, 0xff, 0xff],
            vec![0xdf, 0xff, 0xff, 0xff, 0xff],
            vec![0xdb, 0xff, 0xff, 0xff, 0xff],
            vec![0xc6, 0xff, 0xff, 0xff, 0xff],
            vec![0xc9, 0xff, 0xff, 0xff, 0xff],
        ] {
            assert!(read_value(&bytes).is_err());
        }
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        assert!(read_value(&[0xc0, 0xc0]).is_err());
    }
}
