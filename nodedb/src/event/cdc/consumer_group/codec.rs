// SPDX-License-Identifier: BUSL-1.1

//! Durable encoding of consumer-group offsets: the redb key and the value.

use crate::event::cdc::offset::CdcOffset;
use crate::types::DatabaseId;

/// Byte length of a persisted offset: epoch, index, sequence.
const OFFSET_LEN: usize = 24;

/// Decode a persisted offset: epoch, index, then sequence, each a
/// little-endian u64.
pub(super) fn decode_offset(bytes: &[u8]) -> Option<CdcOffset> {
    let bytes: &[u8; OFFSET_LEN] = bytes.try_into().ok()?;
    let word = |at: usize| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[at..at + 8]);
        u64::from_le_bytes(buf)
    };
    Some(CdcOffset::at(word(0), word(8), word(16)))
}

pub(super) fn encode_offset(offset: CdcOffset) -> [u8; OFFSET_LEN] {
    let mut bytes = [0; OFFSET_LEN];
    bytes[..8].copy_from_slice(&offset.epoch.to_le_bytes());
    bytes[8..16].copy_from_slice(&offset.index.to_le_bytes());
    bytes[16..].copy_from_slice(&offset.sequence.to_le_bytes());
    bytes
}

/// Versioned, length-prefixed key encoding. The lengths make stream and group
/// names containing delimiters unambiguous.
pub(super) fn offset_key(
    database_id: DatabaseId,
    tenant_id: u64,
    stream: &str,
    group: &str,
    partition_id: u32,
) -> String {
    format!(
        "v2:{}:{tenant_id}:{}:{stream}:{}:{group}:{partition_id}",
        database_id.as_u64(),
        stream.len(),
        group.len()
    )
}

/// Decode a key written by [`offset_key`].
pub(super) fn parse_offset_key(key: &str) -> Option<(DatabaseId, u64, String, String, u32)> {
    let rest = key.strip_prefix("v2:")?;
    let (database_id, rest) = rest.split_once(':')?;
    let (tenant_id, rest) = rest.split_once(':')?;
    let (stream_len, rest) = rest.split_once(':')?;
    let stream_len: usize = stream_len.parse().ok()?;
    let stream = rest.get(..stream_len)?.to_string();
    let rest = rest.get(stream_len..)?.strip_prefix(':')?;
    let (group_len, rest) = rest.split_once(':')?;
    let group_len: usize = group_len.parse().ok()?;
    let group = rest.get(..group_len)?.to_string();
    let partition = rest.get(group_len..)?.strip_prefix(':')?.parse().ok()?;
    Some((
        DatabaseId::new(database_id.parse().ok()?),
        tenant_id.parse().ok()?,
        stream,
        group,
        partition,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_names_containing_delimiters() {
        let key = offset_key(DatabaseId::new(3), 9, "a:b", "g:1", 42);
        assert_eq!(
            parse_offset_key(&key),
            Some((
                DatabaseId::new(3),
                9,
                "a:b".to_string(),
                "g:1".to_string(),
                42
            ))
        );
    }

    #[test]
    fn values_round_trip_and_reject_other_lengths() {
        let offset = CdcOffset::at(2, 77, 6);
        assert_eq!(decode_offset(&encode_offset(offset)), Some(offset));
        assert_eq!(decode_offset(&[0u8; 16]), None);
    }
}
