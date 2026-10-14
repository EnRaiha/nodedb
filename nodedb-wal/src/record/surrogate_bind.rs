// SPDX-License-Identifier: Apache-2.0

//! Surrogate ↔ PK binding batch payload.
//!
//! One record carries the bindings one assigner call wrote for one
//! collection. The `(database_id, tenant_id)` scope rides the record header.
//! A batch too large for one record splits into several records. Each record
//! fits one double-write slot, so every bind record keeps torn-write
//! protection and stays under the writer's payload limit.
//!
//! Wire format (little-endian, no serde dep on `nodedb-wal`):
//!
//! ```text
//! ┌──────────────┬────────────┬───────────┬───────────────┐
//! │ name_len u32 │ name bytes │ count u32 │ count × entry │
//! └──────────────┴────────────┴───────────┴───────────────┘
//!
//! entry:
//! ┌───────────────┬────────────┬──────────┐
//! │ surrogate u32 │ pk_len u32 │ pk bytes │
//! └───────────────┴────────────┴──────────┘
//! ```

use crate::crypto::AUTH_TAG_SIZE;
use crate::double_write::slot_record_max;
use crate::error::{Result, WalError};
use crate::record::HEADER_SIZE;

const U32_SIZE: usize = 4;

/// Bytes of one entry ahead of its pk bytes.
const ENTRY_FIXED_SIZE: usize = 2 * U32_SIZE;

/// Largest bind payload whose record fits one double-write slot, with room
/// for the encryption tag.
pub const SURROGATE_BIND_PAYLOAD_MAX: usize = slot_record_max() - HEADER_SIZE - AUTH_TAG_SIZE;

/// The bindings of one collection carried by one bind record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurrogateBindBatchPayload {
    /// Bare catalog name of the bound collection.
    pub collection: String,
    /// `(surrogate, pk_bytes)` pairs, in bind order.
    pub entries: Vec<(u32, Vec<u8>)>,
}

impl SurrogateBindBatchPayload {
    pub fn new(collection: impl Into<String>, entries: Vec<(u32, Vec<u8>)>) -> Self {
        Self {
            collection: collection.into(),
            entries,
        }
    }

    /// Encode this batch as one payload, whatever its size.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let entries: Vec<(u32, &[u8])> = self
            .entries
            .iter()
            .map(|(surrogate, pk)| (*surrogate, pk.as_slice()))
            .collect();
        encode_one(self.collection.as_bytes(), &entries)
    }

    /// Encode `entries` as payloads of at most `max_payload` bytes each, in
    /// order. An entry too large to share a payload is encoded alone, and
    /// that payload exceeds `max_payload`. An empty `entries` yields no
    /// payload.
    pub fn encode_chunks(
        collection: &str,
        entries: &[(u32, &[u8])],
        max_payload: usize,
    ) -> Result<Vec<Vec<u8>>> {
        let name = collection.as_bytes();
        let prefix_size = U32_SIZE + name.len() + U32_SIZE;
        let mut payloads = Vec::new();
        let mut start = 0;
        let mut size = prefix_size;
        for (idx, &(_, pk)) in entries.iter().enumerate() {
            let entry_size = ENTRY_FIXED_SIZE + pk.len();
            if idx > start && size + entry_size > max_payload {
                payloads.push(encode_one(name, &entries[start..idx])?);
                start = idx;
                size = prefix_size;
            }
            size += entry_size;
        }
        if start < entries.len() {
            payloads.push(encode_one(name, &entries[start..])?);
        }
        Ok(payloads)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { buf, off: 0 };
        let name_len = cursor.read_u32()? as usize;
        let name = cursor.take(name_len)?;
        let collection = std::str::from_utf8(name)
            .map_err(|e| invalid(format!("SurrogateBindBatch collection not utf8: {e}")))?
            .to_string();
        let count = cursor.read_u32()? as usize;
        // Every entry takes at least `ENTRY_FIXED_SIZE` encoded bytes, so the
        // rest of the buffer bounds the count. A larger count is corrupt.
        let remaining = cursor.remaining();
        let Some(capacity) = nodedb_types::decode_bounds::checked_decode_capacity(
            count,
            std::mem::size_of::<(u32, Vec<u8>)>(),
            remaining,
            ENTRY_FIXED_SIZE,
            remaining / ENTRY_FIXED_SIZE,
            remaining.saturating_mul(std::mem::size_of::<(u32, Vec<u8>)>()),
        ) else {
            return Err(invalid(format!(
                "SurrogateBindBatch count {count} exceeds the {remaining} bytes left"
            )));
        };
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..count {
            let surrogate = cursor.read_u32()?;
            let pk_len = cursor.read_u32()? as usize;
            let pk = cursor.take(pk_len)?.to_vec();
            entries.push((surrogate, pk));
        }
        if cursor.remaining() != 0 {
            return Err(invalid(format!(
                "SurrogateBindBatch trailing bytes: parsed {}, buffer {}",
                cursor.off,
                buf.len()
            )));
        }
        Ok(Self {
            collection,
            entries,
        })
    }
}

fn encode_one(name: &[u8], entries: &[(u32, &[u8])]) -> Result<Vec<u8>> {
    let size = U32_SIZE
        + name.len()
        + U32_SIZE
        + entries
            .iter()
            .map(|(_, pk)| ENTRY_FIXED_SIZE + pk.len())
            .sum::<usize>();
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(&len_u32(name.len(), "collection name")?.to_le_bytes());
    buf.extend_from_slice(name);
    buf.extend_from_slice(&len_u32(entries.len(), "entry count")?.to_le_bytes());
    for &(surrogate, pk) in entries {
        buf.extend_from_slice(&surrogate.to_le_bytes());
        buf.extend_from_slice(&len_u32(pk.len(), "pk")?.to_le_bytes());
        buf.extend_from_slice(pk);
    }
    Ok(buf)
}

fn len_u32(len: usize, what: &str) -> Result<u32> {
    u32::try_from(len).map_err(|_| invalid(format!("SurrogateBindBatch {what} too long: {len}")))
}

fn invalid(detail: String) -> WalError {
    WalError::InvalidPayload { detail }
}

struct Cursor<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(invalid(format!(
                "SurrogateBindBatch truncated: need {n} bytes at offset {}, have {}",
                self.off,
                self.buf.len()
            )));
        }
        let slice = &self.buf[self.off..self.off + n];
        self.off += n;
        Ok(slice)
    }

    fn read_u32(&mut self) -> Result<u32> {
        let b = self.take(U32_SIZE)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owned(entries: &[(u32, &[u8])]) -> Vec<(u32, Vec<u8>)> {
        entries.iter().map(|&(s, pk)| (s, pk.to_vec())).collect()
    }

    #[test]
    fn batch_roundtrip() {
        let p = SurrogateBindBatchPayload::new(
            "users",
            vec![(42, b"alice".to_vec()), (43, b"bob".to_vec())],
        );
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SurrogateBindBatchPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn single_entry_and_binary_pk_roundtrip() {
        let pk: Vec<u8> = (0..=255u8).collect();
        let p = SurrogateBindBatchPayload::new("binary", vec![(u32::MAX, pk)]);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SurrogateBindBatchPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn empty_pk_roundtrip() {
        let p = SurrogateBindBatchPayload::new("c", vec![(1, Vec::new())]);
        let bytes = p.to_bytes().unwrap();
        assert_eq!(SurrogateBindBatchPayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn empty_batch_encodes_no_payload() {
        let chunks =
            SurrogateBindBatchPayload::encode_chunks("users", &[], SURROGATE_BIND_PAYLOAD_MAX)
                .unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn small_batch_is_one_payload() {
        let entries: Vec<(u32, &[u8])> = vec![
            (1, b"a".as_slice()),
            (2, b"b".as_slice()),
            (3, b"c".as_slice()),
        ];
        let chunks =
            SurrogateBindBatchPayload::encode_chunks("users", &entries, SURROGATE_BIND_PAYLOAD_MAX)
                .unwrap();
        assert_eq!(chunks.len(), 1);
        let decoded = SurrogateBindBatchPayload::from_bytes(&chunks[0]).unwrap();
        assert_eq!(decoded.collection, "users");
        assert_eq!(decoded.entries, owned(&entries));
    }

    #[test]
    fn large_batch_splits_into_slot_sized_payloads_in_order() {
        let pks: Vec<Vec<u8>> = (0..20_000u32)
            .map(|i| format!("pk-{i:010}").into_bytes())
            .collect();
        let entries: Vec<(u32, &[u8])> = pks
            .iter()
            .enumerate()
            .map(|(i, pk)| (i as u32 + 1, pk.as_slice()))
            .collect();
        let chunks =
            SurrogateBindBatchPayload::encode_chunks("users", &entries, SURROGATE_BIND_PAYLOAD_MAX)
                .unwrap();
        assert!(chunks.len() > 1, "a 20k-entry batch must split");
        assert!(
            chunks.len() < 20,
            "a split keeps many entries per payload, got {} payloads",
            chunks.len()
        );
        let mut decoded = Vec::new();
        for chunk in &chunks {
            assert!(chunk.len() <= SURROGATE_BIND_PAYLOAD_MAX);
            let payload = SurrogateBindBatchPayload::from_bytes(chunk).unwrap();
            assert_eq!(payload.collection, "users");
            assert!(!payload.entries.is_empty());
            decoded.extend(payload.entries);
        }
        assert_eq!(decoded, owned(&entries));
    }

    #[test]
    fn oversized_entry_is_encoded_alone() {
        let big = vec![7u8; 256];
        let entries: Vec<(u32, &[u8])> = vec![
            (1, b"a".as_slice()),
            (2, big.as_slice()),
            (3, b"c".as_slice()),
        ];
        let chunks = SurrogateBindBatchPayload::encode_chunks("t", &entries, 64).unwrap();
        assert_eq!(chunks.len(), 3);
        let middle = SurrogateBindBatchPayload::from_bytes(&chunks[1]).unwrap();
        assert_eq!(middle.entries, vec![(2, big.clone())]);
    }

    #[test]
    fn rejects_garbage_truncation_and_trailing_bytes() {
        assert!(SurrogateBindBatchPayload::from_bytes(&[0xFFu8; 7]).is_err());
        let bytes = SurrogateBindBatchPayload::new("users", vec![(9, b"x".to_vec())])
            .to_bytes()
            .unwrap();
        assert!(SurrogateBindBatchPayload::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(SurrogateBindBatchPayload::from_bytes(&trailing).is_err());
    }

    #[test]
    fn corrupt_count_does_not_overallocate() {
        let mut bytes = SurrogateBindBatchPayload::new("u", Vec::new())
            .to_bytes()
            .unwrap();
        let count_at = bytes.len() - U32_SIZE;
        bytes[count_at..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(SurrogateBindBatchPayload::from_bytes(&bytes).is_err());
    }
}
