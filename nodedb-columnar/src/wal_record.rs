// SPDX-License-Identifier: Apache-2.0

//! WAL record types for columnar operations.
//!
//! Each mutation (INSERT, DELETE, memtable flush) produces a WAL record
//! that is written before the mutation is applied. On crash recovery, WAL
//! records are replayed to reconstruct the memtable, delete bitmaps, and
//! segment metadata.
//!
//! Records are serialized as MessagePack for compact wire representation.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;
use serde::{Deserialize, Serialize};
use sonic_rs;
use zerompk::{FromMessagePack, ToMessagePack};

/// A WAL record for a columnar collection operation.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ColumnarWalRecord {
    /// A row was inserted into the memtable.
    ///
    /// Contains the collection name and the row data as packed binary
    /// (the columnar wire format, not MessagePack). On replay, the row
    /// is re-inserted into the memtable.
    #[serde(rename = "insert_row")]
    InsertRow {
        collection: String,
        /// Row data as packed binary values. Each value is encoded per its
        /// column type: i64 as 8 LE bytes, f64 as 8 LE bytes, strings as
        /// length-prefixed UTF-8, etc.
        row_data: Vec<u8>,
    },

    /// Rows were marked as deleted in a segment's delete bitmap.
    ///
    /// On replay, these row indices are re-applied to the segment's
    /// delete bitmap.
    #[serde(rename = "delete_rows")]
    DeleteRows {
        collection: String,
        segment_id: u64,
        row_indices: Vec<u32>,
    },

    /// The memtable was flushed to a new segment.
    ///
    /// On replay, if the segment file exists, update metadata to include it.
    /// If it doesn't exist, the flush was interrupted; rows are already in
    /// the memtable via InsertRow records.
    #[serde(rename = "memtable_flushed")]
    MemtableFlushed {
        collection: String,
        segment_id: u64,
        row_count: u64,
    },
}

impl ColumnarWalRecord {
    /// Collection name this record belongs to.
    pub fn collection(&self) -> &str {
        match self {
            Self::InsertRow { collection, .. }
            | Self::DeleteRows { collection, .. }
            | Self::MemtableFlushed { collection, .. } => collection,
        }
    }

    /// Serialize the record to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::error::ColumnarError> {
        zerompk::to_msgpack_vec(self)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }

    /// Deserialize a record from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, crate::error::ColumnarError> {
        zerompk::from_msgpack(data)
            .map_err(|e| crate::error::ColumnarError::Serialization(e.to_string()))
    }
}

/// Encode a row of values into the columnar wire format for WAL records.
///
/// Each value is written as: [type_tag: u8][value_bytes].
/// This is more compact than MessagePack for typed columns and enables
/// direct replay into the memtable without schema interpretation overhead.
pub fn encode_row_for_wal(
    values: &[nodedb_types::value::Value],
) -> Result<Vec<u8>, crate::error::ColumnarError> {
    use nodedb_types::value::Value;

    let mut buf = Vec::with_capacity(values.len() * 10); // Rough estimate.

    for value in values {
        match value {
            Value::Null => buf.push(0),
            Value::Integer(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Bool(v) => {
                buf.push(3);
                buf.push(*v as u8);
            }
            Value::String(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Bytes(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::DateTime(dt) => {
                buf.push(6);
                buf.extend_from_slice(&dt.micros.to_le_bytes());
            }
            Value::Decimal(d) => {
                buf.push(7);
                buf.extend_from_slice(&d.serialize());
            }
            Value::Uuid(s) => {
                buf.push(8);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Array(arr) => {
                // Vectors stored as: tag(9) + count(u32) + f32 values.
                buf.push(9);
                buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                for v in arr {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            _ => {
                // Geometry and other complex types: serialize as JSON bytes.
                buf.push(10);
                let json = sonic_rs::to_vec(value).map_err(|e| {
                    crate::error::ColumnarError::Serialization(format!(
                        "failed to serialize value as JSON: {e}"
                    ))
                })?;
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(&json);
            }
        }
    }

    Ok(buf)
}

/// Maximum length for a variable-length field in a WAL record (256 MiB).
/// Prevents OOM from crafted/corrupt records with bogus length prefixes.
const MAX_FIELD_LEN: usize = 256 * 1024 * 1024;

/// Read exactly `n` bytes from `data` at `cursor`, advancing cursor.
/// Returns `Err` if not enough bytes remain.
fn read_slice<'a>(
    data: &'a [u8],
    cursor: &mut usize,
    n: usize,
    context: &str,
) -> Result<&'a [u8], crate::error::ColumnarError> {
    let end = cursor.checked_add(n).ok_or_else(|| {
        crate::error::ColumnarError::Serialization(format!("overflow in {context}"))
    })?;
    if end > data.len() {
        return Err(crate::error::ColumnarError::Serialization(format!(
            "truncated {context}: need {n} bytes at offset {cursor}, have {}",
            data.len().saturating_sub(*cursor)
        )));
    }
    let slice = &data[*cursor..end];
    *cursor = end;
    Ok(slice)
}

/// Read a u32 length prefix, validate it against MAX_FIELD_LEN, then read
/// that many bytes. Returns the payload slice.
fn read_length_prefixed<'a>(
    data: &'a [u8],
    cursor: &mut usize,
    context: &str,
) -> Result<&'a [u8], crate::error::ColumnarError> {
    let len_bytes = read_slice(data, cursor, 4, context)?;
    let len = u32::from_le_bytes(len_bytes.try_into().map_err(|_| {
        crate::error::ColumnarError::Serialization(format!("truncated {context} len"))
    })?) as usize;
    if len > MAX_FIELD_LEN {
        return Err(crate::error::ColumnarError::Serialization(format!(
            "{context} length {len} exceeds maximum {MAX_FIELD_LEN}"
        )));
    }
    read_slice(data, cursor, len, context)
}

/// Decode a row from the columnar wire format back into Values.
pub fn decode_row_from_wal(
    data: &[u8],
) -> Result<Vec<nodedb_types::value::Value>, crate::error::ColumnarError> {
    use nodedb_types::value::Value;

    let mut values = Vec::new();
    let mut cursor = 0;

    while cursor < data.len() {
        let tag_slice = read_slice(data, &mut cursor, 1, "tag")?;
        let tag = tag_slice[0];

        let value = match tag {
            0 => Value::Null,
            1 => {
                let bytes = read_slice(data, &mut cursor, 8, "i64")?;
                let v = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated i64".into())
                })?);
                Value::Integer(v)
            }
            2 => {
                let bytes = read_slice(data, &mut cursor, 8, "f64")?;
                let v = f64::from_le_bytes(bytes.try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated f64".into())
                })?);
                Value::Float(v)
            }
            3 => {
                let bytes = read_slice(data, &mut cursor, 1, "bool")?;
                Value::Bool(bytes[0] != 0)
            }
            4 | 5 | 8 => {
                let bytes = read_length_prefixed(
                    data,
                    &mut cursor,
                    match tag {
                        4 => "string",
                        5 => "bytes",
                        8 => "uuid",
                        _ => unreachable!(),
                    },
                )?;
                match tag {
                    4 => Value::String(String::from_utf8_lossy(bytes).into_owned()),
                    5 => Value::Bytes(bytes.to_vec()),
                    8 => Value::Uuid(String::from_utf8_lossy(bytes).into_owned()),
                    _ => unreachable!(),
                }
            }
            6 => {
                let bytes = read_slice(data, &mut cursor, 8, "timestamp")?;
                let micros = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated timestamp".into())
                })?);
                Value::DateTime(nodedb_types::datetime::NdbDateTime::from_micros(micros))
            }
            7 => {
                let bytes = read_slice(data, &mut cursor, 16, "decimal")?;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Value::Decimal(rust_decimal::Decimal::deserialize(arr))
            }
            9 => {
                let count_bytes = read_slice(data, &mut cursor, 4, "vector count")?;
                let count = u32::from_le_bytes(count_bytes.try_into().map_err(|_| {
                    crate::error::ColumnarError::Serialization("truncated vector count".into())
                })?) as usize;
                let remaining_values = data.len().saturating_sub(cursor) / 4;
                let max_count = (MAX_FIELD_LEN / 4).min(remaining_values);
                if count > max_count {
                    return Err(crate::error::ColumnarError::Serialization(format!(
                        "vector count {count} exceeds maximum {max_count}"
                    )));
                }
                let capacity = checked_decode_capacity(
                    count,
                    size_of::<nodedb_types::value::Value>(),
                    data.len().saturating_sub(cursor),
                    4,
                    max_count,
                    usize::MAX,
                )
                .ok_or_else(|| {
                    crate::error::ColumnarError::Serialization(
                        "vector count exceeds decode allocation bounds".into(),
                    )
                })?;
                let mut arr = Vec::with_capacity(capacity);
                for _ in 0..count {
                    let fb = read_slice(data, &mut cursor, 4, "vector f32")?;
                    let f = f32::from_le_bytes(fb.try_into().map_err(|_| {
                        crate::error::ColumnarError::Serialization("truncated f32".into())
                    })?);
                    arr.push(Value::Float(f as f64));
                }
                Value::Array(arr)
            }
            10 => {
                let json_bytes = read_length_prefixed(data, &mut cursor, "json")?;
                sonic_rs::from_slice(json_bytes).unwrap_or(Value::Null)
            }
            _ => {
                return Err(crate::error::ColumnarError::Serialization(format!(
                    "unknown WAL value tag: {tag}"
                )));
            }
        };

        values.push(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use nodedb_types::datetime::NdbDateTime;
    use nodedb_types::value::Value;

    use super::*;

    #[test]
    fn rejects_huge_vector_count_with_tiny_payload_before_allocation() {
        // tag + one value whose vector count claims nearly u32::MAX values.
        let mut bytes = vec![9];
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_row_from_wal(&bytes),
            Err(crate::error::ColumnarError::Serialization(_))
        ));
    }

    #[test]
    fn wal_record_roundtrip() {
        let records = vec![
            ColumnarWalRecord::InsertRow {
                collection: "test".into(),
                row_data: vec![1, 2, 3],
            },
            ColumnarWalRecord::DeleteRows {
                collection: "test".into(),
                segment_id: 0,
                row_indices: vec![5, 10, 15],
            },
            ColumnarWalRecord::MemtableFlushed {
                collection: "test".into(),
                segment_id: 3,
                row_count: 1024,
            },
        ];

        for record in &records {
            let bytes = record.to_bytes().expect("serialize");
            let restored = ColumnarWalRecord::from_bytes(&bytes).expect("deserialize");
            assert_eq!(restored.collection(), record.collection());
        }
    }

    #[test]
    fn row_wire_format_roundtrip() {
        let values = vec![
            Value::Integer(42),
            Value::Float(0.75),
            Value::Bool(true),
            Value::String("hello".into()),
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000)),
            Value::Decimal(rust_decimal::Decimal::new(314, 2)),
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::Null,
            Value::Array(vec![Value::Float(1.0), Value::Float(2.0)]),
        ];

        let encoded = encode_row_for_wal(&values).expect("encode");
        let decoded = decode_row_from_wal(&encoded).expect("decode");

        assert_eq!(decoded.len(), values.len());
        assert_eq!(decoded[0], Value::Integer(42));
        assert_eq!(decoded[1], Value::Float(0.75));
        assert_eq!(decoded[2], Value::Bool(true));
        assert_eq!(decoded[3], Value::String("hello".into()));
        assert_eq!(decoded[4], Value::Bytes(vec![0xDE, 0xAD]));
        assert_eq!(
            decoded[5],
            Value::DateTime(NdbDateTime::from_micros(1_700_000_000))
        );
        assert_eq!(
            decoded[7],
            Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into())
        );
        assert_eq!(decoded[8], Value::Null);
    }

    #[test]
    fn decode_truncated_i64_returns_error() {
        // Tag 1 (i64) requires 8 payload bytes; supply none.
        // Today the slice index `data[cursor..cursor+8]` panics with an index
        // out-of-bounds. After the fix, `try_into()` returns the
        // Serialization error instead.
        let result = decode_row_from_wal(&[1]);
        assert!(
            result.is_err(),
            "truncated i64 payload must return Err, not panic"
        );
    }

    #[test]
    fn decode_truncated_string_returns_error() {
        // Tag 4 (string): length prefix says 255 bytes but the slice ends
        // immediately after the 4-byte length field. The read of
        // `data[cursor..cursor+255]` panics today; after the fix it errors.
        let input = {
            let mut v = vec![4u8]; // tag = string
            v.extend_from_slice(&255u32.to_le_bytes()); // len = 255
            // no payload bytes follow
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "truncated string payload must return Err, not panic"
        );
    }

    #[test]
    fn decode_huge_vector_count_returns_error() {
        // Tag 9 (vector array): count = 0x7FFFFFFF. After reading the count,
        // the very first iteration tries to read 4 bytes of f32 from an empty
        // slice, which panics today. After the fix the loop errors out cleanly
        // before any allocation proportional to count is attempted.
        let input = {
            let mut v = vec![9u8]; // tag = vector array
            v.extend_from_slice(&0x7FFF_FFFFu32.to_le_bytes()); // count
            // no f32 bytes follow
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "huge vector count with no payload must return Err, not panic or OOM"
        );
    }

    #[test]
    fn decode_truncated_decimal_returns_error() {
        // Tag 7 (Decimal) requires 16 bytes; supply only 4.
        // `data[cursor..cursor+16]` panics today; after the fix it errors.
        let input = {
            let mut v = vec![7u8]; // tag = decimal
            v.extend_from_slice(&[0u8; 4]); // only 4 bytes, need 16
            v
        };
        let result = decode_row_from_wal(&input);
        assert!(
            result.is_err(),
            "truncated decimal payload must return Err, not panic"
        );
    }
}
