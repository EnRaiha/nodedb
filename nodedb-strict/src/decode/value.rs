// SPDX-License-Identifier: Apache-2.0

use nodedb_types::columnar::ColumnType;
use nodedb_types::datetime::NdbDateTime;
use nodedb_types::value::Value;

/// Decode a fixed-size raw byte slice into a value.
pub(super) fn decode_fixed_value(col_type: &ColumnType, raw: &[u8]) -> Value {
    match col_type {
        ColumnType::Int64 => Value::Integer(i64::from_le_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ])),
        ColumnType::Float64 => Value::Float(f64::from_le_bytes([
            raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
        ])),
        ColumnType::Bool => Value::Bool(raw[0] != 0),
        ColumnType::Timestamp => {
            let micros = i64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Value::NaiveDateTime(NdbDateTime::from_micros(micros))
        }
        ColumnType::Timestamptz => {
            let micros = i64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ]);
            Value::DateTime(NdbDateTime::from_micros(micros))
        }
        ColumnType::Decimal { .. } => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Decimal(rust_decimal::Decimal::deserialize(bytes))
        }
        ColumnType::Uuid => {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&raw[..16]);
            Value::Uuid(uuid::Uuid::from_bytes(bytes).to_string())
        }
        ColumnType::Vector(dim) => {
            let Ok(d) = usize::try_from(*dim) else {
                return Value::Null;
            };
            let mut floats = Vec::with_capacity(d);
            for chunk in raw.chunks_exact(4).take(d) {
                let bytes = [chunk[0], chunk[1], chunk[2], chunk[3]];
                floats.push(Value::Float(f32::from_le_bytes(bytes) as f64));
            }
            Value::Array(floats)
        }
        _ => Value::Null,
    }
}

/// Decode a variable-length raw byte slice into a value.
pub(super) fn decode_variable_value(col_type: &ColumnType, raw: &[u8]) -> Value {
    match col_type {
        ColumnType::String | ColumnType::SparseVector => {
            Value::String(std::str::from_utf8(raw).unwrap_or_default().to_string())
        }
        ColumnType::Bytes => Value::Bytes(raw.to_vec()),
        ColumnType::Geometry => {
            if let Ok(geometry) = sonic_rs::from_slice::<nodedb_types::geometry::Geometry>(raw) {
                Value::Geometry(geometry)
            } else {
                Value::String(std::str::from_utf8(raw).unwrap_or_default().to_string())
            }
        }
        ColumnType::Json => match nodedb_types::value_from_msgpack(raw) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(len = raw.len(), error = %error, "corrupted JSON msgpack in tuple");
                Value::Null
            }
        },
        _ => Value::Null,
    }
}
