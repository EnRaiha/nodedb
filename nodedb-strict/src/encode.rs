// SPDX-License-Identifier: Apache-2.0

//! Binary Tuple encoder: schema + values → compact byte representation.
//!
//! Layout:
//! ```text
//! [magic: u32 LE = 0x5453_444E "NDST"]   bytes 0..4
//! [format_version: u8 = 1]               byte 4
//! [schema_version: u32 LE]               bytes 5..9
//! [null_bitmap: ceil(N/8) bytes, bit=1 means NULL]
//! [fixed_fields: concatenated, zeroed when null]
//! [offset_table: (N_var + 1) × u32 LE]
//! [variable_data: concatenated variable-length bytes]
//! ```

/// Magic bytes identifying a Binary Tuple: `"NDST"` in little-endian.
pub const MAGIC: u32 = 0x5453_444E;

/// Current Binary Tuple format version.
pub const FORMAT_VERSION: u8 = 1;

use nodedb_types::columnar::{ColumnType, StrictSchema};
use nodedb_types::value::Value;

use crate::error::StrictError;

/// Encodes rows into Binary Tuples according to a fixed schema.
///
/// Reusable: create once per schema, encode many rows. Internal buffers
/// are reused across calls to minimize allocation.
pub struct TupleEncoder {
    schema: StrictSchema,
    /// Precomputed: byte offset of each fixed-size column within the fixed section.
    /// Variable-length columns get `None`.
    fixed_offsets: Vec<Option<usize>>,
    /// Total size of the fixed-fields section.
    fixed_section_size: usize,
    /// Indices of variable-length columns in schema order.
    var_indices: Vec<usize>,
    /// Size of the tuple header: 2 (version) + null_bitmap_size.
    header_size: usize,
}

impl TupleEncoder {
    /// Create an encoder for the given schema.
    pub fn new(schema: &StrictSchema) -> Self {
        let mut fixed_offsets = Vec::with_capacity(schema.columns.len());
        let mut var_indices = Vec::new();
        let mut fixed_offset = 0usize;

        for (i, col) in schema.columns.iter().enumerate() {
            if let Some(size) = col.column_type.fixed_size() {
                fixed_offsets.push(Some(fixed_offset));
                fixed_offset += size;
            } else {
                fixed_offsets.push(None);
                var_indices.push(i);
            }
        }

        // Header: magic(4) + format_version(1) + schema_version(4) + null_bitmap.
        let header_size = 9 + schema.null_bitmap_size();

        Self {
            schema: schema.clone(),
            fixed_offsets,
            fixed_section_size: fixed_offset,
            var_indices,
            header_size,
        }
    }

    /// Encode a row of values into a Binary Tuple.
    ///
    /// `values` must have exactly `schema.len()` entries. A `Value::Null` is
    /// allowed only if the corresponding column is nullable.
    pub fn encode(&self, values: &[Value]) -> Result<Vec<u8>, StrictError> {
        let n_cols = self.schema.columns.len();
        if values.len() != n_cols {
            return Err(StrictError::ValueCountMismatch {
                expected: n_cols,
                got: values.len(),
            });
        }

        // Pre-size: header + fixed + offset_table. Variable data appended later.
        let offset_table_size = (self.var_indices.len() + 1) * 4;
        let base_size = self.header_size + self.fixed_section_size + offset_table_size;
        let mut buf = vec![0u8; base_size];

        // 1. Magic, format version, schema version.
        buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        buf[4] = FORMAT_VERSION;
        buf[5..9].copy_from_slice(&self.schema.version.to_le_bytes());

        // 2. Null bitmap + fixed fields + type validation.
        let bitmap_start = 9;
        let fixed_start = self.header_size;

        for (i, (col, val)) in self.schema.columns.iter().zip(values.iter()).enumerate() {
            let is_null = matches!(val, Value::Null);

            if is_null {
                if !col.nullable {
                    return Err(StrictError::NullViolation(col.name.clone()));
                }
                // Set null bit: byte = i / 8, bit = i % 8.
                buf[bitmap_start + i / 8] |= 1 << (i % 8);
                // Fixed fields remain zeroed; no variable data emitted.
                continue;
            }

            // Type check (with coercion).
            if !col.column_type.accepts(val) {
                return Err(StrictError::TypeMismatch {
                    column: col.name.clone(),
                    expected: col.column_type,
                });
            }

            // Write fixed-size value.
            if let Some(offset) = self.fixed_offsets[i] {
                let dst = fixed_start + offset;
                encode_fixed(&mut buf[dst..], &col.column_type, val).map_err(|()| {
                    StrictError::TypeMismatch {
                        column: col.name.clone(),
                        expected: col.column_type,
                    }
                })?;
            }
            // Variable-length values are handled in the offset table pass below.
        }

        // 3. Variable-length fields: build offset table + variable data.
        let offset_table_start = self.header_size + self.fixed_section_size;
        let mut var_data: Vec<u8> = Vec::new();

        for (var_idx, &col_idx) in self.var_indices.iter().enumerate() {
            // Write current offset.
            let offset = var_data.len() as u32;
            let table_pos = offset_table_start + var_idx * 4;
            buf[table_pos..table_pos + 4].copy_from_slice(&offset.to_le_bytes());

            let val = &values[col_idx];
            if !matches!(val, Value::Null) {
                encode_variable(
                    &mut var_data,
                    &self.schema.columns[col_idx].column_type,
                    val,
                )
                .map_err(|()| StrictError::TypeMismatch {
                    column: self.schema.columns[col_idx].name.clone(),
                    expected: self.schema.columns[col_idx].column_type,
                })?;
            }
            // If null: offset stays the same as next entry → zero length.
        }

        // Final sentinel offset (marks end of last variable field).
        let sentinel = var_data.len() as u32;
        let sentinel_pos = offset_table_start + self.var_indices.len() * 4;
        buf[sentinel_pos..sentinel_pos + 4].copy_from_slice(&sentinel.to_le_bytes());

        // 4. Append variable data.
        buf.extend_from_slice(&var_data);

        Ok(buf)
    }

    /// Access the schema this encoder was built for.
    pub fn schema(&self) -> &StrictSchema {
        &self.schema
    }

    /// Encode a row for a bitemporal strict schema. The three reserved
    /// slots (0/1/2) are populated from the provided timestamps; the
    /// remaining slots are filled from `user_values` in schema order.
    ///
    /// Errors if the schema is not bitemporal or if `user_values.len() !=
    /// schema.len() - 3`.
    pub fn encode_bitemporal(
        &self,
        system_from_ms: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
        user_values: &[Value],
    ) -> Result<Vec<u8>, StrictError> {
        if !self.schema.bitemporal {
            return Err(StrictError::ValueCountMismatch {
                expected: self.schema.columns.len(),
                got: user_values.len() + 3,
            });
        }
        let expected_user = self.schema.columns.len().saturating_sub(3);
        if user_values.len() != expected_user {
            return Err(StrictError::ValueCountMismatch {
                expected: expected_user,
                got: user_values.len(),
            });
        }
        let mut all = Vec::with_capacity(self.schema.columns.len());
        all.push(Value::Integer(system_from_ms));
        all.push(Value::Integer(valid_from_ms));
        all.push(Value::Integer(valid_until_ms));
        all.extend_from_slice(user_values);
        self.encode(&all)
    }
}

/// Encode a fixed-size value into the buffer at the given position.
///
/// Handles both native Value types and SQL coercion sources.
fn encode_fixed(dst: &mut [u8], col_type: &ColumnType, value: &Value) -> Result<(), ()> {
    match (col_type, value) {
        // Int64: native.
        (ColumnType::Int64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        // Float64: native + Int64→Float64 coercion.
        (ColumnType::Float64, Value::Float(v)) => {
            dst[..8].copy_from_slice(&v.to_le_bytes());
        }
        (ColumnType::Float64, Value::Integer(v)) => {
            dst[..8].copy_from_slice(&(*v as f64).to_le_bytes());
        }
        // Bool: native.
        (ColumnType::Bool, Value::Bool(v)) => {
            dst[0] = *v as u8;
        }
        // Timestamp (naive): NaiveDateTime + Integer (micros) + String (ISO 8601 parse).
        (ColumnType::Timestamp, Value::NaiveDateTime(dt)) => {
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (ColumnType::Timestamp, Value::Integer(micros)) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        (ColumnType::Timestamp, Value::String(s)) => {
            let micros = nodedb_types::NdbDateTime::parse(s)
                .map(|dt| dt.micros)
                .unwrap_or(0);
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        // Timestamptz (TZ-aware): DateTime + Integer (micros) + String (ISO 8601 parse).
        (ColumnType::Timestamptz, Value::DateTime(dt)) => {
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (ColumnType::Timestamptz, Value::Integer(micros)) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        (ColumnType::Timestamptz, Value::String(s)) => {
            let micros = nodedb_types::NdbDateTime::parse(s)
                .map(|dt| dt.micros)
                .unwrap_or(0);
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        // System timestamp: UTC micros, decoded canonically as DateTime.
        (ColumnType::SystemTimestamp, Value::DateTime(dt)) => {
            dst[..8].copy_from_slice(&dt.micros.to_le_bytes());
        }
        (ColumnType::SystemTimestamp, Value::Integer(micros)) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        // ULID: parse its textual representation and retain the canonical 16-byte ID.
        (ColumnType::Ulid, Value::Ulid(s) | Value::String(s)) => {
            let id = ulid::Ulid::from_string(s).map_err(|_| ())?;
            dst[..16].copy_from_slice(&id.to_bytes());
        }
        // Duration: native duration, microseconds, or a human-readable literal.
        (ColumnType::Duration, Value::Duration(duration)) => {
            dst[..8].copy_from_slice(&duration.micros.to_le_bytes());
        }
        (ColumnType::Duration, Value::Integer(micros)) => {
            dst[..8].copy_from_slice(&micros.to_le_bytes());
        }
        (ColumnType::Duration, Value::String(s)) => {
            let duration = nodedb_types::NdbDuration::parse(s).ok_or(())?;
            dst[..8].copy_from_slice(&duration.micros.to_le_bytes());
        }
        // Decimal: native Decimal + String/Float/Integer coercion.
        (ColumnType::Decimal { .. }, Value::Decimal(d)) => {
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::String(s)) => {
            let d: rust_decimal::Decimal = s.parse().unwrap_or_default();
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::Float(f)) => {
            let d = rust_decimal::Decimal::try_from(*f).unwrap_or_default();
            dst[..16].copy_from_slice(&d.serialize());
        }
        (ColumnType::Decimal { .. }, Value::Integer(i)) => {
            let d = rust_decimal::Decimal::from(*i);
            dst[..16].copy_from_slice(&d.serialize());
        }
        // Uuid: native Uuid string + String coercion.
        (ColumnType::Uuid, Value::Uuid(s) | Value::String(s)) => {
            if let Ok(parsed) = uuid::Uuid::parse_str(s) {
                dst[..16].copy_from_slice(parsed.as_bytes());
            }
        }
        // Vector: Array of floats + Bytes (packed f32).
        (ColumnType::Vector(dim), Value::Array(arr)) => {
            let d = *dim as usize;
            for (i, v) in arr.iter().take(d).enumerate() {
                let f = match v {
                    Value::Float(f) => *f as f32,
                    Value::Integer(n) => *n as f32,
                    _ => 0.0,
                };
                dst[i * 4..(i + 1) * 4].copy_from_slice(&f.to_le_bytes());
            }
        }
        (ColumnType::Vector(dim), Value::Bytes(b)) => {
            let byte_len = (*dim as usize) * 4;
            let copy_len = b.len().min(byte_len);
            dst[..copy_len].copy_from_slice(&b[..copy_len]);
        }
        _ => {} // Type mismatch caught earlier by accepts().
    }
    Ok(())
}

/// Encode a variable-length value, appending to the data buffer.
///
/// Handles both native Value types and SQL coercion sources.
fn encode_variable(var_data: &mut Vec<u8>, col_type: &ColumnType, value: &Value) -> Result<(), ()> {
    match (col_type, value) {
        (ColumnType::String, Value::String(s)) => {
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::Bytes, Value::Bytes(b)) => {
            var_data.extend_from_slice(b);
        }
        // Geometry: native Geometry (JSON-serialized) + String (WKT/GeoJSON passthrough).
        (ColumnType::Geometry, Value::Geometry(g)) => {
            if let Ok(json) = sonic_rs::to_vec(g) {
                var_data.extend_from_slice(&json);
            }
        }
        (ColumnType::Geometry, Value::String(s)) => {
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::Json, Value::String(s)) => {
            // String input for JSON column: parse as JSON, then serialize as MessagePack.
            // This handles VALUES ('{"key":"val"}') where the SQL planner passes a string literal.
            let parsed = sonic_rs::from_str::<serde_json::Value>(s)
                .ok()
                .map(nodedb_types::Value::from);
            let to_encode = parsed.as_ref().unwrap_or(value);
            if let Ok(bytes) = nodedb_types::value_to_msgpack(to_encode) {
                var_data.extend_from_slice(&bytes);
            }
        }
        (ColumnType::Json, value) => {
            // Non-string input (Object, Array, etc.): serialize directly as MessagePack.
            if let Ok(bytes) = nodedb_types::value_to_msgpack(value) {
                var_data.extend_from_slice(&bytes);
            }
        }
        // SparseVector: a `'{id: weight}'` literal stored as raw UTF-8 bytes,
        // mirroring the String path (parsed at index-build time). Raw bytes
        // pass through unchanged.
        (ColumnType::SparseVector, Value::String(s)) => {
            var_data.extend_from_slice(s.as_bytes());
        }
        (ColumnType::SparseVector, Value::Bytes(b)) => {
            var_data.extend_from_slice(b);
        }
        // Typed variable columns use tagged NodeDB MessagePack so their Value
        // variant survives storage. Coercion inputs are converted first.
        (ColumnType::Array, Value::Array(_))
        | (ColumnType::Set, Value::Set(_))
        | (ColumnType::Regex, Value::Regex(_))
        | (ColumnType::Range, Value::Range { .. })
        | (ColumnType::Record, Value::Record { .. }) => {
            append_msgpack(var_data, value)?;
        }
        (ColumnType::Set, Value::Array(items)) => {
            append_msgpack(var_data, &Value::Set(items.clone()))?;
        }
        (ColumnType::Regex, Value::String(pattern)) => {
            append_msgpack(var_data, &Value::Regex(pattern.clone()))?;
        }
        (ColumnType::Record, Value::String(reference)) => {
            let (table, id) = reference.split_once(':').ok_or(())?;
            if table.is_empty() || id.is_empty() {
                return Err(());
            }
            append_msgpack(
                var_data,
                &Value::Record {
                    table: table.to_owned(),
                    id: id.to_owned(),
                },
            )?;
        }
        _ => {} // Type mismatch caught earlier by accepts().
    }
    Ok(())
}

/// Append a lossless NodeDB MessagePack representation.
fn append_msgpack(var_data: &mut Vec<u8>, value: &Value) -> Result<(), ()> {
    let bytes = zerompk::to_msgpack_vec(value).map_err(|_| ())?;
    var_data.extend_from_slice(&bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;
    use nodedb_types::datetime::NdbDateTime;

    use super::*;

    fn crm_schema() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("email", ColumnType::String),
            ColumnDef::required(
                "balance",
                ColumnType::Decimal {
                    precision: 18,
                    scale: 4,
                },
            ),
            ColumnDef::nullable("active", ColumnType::Bool),
        ])
        .unwrap()
    }

    #[test]
    fn encode_basic_row() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Integer(42),
            Value::String("Alice".into()),
            Value::String("alice@example.com".into()),
            Value::Decimal(rust_decimal::Decimal::new(5000, 2)),
            Value::Bool(true),
        ];

        let tuple = encoder.encode(&values).unwrap();

        // Header: magic(4) + format_version(1) + schema_version(4) + null_bitmap(1) = 10 bytes
        // magic = 0x5453_444E "NDST" LE
        assert_eq!(&tuple[0..4], &0x5453_444Eu32.to_le_bytes()); // magic
        assert_eq!(tuple[4], 1); // format_version
        assert_eq!(tuple[5], 1); // schema version low byte = 1
        assert_eq!(tuple[6], 0); // schema version byte 1
        assert_eq!(tuple[7], 0); // schema version byte 2
        assert_eq!(tuple[8], 0); // schema version byte 3
        assert_eq!(tuple[9], 0); // null bitmap: no nulls

        // Fixed section: Int64(8) + Decimal(16) + Bool(1) = 25 bytes
        // Starting at offset 10
        let id_bytes = &tuple[10..18];
        assert_eq!(i64::from_le_bytes(id_bytes.try_into().unwrap()), 42);
    }

    #[test]
    fn encode_with_nulls() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Integer(1),
            Value::String("Bob".into()),
            Value::Null, // email is nullable
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null, // active is nullable
        ];

        let tuple = encoder.encode(&values).unwrap();

        // Null bitmap at byte 9: bit 2 (email) and bit 4 (active) set.
        // Bit 2 = 0b00000100 = 4, bit 4 = 0b00010000 = 16. Combined = 20.
        assert_eq!(tuple[9], 0b00010100);
    }

    #[test]
    fn encode_null_violation() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::Null, // id is NOT NULL
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        let err = encoder.encode(&values).unwrap_err();
        assert!(matches!(err, StrictError::NullViolation(ref s) if s == "id"));
    }

    #[test]
    fn encode_type_mismatch() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let values = vec![
            Value::String("not_an_int".into()), // id expects Int64
            Value::String("x".into()),
            Value::Null,
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Null,
        ];

        let err = encoder.encode(&values).unwrap_err();
        assert!(matches!(err, StrictError::TypeMismatch { .. }));
    }

    #[test]
    fn encode_value_count_mismatch() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);

        let err = encoder.encode(&[Value::Integer(1)]).unwrap_err();
        assert!(matches!(err, StrictError::ValueCountMismatch { .. }));
    }

    #[test]
    fn encode_int_to_float_coercion() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("val", ColumnType::Float64)]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        // Int64 → Float64 coercion should work.
        let tuple = encoder.encode(&[Value::Integer(42)]).unwrap();
        // Header: magic(4)+format_version(1)+schema_version(4)+bitmap(1) = 10. Fixed: 8 bytes Float64.
        let f = f64::from_le_bytes(tuple[10..18].try_into().unwrap());
        assert_eq!(f, 42.0);
    }

    #[test]
    fn encode_timestamp() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("ts", ColumnType::Timestamp)]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        let dt = NdbDateTime::from_micros(1_700_000_000_000_000);
        let tuple = encoder.encode(&[Value::NaiveDateTime(dt)]).unwrap();
        let micros = i64::from_le_bytes(tuple[10..18].try_into().unwrap());
        assert_eq!(micros, 1_700_000_000_000_000);
    }

    #[test]
    fn encode_timestamptz() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("ts", ColumnType::Timestamptz)]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        let dt = NdbDateTime::from_micros(1_700_000_000_000_000);
        let tuple = encoder.encode(&[Value::DateTime(dt)]).unwrap();
        let micros = i64::from_le_bytes(tuple[10..18].try_into().unwrap());
        assert_eq!(micros, 1_700_000_000_000_000);
    }

    #[test]
    fn encode_decode_json_column() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("metadata", ColumnType::Json),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);

        let metadata = Value::Object(std::collections::HashMap::from([
            ("source".to_string(), Value::String("web".to_string())),
            ("priority".to_string(), Value::Integer(3)),
        ]));
        let values = vec![Value::Integer(1), metadata.clone()];
        let tuple = encoder.encode(&values).unwrap();

        // Tuple must be longer than just the header + fixed section.
        // Header: 10 bytes. Fixed: 8 (Int64). Offset table: 8 (2 entries × u32).
        // Variable data must be non-empty (MessagePack of the object).
        let min_size = 10 + 8 + 8;
        assert!(tuple.len() > min_size, "tuple should contain variable data");

        // Decode and verify the value roundtrips correctly.
        let decoder = crate::decode::TupleDecoder::new(&schema);
        let decoded = decoder.extract_all(&tuple).unwrap();
        assert_eq!(decoded[0], Value::Integer(1));
        assert_eq!(decoded[1], metadata);
    }

    #[test]
    fn encode_json_null() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("data", ColumnType::Json),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let tuple = encoder.encode(&[Value::Integer(1), Value::Null]).unwrap();
        // Null bitmap byte (index 9): bit 1 (column 1) should be set → 0b00000010 = 2.
        assert_eq!(tuple[9] & 0b10, 0b10);
    }

    #[test]
    fn encode_bitemporal_roundtrip() {
        let schema = StrictSchema::new_bitemporal(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .unwrap();
        assert!(schema.bitemporal);
        assert_eq!(schema.columns[0].name, "__system_from_ms");
        assert_eq!(schema.columns[1].name, "__valid_from_ms");
        assert_eq!(schema.columns[2].name, "__valid_until_ms");
        assert_eq!(schema.columns[3].name, "id");

        let encoder = TupleEncoder::new(&schema);
        let tuple = encoder
            .encode_bitemporal(
                100,
                200,
                i64::MAX,
                &[Value::Integer(42), Value::String("alice".into())],
            )
            .unwrap();

        let decoder = crate::decode::TupleDecoder::new(&schema);
        let (sys, vf, vu) = decoder.extract_bitemporal_timestamps(&tuple).unwrap();
        assert_eq!((sys, vf, vu), (100, 200, i64::MAX));
        assert_eq!(
            decoder.extract_by_name(&tuple, "id").unwrap(),
            Value::Integer(42)
        );
        assert_eq!(
            decoder.extract_by_name(&tuple, "name").unwrap(),
            Value::String("alice".into())
        );
    }

    #[test]
    fn reserved_column_name_rejected() {
        let err = StrictSchema::new(vec![ColumnDef::required(
            "__system_from_ms",
            ColumnType::Int64,
        )])
        .unwrap_err();
        assert!(matches!(
            err,
            nodedb_types::columnar::SchemaError::ReservedColumnName(ref s) if s == "__system_from_ms"
        ));
    }

    #[test]
    fn encode_bitemporal_rejects_wrong_user_count() {
        let schema = StrictSchema::new_bitemporal(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let err = encoder.encode_bitemporal(0, 0, 0, &[]).unwrap_err();
        assert!(matches!(
            err,
            StrictError::ValueCountMismatch {
                expected: 1,
                got: 0
            }
        ));
    }

    #[test]
    fn encode_bitemporal_on_non_bitemporal_schema_errors() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);
        let err = encoder.encode_bitemporal(0, 0, 0, &[]).unwrap_err();
        assert!(matches!(err, StrictError::ValueCountMismatch { .. }));
    }

    #[test]
    fn encode_vector() {
        let schema =
            StrictSchema::new(vec![ColumnDef::required("emb", ColumnType::Vector(3))]).unwrap();
        let encoder = TupleEncoder::new(&schema);

        let vals = vec![Value::Array(vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
        ])];
        let tuple = encoder.encode(&vals).unwrap();
        // Header: 10 bytes. Fixed: 12 bytes (3 × f32).
        let f0 = f32::from_le_bytes(tuple[10..14].try_into().unwrap());
        let f1 = f32::from_le_bytes(tuple[14..18].try_into().unwrap());
        let f2 = f32::from_le_bytes(tuple[18..22].try_into().unwrap());
        assert_eq!((f0, f1, f2), (1.0, 2.0, 3.0));
    }

    #[test]
    fn canonical_strict_types_roundtrip_through_decoder() {
        let schema = StrictSchema::new(vec![
            ColumnDef::required("system_ts", ColumnType::SystemTimestamp),
            ColumnDef::required("ulid", ColumnType::Ulid),
            ColumnDef::required("duration_native", ColumnType::Duration),
            ColumnDef::required("duration_integer", ColumnType::Duration),
            ColumnDef::required("duration_string", ColumnType::Duration),
            ColumnDef::required("array", ColumnType::Array),
            ColumnDef::required("set", ColumnType::Set),
            ColumnDef::required("regex", ColumnType::Regex),
            ColumnDef::required("range", ColumnType::Range),
            ColumnDef::required("record", ColumnType::Record),
        ])
        .unwrap();
        let encoder = TupleEncoder::new(&schema);
        let timestamp = NdbDateTime::from_micros(1_700_000_000_000_000);
        let range = Value::Range {
            start: Some(Box::new(Value::Integer(10))),
            end: Some(Box::new(Value::Integer(20))),
            inclusive: true,
        };
        let values = vec![
            Value::DateTime(timestamp),
            Value::String("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            Value::Duration(nodedb_types::NdbDuration::from_micros(42)),
            Value::Integer(-1_500),
            Value::String("1h30m".into()),
            Value::Array(vec![Value::Integer(1), Value::String("two".into())]),
            Value::Array(vec![Value::Integer(3)]),
            Value::String("^node.*$".into()),
            range.clone(),
            Value::String("users:42".into()),
        ];

        let tuple = encoder.encode(&values).unwrap();
        let decoder = crate::decode::TupleDecoder::new(&schema);
        let expected = vec![
            Value::DateTime(timestamp),
            Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
            Value::Duration(nodedb_types::NdbDuration::from_micros(42)),
            Value::Duration(nodedb_types::NdbDuration::from_micros(-1_500)),
            Value::Duration(nodedb_types::NdbDuration::from_micros(5_400_000_000)),
            Value::Array(vec![Value::Integer(1), Value::String("two".into())]),
            Value::Set(vec![Value::Integer(3)]),
            Value::Regex("^node.*$".into()),
            range,
            Value::Record {
                table: "users".into(),
                id: "42".into(),
            },
        ];

        assert_eq!(decoder.extract_all(&tuple).unwrap(), expected);
        assert_eq!(decoder.extract_value(&tuple, 9).unwrap(), expected[9]);
    }

    #[test]
    fn typed_variable_columns_reject_wrong_msgpack_variants() {
        let typed_columns = [
            ColumnType::Array,
            ColumnType::Set,
            ColumnType::Regex,
            ColumnType::Range,
            ColumnType::Record,
        ];

        for column_type in typed_columns {
            let schema =
                StrictSchema::new(vec![ColumnDef::required("value", column_type)]).unwrap();
            let encoder = TupleEncoder::new(&schema);
            let input = match column_type {
                ColumnType::Array => Value::Array(vec![]),
                ColumnType::Set => Value::Set(vec![]),
                ColumnType::Regex => Value::Regex("pattern".into()),
                ColumnType::Range => Value::Range {
                    start: None,
                    end: None,
                    inclusive: false,
                },
                ColumnType::Record => Value::Record {
                    table: "table".into(),
                    id: "id".into(),
                },
                _ => unreachable!(),
            };
            let mut tuple = encoder.encode(&[input]).unwrap();
            let wrong = zerompk::to_msgpack_vec(&Value::String("wrong".into())).unwrap();
            let variable_data_start = 10 + 8;
            tuple.truncate(variable_data_start);
            tuple.extend_from_slice(&wrong);
            tuple[14..18].copy_from_slice(&(wrong.len() as u32).to_le_bytes());

            let decoder = crate::decode::TupleDecoder::new(&schema);
            assert_eq!(decoder.extract_value(&tuple, 0).unwrap(), Value::Null);
        }
    }

    /// Asserts NDST magic at [0..4], FORMAT_VERSION == 1 at [4], and
    /// schema_version u32 at [5..9].
    #[test]
    fn golden_strict_tuple_format() {
        let schema = crm_schema();
        let encoder = TupleEncoder::new(&schema);
        let values = vec![
            Value::Integer(1),
            Value::String("A".into()),
            Value::String("a@b.com".into()),
            Value::Decimal(rust_decimal::Decimal::ZERO),
            Value::Bool(false),
        ];
        let tuple = encoder.encode(&values).unwrap();

        // Magic at [0..4]: "NDST" LE = 0x5453_444E.
        assert_eq!(
            &tuple[0..4],
            &0x5453_444Eu32.to_le_bytes(),
            "magic mismatch"
        );
        assert_eq!(&tuple[0..4], b"NDST", "magic bytes mismatch");

        // FORMAT_VERSION == 1 at [4].
        assert_eq!(tuple[4], FORMAT_VERSION, "format_version mismatch");
        assert_eq!(tuple[4], 1u8, "expected FORMAT_VERSION == 1");

        // schema_version u32 LE at [5..9].
        let schema_ver = u32::from_le_bytes([tuple[5], tuple[6], tuple[7], tuple[8]]);
        assert_eq!(
            schema_ver, 1u32,
            "schema_version must be 1 for version-1 schema"
        );

        // null bitmap at [9]: no nulls → 0.
        assert_eq!(tuple[9], 0u8, "expected no null bits");

        // Tuple must be longer than the 10-byte header.
        assert!(
            tuple.len() > 10,
            "tuple must contain fixed/variable data after header"
        );
    }
}
