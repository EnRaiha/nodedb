use nodedb_strict::{TupleDecoder, TupleEncoder};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::geometry::Geometry;
use nodedb_types::value::Value;
use nodedb_types::{NdbDateTime, NdbDuration};

const MAX_SCHEMA_COLUMNS: usize = 16;
const COLUMN_TYPE_COUNT: u8 = 21;
const MAX_DESCRIPTOR_BYTES: usize = 1 + (MAX_SCHEMA_COLUMNS * 2);
const MAX_TUPLE_BYTES: usize = 4096;
const MAX_VECTOR_DIMENSIONS: u32 = 32;
const MAX_TRUNCATION_BYTES: usize = 16;

fn partition_input(data: &[u8]) -> (&[u8], &[u8]) {
    let bounded = &data[..data.len().min(MAX_DESCRIPTOR_BYTES + MAX_TUPLE_BYTES)];
    let descriptor_len = bounded
        .first()
        .map_or(0, |byte| 1 + (usize::from(*byte) % MAX_DESCRIPTOR_BYTES))
        .min(bounded.len());
    let (descriptor, remainder) = bounded.split_at(descriptor_len);
    let tuple_len = remainder.len().min(MAX_TUPLE_BYTES);
    (descriptor, &remainder[..tuple_len])
}

fn schema_from_descriptor(descriptor: &[u8]) -> StrictSchema {
    let count = descriptor
        .first()
        .map_or(1, |byte| (usize::from(*byte) % MAX_SCHEMA_COLUMNS) + 1);
    let columns = (0..count)
        .map(|index| {
            let spec_offset = 1 + (index * 2);
            let kind = descriptor.get(spec_offset).copied().unwrap_or(0) % COLUMN_TYPE_COUNT;
            let column_type = match kind {
                0 => ColumnType::Int64,
                1 => ColumnType::Float64,
                2 => ColumnType::Bool,
                3 => ColumnType::String,
                4 => ColumnType::Bytes,
                5 => ColumnType::Timestamp,
                6 => ColumnType::Timestamptz,
                7 => ColumnType::Decimal {
                    precision: 38,
                    scale: 10,
                },
                8 => ColumnType::Uuid,
                9 => ColumnType::Vector(
                    descriptor
                        .get(spec_offset + 1)
                        .copied()
                        .map_or(1, |byte| u32::from(byte % MAX_VECTOR_DIMENSIONS as u8) + 1),
                ),
                10 => ColumnType::SparseVector,
                11 => ColumnType::Geometry,
                12 => ColumnType::Json,
                13 => ColumnType::SystemTimestamp,
                14 => ColumnType::Ulid,
                15 => ColumnType::Duration,
                16 => ColumnType::Array,
                17 => ColumnType::Set,
                18 => ColumnType::Regex,
                19 => ColumnType::Range,
                _ => ColumnType::Record,
            };
            ColumnDef::required(format!("f{index}"), column_type)
        })
        .collect();
    StrictSchema {
        columns,
        version: 1,
        dropped_columns: Vec::new(),
        bitemporal: false,
    }
}

fn valid_value(column_type: ColumnType, seed: u8) -> Option<Value> {
    match column_type {
        ColumnType::Int64 => Some(Value::Integer(i64::from(seed))),
        ColumnType::Timestamp => Some(Value::NaiveDateTime(NdbDateTime::from_micros(i64::from(
            seed,
        )))),
        ColumnType::Timestamptz | ColumnType::SystemTimestamp => {
            Some(Value::DateTime(NdbDateTime::from_micros(i64::from(seed))))
        }
        ColumnType::Float64 => Some(Value::Float(f64::from(seed))),
        ColumnType::Bool => Some(Value::Bool(seed & 1 != 0)),
        ColumnType::String => Some(Value::String(format!("s{seed}"))),
        ColumnType::Bytes => Some(Value::Bytes(vec![seed, seed.wrapping_add(1)])),
        ColumnType::Decimal { .. } => Some(Value::Decimal(i64::from(seed).into())),
        ColumnType::Uuid => Some(Value::Uuid(
            "00000000-0000-0000-0000-000000000000".to_owned(),
        )),
        ColumnType::Vector(dim) => {
            let dimensions = usize::try_from(dim).ok()?;
            if dimensions > MAX_VECTOR_DIMENSIONS as usize {
                return None;
            }
            Some(Value::Array(vec![
                Value::Float(f64::from(seed));
                dimensions
            ]))
        }
        ColumnType::SparseVector => Some(Value::String("{1: 1.0}".to_owned())),
        ColumnType::Geometry => Some(Value::Geometry(Geometry::point(
            f64::from(seed) - 128.0,
            f64::from(seed % 181) - 90.0,
        ))),
        ColumnType::Json => Some(Value::Array(vec![Value::Integer(i64::from(seed))])),
        ColumnType::Ulid => Some(Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned())),
        ColumnType::Duration => Some(Value::Duration(NdbDuration::from_micros(i64::from(seed)))),
        ColumnType::Array => Some(Value::Array(vec![Value::Integer(i64::from(seed))])),
        ColumnType::Set => Some(Value::Set(vec![Value::Integer(i64::from(seed))])),
        ColumnType::Regex => Some(Value::Regex("^s$".to_owned())),
        ColumnType::Range => Some(Value::Range {
            start: Some(Box::new(Value::Integer(i64::from(seed)))),
            end: Some(Box::new(Value::Integer(i64::from(seed) + 1))),
            inclusive: false,
        }),
        ColumnType::Record => Some(Value::Record {
            table: "t".to_owned(),
            id: format!("r{seed}"),
        }),
        _ => None,
    }
}

fn valid_values(schema: &StrictSchema, descriptor: &[u8]) -> Option<Vec<Value>> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            valid_value(
                column.column_type,
                descriptor.get(1 + (index * 2)).copied().unwrap_or(0),
            )
        })
        .collect()
}

fn all_supported_schema() -> StrictSchema {
    let column_types = [
        ColumnType::Int64,
        ColumnType::Float64,
        ColumnType::Bool,
        ColumnType::String,
        ColumnType::Bytes,
        ColumnType::Timestamp,
        ColumnType::Timestamptz,
        ColumnType::SystemTimestamp,
        ColumnType::Decimal {
            precision: 38,
            scale: 10,
        },
        ColumnType::Uuid,
        ColumnType::Vector(1),
        ColumnType::SparseVector,
        ColumnType::Geometry,
        ColumnType::Json,
        ColumnType::Ulid,
        ColumnType::Duration,
        ColumnType::Array,
        ColumnType::Set,
        ColumnType::Regex,
        ColumnType::Range,
        ColumnType::Record,
    ];
    let columns = column_types
        .into_iter()
        .enumerate()
        .map(|(index, column_type)| ColumnDef::required(format!("f{index}"), column_type))
        .collect();
    StrictSchema {
        columns,
        version: 1,
        dropped_columns: Vec::new(),
        bitemporal: false,
    }
}

fn assert_valid_roundtrip(decoder: &TupleDecoder, tuple: &[u8], expected: &[Value]) {
    let decoded = decoder
        .extract_all(tuple)
        .expect("encoder output must be accepted by its matching decoder");
    assert_eq!(decoded, expected, "strict tuple roundtrip changed values");
}

fn exercise_decoder(decoder: &TupleDecoder, schema: &StrictSchema, tuple: &[u8]) {
    let _ = decoder.schema();
    let _ = decoder.schema_version(tuple);
    for (index, column) in schema.columns.iter().enumerate() {
        let _ = decoder.is_null(tuple, index);
        let _ = decoder.extract_fixed_raw(tuple, index);
        let _ = decoder.extract_variable_raw(tuple, index);
        let _ = decoder.extract_value(tuple, index);
        let _ = decoder.extract_by_name(tuple, &column.name);
    }
    let _ = decoder.extract_all(tuple);
    let _ = decoder.extract_by_name(tuple, "missing");

    for old_col_count in [0, schema.columns.len() / 2, schema.columns.len()] {
        for index in 0..schema.columns.len() {
            let _ = decoder.extract_value_versioned(tuple, index, old_col_count);
        }
    }
}

fn mutation_byte(data: &[u8], index: usize) -> u8 {
    let bounded_index = index % data.len().max(1);
    data.get(bounded_index).map_or(0, |byte| *byte)
}

fn exercise_near_valid_mutations<F>(
    decoder: &TupleDecoder,
    tuple: &[u8],
    data: &[u8],
    mut exercise: F,
) where
    F: FnMut(&[u8]),
{
    if tuple.is_empty() {
        return;
    }

    // One clone per mutation: header bit, variable-area/body bit, truncation.
    let header_len = decoder.fixed_section_start().min(tuple.len());
    let mut header_mutation = tuple.to_vec();
    let header_index = usize::from(mutation_byte(data, 0)) % header_len;
    let header_bit = mutation_byte(data, 1) & 7;
    header_mutation[header_index] ^= 1 << header_bit;
    exercise(&header_mutation);

    let body_start = match decoder.var_data_start() {
        Ok(start) if start < tuple.len() => start,
        _ => decoder.fixed_section_start().min(tuple.len() - 1),
    };
    let mut body_mutation = tuple.to_vec();
    let body_len = tuple.len() - body_start;
    let body_index = body_start + (usize::from(mutation_byte(data, 2)) % body_len);
    let body_bit = mutation_byte(data, 3) & 7;
    body_mutation[body_index] ^= 1 << body_bit;
    exercise(&body_mutation);

    let truncation_limit = tuple.len().min(MAX_TRUNCATION_BYTES);
    let truncation = 1 + (usize::from(mutation_byte(data, 4)) % truncation_limit);
    let truncated = tuple[..tuple.len() - truncation].to_vec();
    exercise(&truncated);
}

fn exercise_bitemporal_decoder(decoder: &TupleDecoder, tuple: &[u8]) {
    let _ = decoder.extract_bitemporal_timestamps(tuple);
    let _ = decoder.extract_all(tuple);
}

pub fn run(data: &[u8]) {
    let (descriptor, arbitrary_tuple) = partition_input(data);
    let schema = schema_from_descriptor(descriptor);
    let decoder = TupleDecoder::new(&schema);

    // Arbitrary tuple bytes remain independent from the schema descriptor.
    exercise_decoder(&decoder, &schema, arbitrary_tuple);

    if let Some(values) = valid_values(&schema, descriptor) {
        let encoder = TupleEncoder::new(&schema);
        if let Ok(tuple) = encoder.encode(&values) {
            assert_valid_roundtrip(&decoder, &tuple, &values);
            exercise_decoder(&decoder, &schema, &tuple);
            exercise_near_valid_mutations(&decoder, &tuple, data, |mutated| {
                exercise_decoder(&decoder, &schema, mutated);
            });
        }
    }

    // This path is independent of the descriptor-derived schema, so even a
    // one-byte input reaches valid encode/decode paths for every strict type.
    let all_types_schema = all_supported_schema();
    let all_types_decoder = TupleDecoder::new(&all_types_schema);
    if let Some(values) = valid_values(&all_types_schema, descriptor) {
        let encoder = TupleEncoder::new(&all_types_schema);
        if let Ok(tuple) = encoder.encode(&values) {
            assert_valid_roundtrip(&all_types_decoder, &tuple, &values);
            exercise_decoder(&all_types_decoder, &all_types_schema, &tuple);
            exercise_near_valid_mutations(&all_types_decoder, &tuple, data, |mutated| {
                exercise_decoder(&all_types_decoder, &all_types_schema, mutated);
            });
        }
    }

    let bitemporal_schema =
        StrictSchema::new_bitemporal(vec![ColumnDef::required("payload", ColumnType::Bytes)]);
    if let Ok(bitemporal_schema) = bitemporal_schema {
        let bitemporal_decoder = TupleDecoder::new(&bitemporal_schema);
        let _ = bitemporal_decoder.extract_bitemporal_timestamps(arbitrary_tuple);

        let encoder = TupleEncoder::new(&bitemporal_schema);
        let payload = Value::Bytes(descriptor.iter().copied().take(16).collect());
        if let Ok(tuple) = encoder.encode_bitemporal(1, 2, 3, &[payload]) {
            exercise_bitemporal_decoder(&bitemporal_decoder, &tuple);
            exercise_near_valid_mutations(&bitemporal_decoder, &tuple, data, |mutated| {
                exercise_bitemporal_decoder(&bitemporal_decoder, mutated);
            });
        }
    }
}
