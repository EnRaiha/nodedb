use std::collections::HashMap;

use nodedb_types::{
    Document, Value, json_from_msgpack, msgpack_to_json_string, value_from_msgpack,
    value_to_msgpack,
};

const MAX_MALFORMED_BYTES: usize = 1024 * 1024;
const MAX_SOURCE_PREFIX: usize = 32;
const MAX_ID_SOURCE_BYTES: usize = 8;
const MAX_DOCUMENT_FIELDS: usize = 4;
const MAX_NESTING: usize = 2;
const MAX_NESTED_MAP_FIELDS: usize = 2;
const MAX_VALID_FRAME_BYTES: usize = 512;

pub fn run(data: &[u8]) {
    let malformed = &data[..data.len().min(MAX_MALFORMED_BYTES)];
    decode_all(malformed);

    let source = &data[..data.len().min(MAX_SOURCE_PREFIX)];
    let id = document_id(source);
    let mut document = Document::new(id.clone());
    document
        .set("text", source_text(source))
        .set("number", Value::Integer(source_number(source)))
        .set("binary", Value::Bytes(source.to_vec()))
        .set("nested", nested_value(source));
    debug_assert!(document.fields.len() <= MAX_DOCUMENT_FIELDS);

    encode_document(&document, source);
    encode_document(&Document::new(id), source);
    encode_value(&nested_value(source), source);
}

fn decode_all(bytes: &[u8]) {
    let _ = value_from_msgpack(bytes);
    let _ = json_from_msgpack(bytes);
    let _ = msgpack_to_json_string(bytes);
    let _ = Document::from_msgpack(bytes);
}

fn encode_document(document: &Document, source: &[u8]) {
    if let Ok(bytes) = document.to_msgpack() {
        decode_valid_frame(&bytes, source);
    }
}

fn encode_value(value: &Value, source: &[u8]) {
    if let Ok(bytes) = value_to_msgpack(value) {
        decode_valid_frame(&bytes, source);
    }
}

fn decode_valid_frame(frame: &[u8], source: &[u8]) {
    decode_all(frame);

    if frame.is_empty() || frame.len() > MAX_VALID_FRAME_BYTES {
        return;
    }

    let mut header_flip = frame.to_vec();
    header_flip[0] ^= source_mask(source, 0);
    decode_all(&header_flip);

    if frame.len() > 1 {
        let body_index = 1 + source_index(source, frame.len() - 1);
        let mut body_flip = frame.to_vec();
        body_flip[body_index] ^= source_mask(source, 1);
        decode_all(&body_flip);
    }

    decode_all(&frame[..frame.len() - 1]);
}

fn source_index(source: &[u8], limit: usize) -> usize {
    match source.first() {
        Some(byte) => usize::from(*byte) % limit,
        None => 0,
    }
}

fn source_mask(source: &[u8], index: usize) -> u8 {
    match source.get(index) {
        Some(byte) => *byte | 1,
        None => 1,
    }
}

fn document_id(source: &[u8]) -> String {
    let encoded = source
        .iter()
        .take(MAX_ID_SOURCE_BYTES)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("doc-{encoded}")
}

fn source_text(source: &[u8]) -> Value {
    Value::String(source.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn source_number(source: &[u8]) -> i64 {
    source.iter().fold(0_i64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(i64::from(*byte))
    })
}

fn nested_value(source: &[u8]) -> Value {
    let mut value = Value::Bytes(source.to_vec());
    for level in 0..MAX_NESTING {
        let mut map = HashMap::with_capacity(MAX_NESTED_MAP_FIELDS);
        map.insert("level".to_owned(), Value::Integer(level as i64));
        map.insert("value".to_owned(), value);
        value = Value::Array(vec![Value::Object(map)]);
    }
    value
}
