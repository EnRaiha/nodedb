// SPDX-License-Identifier: Apache-2.0

//! `JsonValue` newtype wrapper around `serde_json::Value` with zerompk traits.

use zerompk::{ToMessagePack, Write};

use crate::value::float_text::float_to_json;

/// Newtype wrapper around `serde_json::Value` implementing zerompk traits.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonValue(pub serde_json::Value);

impl From<serde_json::Value> for JsonValue {
    #[inline]
    fn from(v: serde_json::Value) -> Self {
        Self(v)
    }
}

impl From<JsonValue> for serde_json::Value {
    #[inline]
    fn from(v: JsonValue) -> Self {
        v.0
    }
}

impl std::ops::Deref for JsonValue {
    type Target = serde_json::Value;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for JsonValue {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// ─── Serialization (via zerompk Write trait) ───────────────────────────────

impl ToMessagePack for JsonValue {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        write_json_value(&self.0, writer)
    }
}

fn write_json_value<W: Write>(val: &serde_json::Value, writer: &mut W) -> zerompk::Result<()> {
    match val {
        serde_json::Value::Null => writer.write_nil(),
        serde_json::Value::Bool(b) => writer.write_boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                writer.write_i64(i)
            } else if let Some(u) = n.as_u64() {
                writer.write_u64(u)
            } else if let Some(f) = n.as_f64() {
                writer.write_f64(f)
            } else {
                writer.write_f64(0.0)
            }
        }
        serde_json::Value::String(s) => writer.write_string(s),
        serde_json::Value::Array(arr) => {
            writer.write_array_len(arr.len())?;
            for item in arr {
                write_json_value(item, writer)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            writer.write_map_len(map.len())?;
            for (key, val) in map {
                writer.write_string(key)?;
                write_json_value(val, writer)?;
            }
            Ok(())
        }
    }
}

// ─── Deserialization (deterministic raw byte parser) ───────────────────────

impl<'a> zerompk::FromMessagePack<'a> for JsonValue {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        read_json_from_reader(reader)
    }
}

/// Read a JSON value from a zerompk reader.
///
/// SAFETY CONTRACT: This function relies on the reader returning Err WITHOUT
/// advancing the cursor when a marker byte doesn't match. This is guaranteed
/// by zerompk::SliceReader (used by from_msgpack) but NOT by IOReader.
fn read_json_from_reader<'a, R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<JsonValue> {
    // Try nil (0xC0)
    if reader.read_nil().is_ok() {
        return Ok(JsonValue(serde_json::Value::Null));
    }
    // Try bool (0xC2, 0xC3)
    if let Ok(b) = reader.read_boolean() {
        return Ok(JsonValue(serde_json::Value::Bool(b)));
    }
    // Try i64 (covers fixint 0x00-0x7F, neg fixint 0xE0-0xFF, int8-int64)
    if let Ok(i) = reader.read_i64() {
        return Ok(JsonValue(serde_json::Value::Number(i.into())));
    }
    // Try u64 (covers uint64 values > i64::MAX)
    if let Ok(u) = reader.read_u64() {
        return Ok(JsonValue(serde_json::Value::Number(u.into())));
    }
    // Try f64 (0xCB). A non-finite float is its PostgreSQL text in a JSON
    // string, never `null`.
    if let Ok(f) = reader.read_f64() {
        return Ok(JsonValue(float_to_json(f)));
    }
    // Try f32 (0xCA)
    if let Ok(f) = reader.read_f32() {
        return Ok(JsonValue(float_to_json(f64::from(f))));
    }
    // Try string (fixstr 0xA0-0xBF, str8 0xD9, str16 0xDA, str32 0xDB)
    if let Ok(s) = reader.read_string() {
        return Ok(JsonValue(serde_json::Value::String(s.into_owned())));
    }
    // Try array (fixarray 0x90-0x9F, array16 0xDC, array32 0xDD)
    if let Ok(len) = reader.read_array_len() {
        reader.increment_depth()?;
        let mut arr = Vec::with_capacity(len.min(4096));
        for _ in 0..len {
            let JsonValue(v) = read_json_from_reader(reader)?;
            arr.push(v);
        }
        reader.decrement_depth();
        return Ok(JsonValue(serde_json::Value::Array(arr)));
    }
    // Try map (fixmap 0x80-0x8F, map16 0xDE, map32 0xDF)
    if let Ok(len) = reader.read_map_len() {
        reader.increment_depth()?;
        let mut map = serde_json::Map::with_capacity(len.min(4096));
        for _ in 0..len {
            let key = reader.read_string()?;
            let JsonValue(val) = read_json_from_reader(reader)?;
            map.insert(key.into_owned(), val);
        }
        reader.decrement_depth();
        return Ok(JsonValue(serde_json::Value::Object(map)));
    }

    Err(zerompk::Error::InvalidMarker(0))
}
