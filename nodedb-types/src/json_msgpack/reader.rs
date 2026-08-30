// SPDX-License-Identifier: Apache-2.0

//! Cursor-based msgpack → `serde_json::Value` and `nodedb_types::Value` readers.
//!
//! Deterministic raw byte parser — the first byte of each msgpack value
//! unambiguously identifies its type per the msgpack specification.

use super::error::{MsgpackError, MsgpackResult};

pub(crate) struct Cursor<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) pos: usize,
    pub(crate) depth: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            depth: 0,
        }
    }

    #[inline]
    pub(crate) fn peek(&self) -> zerompk::Result<u8> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or(zerompk::Error::BufferTooSmall)
    }

    #[inline]
    pub(crate) fn take(&mut self) -> zerompk::Result<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    pub(crate) fn take_n(&mut self, n: usize) -> zerompk::Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return Err(zerompk::Error::BufferTooSmall);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub(crate) fn read_u16_be(&mut self) -> zerompk::Result<u16> {
        let b = self.take_n(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub(crate) fn read_u32_be(&mut self) -> zerompk::Result<u32> {
        let b = self.take_n(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Assert the whole input belonged to the value just read.
    ///
    /// A body carries exactly one top-level value, so anything left over means
    /// the bytes are not the value they claim to be — a stray suffix, a
    /// truncated body with another appended, or two values in one slot.
    /// Returning the leading value and dropping the rest would report success
    /// on corrupt bytes, so the remainder is an error.
    pub(crate) fn finish(&self) -> MsgpackResult<()> {
        if self.pos == self.data.len() {
            Ok(())
        } else {
            Err(MsgpackError::TrailingBytes {
                consumed: self.pos,
                total: self.data.len(),
            })
        }
    }
}

/// Deserialize a `serde_json::Value` from MessagePack bytes.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected.
pub fn json_from_msgpack(bytes: &[u8]) -> MsgpackResult<serde_json::Value> {
    let mut cursor = Cursor::new(bytes);
    let value = read_json_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

/// Deserialize a `nodedb_types::Value` from standard MessagePack bytes.
///
/// The input must contain exactly one top-level value and nothing else;
/// trailing bytes are rejected.
pub fn value_from_msgpack(bytes: &[u8]) -> MsgpackResult<crate::Value> {
    let mut cursor = Cursor::new(bytes);
    let value = read_native_value(&mut cursor)?;
    cursor.finish()?;
    Ok(value)
}

// ── JSON value reader ──

fn read_json_value(c: &mut Cursor<'_>) -> zerompk::Result<serde_json::Value> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        0xC0 => Ok(serde_json::Value::Null),
        0xC2 => Ok(serde_json::Value::Bool(false)),
        0xC3 => Ok(serde_json::Value::Bool(true)),

        0x00..=0x7F => Ok(serde_json::Value::Number((marker as i64).into())),
        0xE0..=0xFF => Ok(serde_json::Value::Number((marker as i8 as i64).into())),

        0xCC => Ok(serde_json::Value::Number(c.take()?.into())),
        0xCD => Ok(serde_json::Value::Number(c.read_u16_be()?.into())),
        0xCE => Ok(serde_json::Value::Number(c.read_u32_be()?.into())),
        0xCF => {
            let b = c.take_n(8)?;
            let v = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(serde_json::Value::Number(v.into()))
        }

        0xD0 => Ok(serde_json::Value::Number((c.take()? as i8 as i64).into())),
        0xD1 => {
            let b = c.take_n(2)?;
            Ok(serde_json::Value::Number(
                (i16::from_be_bytes([b[0], b[1]]) as i64).into(),
            ))
        }
        0xD2 => {
            let b = c.take_n(4)?;
            Ok(serde_json::Value::Number(
                (i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64).into(),
            ))
        }
        0xD3 => {
            let b = c.take_n(8)?;
            Ok(serde_json::Value::Number(
                i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]).into(),
            ))
        }

        0xCA => {
            let b = c.take_n(4)?;
            Ok(serde_json::json!(
                f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64
            ))
        }
        0xCB => {
            let b = c.take_n(8)?;
            Ok(serde_json::json!(f64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]
            ])))
        }

        m @ 0xA0..=0xBF => read_json_str(c, (m & 0x1F) as usize),
        0xD9 => {
            let l = c.take()? as usize;
            read_json_str(c, l)
        }
        0xDA => {
            let l = c.read_u16_be()? as usize;
            read_json_str(c, l)
        }
        0xDB => {
            let l = c.read_u32_be()? as usize;
            read_json_str(c, l)
        }

        0xC4 => {
            let l = c.take()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }
        0xC5 => {
            let l = c.read_u16_be()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }
        0xC6 => {
            let l = c.read_u32_be()? as usize;
            Ok(serde_json::Value::String(base64_encode(c.take_n(l)?)))
        }

        m @ 0x90..=0x9F => read_json_array(c, (m & 0x0F) as usize),
        0xDC => {
            let l = c.read_u16_be()? as usize;
            read_json_array(c, l)
        }
        0xDD => {
            let l = c.read_u32_be()? as usize;
            read_json_array(c, l)
        }

        m @ 0x80..=0x8F => read_json_map(c, (m & 0x0F) as usize),
        0xDE => {
            let l = c.read_u16_be()? as usize;
            read_json_map(c, l)
        }
        0xDF => {
            let l = c.read_u32_be()? as usize;
            read_json_map(c, l)
        }

        // ext types — skip
        0xD4 => {
            c.take_n(2)?;
            Ok(serde_json::Value::Null)
        }
        0xD5 => {
            c.take_n(3)?;
            Ok(serde_json::Value::Null)
        }
        0xD6 => {
            c.take_n(5)?;
            Ok(serde_json::Value::Null)
        }
        0xD7 => {
            c.take_n(9)?;
            Ok(serde_json::Value::Null)
        }
        0xD8 => {
            c.take_n(17)?;
            Ok(serde_json::Value::Null)
        }
        0xC7 => {
            let l = c.take()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }
        0xC8 => {
            let l = c.read_u16_be()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }
        0xC9 => {
            let l = c.read_u32_be()? as usize;
            c.take_n(1 + l)?;
            Ok(serde_json::Value::Null)
        }

        _ => Err(zerompk::Error::InvalidMarker(marker)),
    }
}

fn read_json_str(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    let bytes = c.take_n(len)?;
    let s = String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
    Ok(serde_json::Value::String(s))
}

fn read_json_array(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut arr = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        arr.push(read_json_value(c)?);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Array(arr))
}

fn read_json_map(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<serde_json::Value> {
    c.depth += 1;
    let mut map = serde_json::Map::with_capacity(len.min(4096));
    for _ in 0..len {
        let key_marker = c.peek()?;
        let key = if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            match read_json_value(c)? {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            }
        } else {
            read_json_value(c)?.to_string()
        };
        let val = read_json_value(c)?;
        map.insert(key, val);
    }
    c.depth -= 1;
    Ok(serde_json::Value::Object(map))
}

// ── Native value reader ──

fn read_native_value(c: &mut Cursor<'_>) -> zerompk::Result<crate::Value> {
    if c.depth > 500 {
        return Err(zerompk::Error::DepthLimitExceeded { max: 500 });
    }

    let marker = c.take()?;
    match marker {
        0xC0 => Ok(crate::Value::Null),
        0xC2 => Ok(crate::Value::Bool(false)),
        0xC3 => Ok(crate::Value::Bool(true)),

        0x00..=0x7F => Ok(crate::Value::Integer(marker as i64)),
        0xE0..=0xFF => Ok(crate::Value::Integer(marker as i8 as i64)),

        0xCC => Ok(crate::Value::Integer(c.take()? as i64)),
        0xCD => Ok(crate::Value::Integer(c.read_u16_be()? as i64)),
        0xCE => Ok(crate::Value::Integer(c.read_u32_be()? as i64)),
        0xCF => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Integer(u64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ]) as i64))
        }

        0xD0 => Ok(crate::Value::Integer(c.take()? as i8 as i64)),
        0xD1 => {
            let b = c.take_n(2)?;
            Ok(crate::Value::Integer(
                i16::from_be_bytes([b[0], b[1]]) as i64
            ))
        }
        0xD2 => {
            let b = c.take_n(4)?;
            Ok(crate::Value::Integer(
                i32::from_be_bytes([b[0], b[1], b[2], b[3]]) as i64,
            ))
        }
        0xD3 => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Integer(i64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }

        0xCA => {
            let b = c.take_n(4)?;
            Ok(crate::Value::Float(
                f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64,
            ))
        }
        0xCB => {
            let b = c.take_n(8)?;
            Ok(crate::Value::Float(f64::from_be_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])))
        }

        m @ 0xA0..=0xBF => read_native_str(c, (m & 0x1F) as usize),
        0xD9 => {
            let l = c.take()? as usize;
            read_native_str(c, l)
        }
        0xDA => {
            let l = c.read_u16_be()? as usize;
            read_native_str(c, l)
        }
        0xDB => {
            let l = c.read_u32_be()? as usize;
            read_native_str(c, l)
        }

        0xC4 => {
            let l = c.take()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }
        0xC5 => {
            let l = c.read_u16_be()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }
        0xC6 => {
            let l = c.read_u32_be()? as usize;
            Ok(crate::Value::Bytes(c.take_n(l)?.to_vec()))
        }

        m @ 0x90..=0x9F => read_native_array(c, (m & 0x0F) as usize),
        0xDC => {
            let l = c.read_u16_be()? as usize;
            read_native_array(c, l)
        }
        0xDD => {
            let l = c.read_u32_be()? as usize;
            read_native_array(c, l)
        }

        m @ 0x80..=0x8F => read_native_map(c, (m & 0x0F) as usize),
        0xDE => {
            let l = c.read_u16_be()? as usize;
            read_native_map(c, l)
        }
        0xDF => {
            let l = c.read_u32_be()? as usize;
            read_native_map(c, l)
        }

        // ext types — skip
        0xD4 => {
            c.take_n(2)?;
            Ok(crate::Value::Null)
        }
        0xD5 => {
            c.take_n(3)?;
            Ok(crate::Value::Null)
        }
        0xD6 => {
            c.take_n(5)?;
            Ok(crate::Value::Null)
        }
        0xD7 => {
            c.take_n(9)?;
            Ok(crate::Value::Null)
        }
        0xD8 => {
            c.take_n(17)?;
            Ok(crate::Value::Null)
        }
        0xC7 => {
            let l = c.take()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }
        0xC8 => {
            let l = c.read_u16_be()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }
        0xC9 => {
            let l = c.read_u32_be()? as usize;
            c.take_n(1 + l)?;
            Ok(crate::Value::Null)
        }

        _ => Err(zerompk::Error::InvalidMarker(marker)),
    }
}

fn read_native_str(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    let bytes = c.take_n(len)?;
    let s = String::from_utf8(bytes.to_vec()).map_err(|_| zerompk::Error::InvalidMarker(0))?;
    Ok(crate::Value::String(s))
}

fn read_native_array(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    c.depth += 1;
    let mut arr = Vec::with_capacity(len.min(4096));
    for _ in 0..len {
        arr.push(read_native_value(c)?);
    }
    c.depth -= 1;
    Ok(crate::Value::Array(arr))
}

fn read_native_map(c: &mut Cursor<'_>, len: usize) -> zerompk::Result<crate::Value> {
    c.depth += 1;
    let mut map = std::collections::HashMap::with_capacity(len.min(4096));
    for _ in 0..len {
        let key_marker = c.peek()?;
        let key = if (0xA0..=0xBF).contains(&key_marker)
            || key_marker == 0xD9
            || key_marker == 0xDA
            || key_marker == 0xDB
        {
            match read_native_value(c)? {
                crate::Value::String(s) => s,
                other => format!("{other:?}"),
            }
        } else {
            let v = read_native_value(c)?;
            format!("{v:?}")
        };
        let val = read_native_value(c)?;
        map.insert(key, val);
    }
    c.depth -= 1;
    Ok(crate::Value::Object(map))
}

// ── Shared helpers ──

pub(crate) fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        let _ = write!(out, "{}", CHARS[((triple >> 18) & 0x3F) as usize] as char);
        let _ = write!(out, "{}", CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            let _ = write!(out, "{}", CHARS[((triple >> 6) & 0x3F) as usize] as char);
        }
        if chunk.len() > 2 {
            let _ = write!(out, "{}", CHARS[(triple & 0x3F) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    //! Roundtrip tests for json_msgpack reader/writer.

    use super::*;
    use crate::json_msgpack::msgpack_to_json_string;
    use crate::json_msgpack::writer::{json_to_msgpack, value_to_msgpack};
    use serde_json::json;

    #[test]
    fn roundtrip_null() {
        let val = json!(null);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_bool() {
        for val in [json!(true), json!(false)] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_integers() {
        for val in [
            json!(0),
            json!(42),
            json!(-1),
            json!(i64::MAX),
            json!(i64::MIN),
        ] {
            let bytes = json_to_msgpack(&val).unwrap();
            let restored = json_from_msgpack(&bytes).unwrap();
            assert_eq!(val, restored);
        }
    }

    #[test]
    fn roundtrip_float() {
        let val = json!(9.81);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_string() {
        let val = json!("hello world");
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_array() {
        let val = json!([1, "two", true, null, 2.72]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_nested_object() {
        let val = json!({"a": 1, "b": {"c": [2, 3]}, "d": null});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_map() {
        let val = json!({});
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_empty_array() {
        let val = json!([]);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn roundtrip_large_string() {
        let s = "x".repeat(300);
        let val = json!(s);
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }

    #[test]
    fn native_value_roundtrip() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), crate::Value::String("host1".into()));
        map.insert("cpu".to_string(), crate::Value::Float(0.75));
        map.insert("mem".to_string(), crate::Value::Float(0.5));

        let row = crate::Value::Object(map);
        let arr = crate::Value::Array(vec![row]);

        let bytes = value_to_msgpack(&arr).unwrap();
        let decoded = value_from_msgpack(&bytes).unwrap();

        match &decoded {
            crate::Value::Array(items) => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    crate::Value::Object(m) => {
                        assert_eq!(m.len(), 3);
                        assert_eq!(m.get("id"), Some(&crate::Value::String("host1".into())));
                        assert_eq!(m.get("cpu"), Some(&crate::Value::Float(0.75)));
                        assert_eq!(m.get("mem"), Some(&crate::Value::Float(0.5)));
                    }
                    other => panic!("expected Object, got {other:?}"),
                }
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    /// The readers decode exactly one top-level value. Bytes left over mean the
    /// input is not the value it claims to be, so returning the leading value would
    /// report success on corrupt input.
    #[test]
    fn trailing_byte_is_rejected() {
        let val = json!({"a": 1, "b": "two"});
        let mut bytes = json_to_msgpack(&val).unwrap();
        assert_eq!(json_from_msgpack(&bytes).unwrap(), val);

        bytes.push(0xC0);
        match json_from_msgpack(&bytes) {
            Err(MsgpackError::TrailingBytes { consumed, total }) => {
                assert_eq!(total, consumed + 1);
            }
            other => panic!("expected TrailingBytes, got {other:?}"),
        }
        assert!(value_from_msgpack(&bytes).is_err());
        assert!(msgpack_to_json_string(&bytes).is_err());
    }

    #[test]
    fn two_concatenated_values_are_rejected() {
        let first = json_to_msgpack(&json!({"a": 1})).unwrap();
        let second = json_to_msgpack(&json!({"b": 2})).unwrap();
        let mut joined = first.clone();
        joined.extend_from_slice(&second);

        assert!(json_from_msgpack(&first).is_ok());
        assert!(json_from_msgpack(&joined).is_err());
        assert!(value_from_msgpack(&joined).is_err());
        assert!(msgpack_to_json_string(&joined).is_err());
    }

    #[test]
    fn empty_input_is_unchanged() {
        // Readers still fail on empty input; the transcoder still yields "".
        assert!(json_from_msgpack(&[]).is_err());
        assert!(value_from_msgpack(&[]).is_err());
        assert_eq!(msgpack_to_json_string(&[]).unwrap(), "");
    }

    #[test]
    fn native_value_scalars() {
        let cases: Vec<crate::Value> = vec![
            crate::Value::Null,
            crate::Value::Bool(true),
            crate::Value::Integer(42),
            crate::Value::Float(2.72),
            crate::Value::String("hello".into()),
        ];
        for val in cases {
            let bytes = value_to_msgpack(&val).unwrap();
            let decoded = value_from_msgpack(&bytes).unwrap();
            assert_eq!(val, decoded);
        }
    }
}
