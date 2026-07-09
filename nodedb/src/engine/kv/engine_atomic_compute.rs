// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for `INCR`/`INCR_FLOAT`/`CAS`/`GETSET`, shared by
//! the autocommit `KvEngine` methods (`engine_atomic.rs`) and the
//! in-transaction staging handlers (`stage_kv_atomic.rs`), so a staged value
//! and its COMMIT-time durable replay are always computed by the exact same
//! code. Split out of `engine_atomic.rs` to keep that file under the
//! file-size limit.

use super::engine_atomic::AtomicError;

/// Decode a MessagePack-encoded value as i64.
///
/// If the value is a map (typed KV entry), extracts the first numeric field.
fn decode_msgpack_i64(bytes: &[u8]) -> Result<i64, AtomicError> {
    // Try i64 first, then u64 (MessagePack encodes small positive as u64).
    if let Ok(v) = zerompk::from_msgpack::<i64>(bytes) {
        return Ok(v);
    }
    if let Ok(v) = zerompk::from_msgpack::<u64>(bytes) {
        return i64::try_from(v).map_err(|_| AtomicError::Overflow);
    }
    // Try f64 → i64 truncation for values stored as float.
    if let Ok(v) = zerompk::from_msgpack::<f64>(bytes)
        && v.fract() == 0.0
        && v >= i64::MIN as f64
        && v <= i64::MAX as f64
    {
        return Ok(v as i64);
    }
    // If value is a map (typed KV entry), find the first numeric field.
    if let Ok(nodedb_types::Value::Object(map)) = nodedb_types::value_from_msgpack(bytes) {
        for (k, v) in &map {
            if k == "key" {
                continue;
            }
            match v {
                nodedb_types::Value::Integer(i) => return Ok(*i),
                nodedb_types::Value::Float(f) if f.fract() == 0.0 => return Ok(*f as i64),
                _ => {}
            }
        }
    }
    Err(AtomicError::TypeMismatch {
        detail: "value is not an integer".into(),
    })
}

/// Decode a MessagePack-encoded value as f64.
///
/// If the value is a map (typed KV entry), extracts the first numeric field.
fn decode_msgpack_f64(bytes: &[u8]) -> Result<f64, AtomicError> {
    if let Ok(v) = zerompk::from_msgpack::<f64>(bytes) {
        return Ok(v);
    }
    // Accept integer values promoted to float.
    if let Ok(v) = zerompk::from_msgpack::<i64>(bytes) {
        return Ok(v as f64);
    }
    if let Ok(v) = zerompk::from_msgpack::<u64>(bytes) {
        return Ok(v as f64);
    }
    // If value is a map, find the first numeric field.
    if let Ok(nodedb_types::Value::Object(map)) = nodedb_types::value_from_msgpack(bytes) {
        for (k, v) in &map {
            if k == "key" {
                continue;
            }
            match v {
                nodedb_types::Value::Float(f) => return Ok(*f),
                nodedb_types::Value::Integer(i) => return Ok(*i as f64),
                _ => {}
            }
        }
    }
    Err(AtomicError::TypeMismatch {
        detail: "value is not numeric".into(),
    })
}

/// Encode an `i64` as MessagePack, wrapping the (practically unreachable, but
/// not type-system-excluded) encode failure in [`AtomicError::Encode`] rather
/// than panicking.
fn encode_i64(v: i64) -> Result<Vec<u8>, AtomicError> {
    zerompk::to_msgpack_vec(&v).map_err(|e| AtomicError::Encode {
        detail: format!("i64 re-encode: {e}"),
    })
}

/// Encode an `f64` as MessagePack, same rationale as [`encode_i64`].
fn encode_f64(v: f64) -> Result<Vec<u8>, AtomicError> {
    zerompk::to_msgpack_vec(&v).map_err(|e| AtomicError::Encode {
        detail: format!("f64 re-encode: {e}"),
    })
}

/// Compute the new value for `INCR`, given the current raw bytes (if
/// any). Returns `(new_i64, new_bytes)` -- mirrors the typed-map /
/// plain-i64 branches of [`super::engine_atomic::KvEngine::incr`] exactly.
pub fn incr(current: Option<&[u8]>, delta: i64) -> Result<(i64, Vec<u8>), AtomicError> {
    let old_i64 = match current {
        None => 0i64,
        Some(bytes) => decode_msgpack_i64(bytes)?,
    };
    let new_i64 = old_i64.checked_add(delta).ok_or(AtomicError::Overflow)?;

    // If value is a map (typed KV entry), update the numeric field in-place.
    let new_bytes = if let Some(cur) = current
        && let Ok(nodedb_types::Value::Object(mut map)) = nodedb_types::value_from_msgpack(cur)
        && map.len() > 1
    {
        let mut updated = false;
        for (k, v) in map.iter_mut() {
            if k == "key" {
                continue;
            }
            if matches!(
                v,
                nodedb_types::Value::Integer(_) | nodedb_types::Value::Float(_)
            ) {
                *v = nodedb_types::Value::Integer(new_i64);
                updated = true;
                break;
            }
        }
        if updated {
            match nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map)) {
                Ok(bytes) => bytes,
                Err(_) => encode_i64(new_i64)?,
            }
        } else {
            encode_i64(new_i64)?
        }
    } else {
        encode_i64(new_i64)?
    };
    Ok((new_i64, new_bytes))
}

/// Compute the new value for `INCR_FLOAT`. Returns `(new_f64, new_bytes)`
/// -- mirrors [`super::engine_atomic::KvEngine::incr_float`] exactly (no
/// typed-map branch: float counters are always stored as a bare f64).
pub fn incr_float(current: Option<&[u8]>, delta: f64) -> Result<(f64, Vec<u8>), AtomicError> {
    let old_f64 = match current {
        None => 0.0f64,
        Some(bytes) => decode_msgpack_f64(bytes)?,
    };
    let new_f64 = old_f64 + delta;
    if new_f64.is_nan() || new_f64.is_infinite() {
        return Err(AtomicError::Overflow);
    }
    let new_bytes = encode_f64(new_f64)?;
    Ok((new_f64, new_bytes))
}

/// Compute the CAS outcome: whether `expected` matches the current value
/// (with the typed-map first-string-field fallback), and the bytes to
/// write when it does. Mirrors [`super::engine_atomic::KvEngine::cas`]
/// exactly.
pub fn cas(current: Option<&[u8]>, expected: &[u8], new_value: &[u8]) -> (bool, Vec<u8>) {
    let matches = match current {
        None => expected.is_empty(),
        Some(v) => {
            if v == expected {
                true
            } else if let Ok(nodedb_types::Value::Object(map)) = nodedb_types::value_from_msgpack(v)
            {
                let expected_str = String::from_utf8_lossy(expected);
                map.iter().any(|(k, val)| {
                    k != "key"
                        && matches!(val, nodedb_types::Value::String(s) if s == expected_str.as_ref())
                })
            } else {
                false
            }
        }
    };

    if !matches {
        return (false, Vec::new());
    }

    let write_bytes = if let Some(cur) = current
        && let Ok(nodedb_types::Value::Object(mut map)) = nodedb_types::value_from_msgpack(cur)
        && map.len() > 1
    {
        let new_str = String::from_utf8_lossy(new_value).to_string();
        let mut updated = false;
        for (k, v) in map.iter_mut() {
            if k == "key" {
                continue;
            }
            if matches!(v, nodedb_types::Value::String(_)) {
                *v = nodedb_types::Value::String(new_str.clone());
                updated = true;
                break;
            }
        }
        if updated {
            nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map))
                .unwrap_or_else(|_| new_value.to_vec())
        } else {
            new_value.to_vec()
        }
    } else {
        new_value.to_vec()
    };
    (true, write_bytes)
}

/// Compute the bytes to write for `GETSET`. Mirrors
/// [`super::engine_atomic::KvEngine::getset`] exactly (typed-map
/// first-string-field update, or a plain overwrite).
pub fn getset(current: Option<&[u8]>, new_value: &[u8]) -> Vec<u8> {
    if let Some(cur) = current
        && let Ok(nodedb_types::Value::Object(mut map)) = nodedb_types::value_from_msgpack(cur)
        && map.len() > 1
    {
        let new_str = String::from_utf8_lossy(new_value).to_string();
        let mut updated = false;
        for (k, v) in map.iter_mut() {
            if k == "key" {
                continue;
            }
            if matches!(v, nodedb_types::Value::String(_)) {
                *v = nodedb_types::Value::String(new_str.clone());
                updated = true;
                break;
            }
        }
        if updated {
            nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map))
                .unwrap_or_else(|_| new_value.to_vec())
        } else {
            new_value.to_vec()
        }
    } else {
        new_value.to_vec()
    }
}
