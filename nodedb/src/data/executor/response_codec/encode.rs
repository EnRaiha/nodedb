// SPDX-License-Identifier: BUSL-1.1

//! Generic encoders for Data Plane response payloads, plus the
//! `decode_payload` / `decode_payload_to_json` counterparts used at the Control
//! Plane boundary.
//!
//! # Every encoder here emits MessagePack
//!
//! Including the ones whose names mention JSON: `encode_json_as_msgpack` and
//! `encode_json_vec_as_msgpack` are named for the `serde_json::Value` they
//! TAKE, never for what they produce. A Control-Plane caller that hands these
//! bytes to a JSON parser gets a parse failure on the first byte, and a caller
//! that then defaults the failure away (`unwrap_or_default`, `if let Ok`)
//! reports a successful empty result for every query — which is silent data
//! loss, not a degraded mode. Read a payload back with [`decode_payload`] (or
//! [`decode_payload_to_json`] for the text form); nothing else is a correct
//! counterpart.

use serde::de::DeserializeOwned;

/// Serialize a response payload as MessagePack bytes.
///
/// Drop-in replacement for `serde_json::to_vec(&value)` in handler code.
/// Returns MessagePack bytes that are 30-50% smaller and 2-3x faster to
/// produce than JSON. Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode<T: zerompk::ToMessagePack>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a `serde_json::Value` payload as MessagePack bytes.
///
/// Named for its INPUT: the output is MessagePack, like every encoder in this
/// module. Read back with [`decode_payload`] / [`decode_payload_to_json`].
pub(in crate::data::executor) fn encode_json_as_msgpack(
    value: &serde_json::Value,
) -> crate::Result<Vec<u8>> {
    nodedb_types::json_to_msgpack(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode any `Serialize` type as MessagePack bytes.
///
/// Serializes via serde to an intermediate `serde_json::Value`, then converts
/// to MessagePack. Use `encode()` for types that implement `ToMessagePack`
/// directly (faster, no intermediate). Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode_serde<T: serde::Serialize>(
    value: &T,
) -> crate::Result<Vec<u8>> {
    let json_value = serde_json::to_value(value).map_err(|e| crate::Error::Codec {
        detail: format!("serde serialization: {e}"),
    })?;
    encode_json_as_msgpack(&json_value)
}

/// Encode a slice of `serde_json::Value` rows as MessagePack bytes.
///
/// The name carries `as_msgpack` because the old one (`encode_json_vec`) read
/// as "encode to JSON" and was taken that way by four separate Control-Plane
/// decoders, each of which parsed these bytes as JSON, failed, and defaulted
/// the failure into an empty row set. Read back with [`decode_payload`].
pub(in crate::data::executor) fn encode_json_vec_as_msgpack(
    values: &[serde_json::Value],
) -> crate::Result<Vec<u8>> {
    let wrapped: Vec<nodedb_types::JsonValue> = values
        .iter()
        .map(|v| nodedb_types::JsonValue(v.clone()))
        .collect();
    zerompk::to_msgpack_vec(&wrapped).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a slice of `nodedb_types::Value` as a msgpack array.
///
/// No JSON intermediary — values are serialized directly to standard msgpack.
pub(in crate::data::executor) fn encode_value_vec(
    values: &[nodedb_types::Value],
) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(values.len() * 64);
    let n = values.len();
    if n <= 15 {
        buf.push(0x90 | n as u8);
    } else if n <= 0xFFFF {
        buf.push(0xDC);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for val in values {
        let encoded = nodedb_types::value_to_msgpack(val).map_err(|e| crate::Error::Codec {
            detail: format!("value serialization: {e}"),
        })?;
        buf.extend_from_slice(&encoded);
    }
    Ok(buf)
}

/// Encode a simple `{"key": count}` response (for insert confirmations).
pub(in crate::data::executor) fn encode_count(key: &str, count: usize) -> crate::Result<Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(key, count);
    zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Codec {
        detail: format!("count response serialization: {e}"),
    })
}

/// Deserialize a Data-Plane response payload into `T`.
///
/// THE counterpart to the encoders above, and the only correct way for a
/// Control-Plane caller to read one of their payloads back. Every encoder in
/// this module emits MessagePack — including `encode_json_as_msgpack` and
/// `encode_json_vec_as_msgpack`, which are named for the `serde_json::Value`
/// they take — so a bare `sonic_rs::from_slice` / `serde_json::from_slice` on
/// these bytes fails on the first byte, every time.
///
/// An empty payload yields `T::default()`: a handler with nothing to report
/// sends no bytes, and for the row-shaped `T`s these callers use that is an
/// empty result, not a failure. A NON-empty payload that will not deserialize
/// is an error, never a default — that is the distinction whose absence turned
/// four decode bugs into silently empty answers instead of loud ones.
pub fn decode_payload<T: DeserializeOwned + Default>(payload: &[u8]) -> crate::Result<T> {
    if payload.is_empty() {
        return Ok(T::default());
    }
    let text = decode_payload_to_json(payload);
    sonic_rs::from_str(&text).map_err(|e| crate::Error::Codec {
        detail: format!("response payload could not be decoded: {e}"),
    })
}

/// Decode a MessagePack or JSON payload to a JSON string for pgwire/HTTP output.
///
/// Auto-detects format: if first byte indicates MessagePack, transcodes directly
/// to JSON text via streaming transcoder (no intermediate `serde_json::Value`).
/// If already JSON (starts with `[` or `{`), returns as-is.
pub fn decode_payload_to_json(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }

    let first = payload[0];

    let is_likely_json = first == b'['
        || first == b'{'
        || first == b'"'
        || first.is_ascii_digit()
        || first == b't'
        || first == b'f'
        || first == b'n';

    if is_likely_json {
        return String::from_utf8_lossy(payload).into_owned();
    }

    nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned())
}
