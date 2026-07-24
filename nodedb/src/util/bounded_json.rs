// SPDX-License-Identifier: BUSL-1.1

//! Bounded JSON decoding for values received from external clients.

use std::fmt;

use serde::de::DeserializeOwned;

/// Largest JSON document accepted at an external input boundary.
pub const MAX_JSON_BYTES: usize = 8 * 1024 * 1024;
/// Maximum structural object/array nesting accepted at an external input boundary.
pub const MAX_JSON_DEPTH: usize = 64;

/// Failure while validating or decoding untrusted JSON.
#[derive(Debug)]
pub struct BoundedJsonError {
    detail: &'static str,
}

impl BoundedJsonError {
    const fn new(detail: &'static str) -> Self {
        Self { detail }
    }
}

impl fmt::Display for BoundedJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "bounded JSON decode failed: {}", self.detail)
    }
}

impl std::error::Error for BoundedJsonError {}

/// Validate and decode JSON received over an external byte-oriented protocol.
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, BoundedJsonError> {
    from_slice_with_limits(bytes, MAX_JSON_BYTES, MAX_JSON_DEPTH)
}

/// Validate and decode JSON received over an external text-oriented protocol.
pub fn from_str<T: DeserializeOwned>(text: &str) -> Result<T, BoundedJsonError> {
    from_slice(text.as_bytes())
}

fn from_slice_with_limits<T: DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
    max_depth: usize,
) -> Result<T, BoundedJsonError> {
    if bytes.len() > max_bytes {
        return Err(BoundedJsonError::new("input exceeds byte limit"));
    }
    validate_structure(bytes, max_depth)?;
    sonic_rs::from_slice(bytes).map_err(|_| BoundedJsonError::new("invalid JSON value"))
}

/// Scan only JSON's structural syntax before passing the document to sonic.
///
/// This deliberately does not duplicate JSON grammar validation. It establishes
/// a finite structural nesting bound while treating all string contents,
/// including escaped quotes and brackets, as opaque. sonic performs the exact
/// JSON parse after this preflight succeeds.
fn validate_structure(bytes: &[u8], max_depth: usize) -> Result<(), BoundedJsonError> {
    let mut stack = Vec::with_capacity(max_depth.min(64));
    let mut in_string = false;
    let mut escaped = false;

    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' => push_container(&mut stack, b'}', max_depth)?,
            b'[' => push_container(&mut stack, b']', max_depth)?,
            closing @ (b'}' | b']') if stack.pop() != Some(closing) => {
                return Err(BoundedJsonError::new("mismatched JSON container"));
            }
            b'}' | b']' => {}
            _ => {}
        }
    }

    if in_string {
        return Err(BoundedJsonError::new("unterminated JSON string"));
    }
    if !stack.is_empty() {
        return Err(BoundedJsonError::new("unterminated JSON container"));
    }
    Ok(())
}

fn push_container(
    stack: &mut Vec<u8>,
    closing: u8,
    max_depth: usize,
) -> Result<(), BoundedJsonError> {
    if stack.len() >= max_depth {
        return Err(BoundedJsonError::new("nesting exceeds depth limit"));
    }
    stack.push(closing);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maximum_depth_is_accepted() {
        let mut json = "[".repeat(MAX_JSON_DEPTH);
        json.push('0');
        json.push_str(&"]".repeat(MAX_JSON_DEPTH));
        assert!(from_str::<serde_json::Value>(&json).is_ok());
    }

    #[test]
    fn depth_above_limit_is_rejected() {
        let mut json = "[".repeat(MAX_JSON_DEPTH + 1);
        json.push('0');
        json.push_str(&"]".repeat(MAX_JSON_DEPTH + 1));
        assert!(from_str::<serde_json::Value>(&json).is_err());
    }

    #[test]
    fn brackets_and_escaped_quotes_inside_strings_are_opaque() {
        let json = r#"{"value":"[{\"not a container\"}]"}"#;
        assert!(from_str::<serde_json::Value>(json).is_ok());
    }

    #[test]
    fn utf8_and_even_backslash_escape_parity_are_preserved() {
        let json = r#"{"text":"雪\\","array":[]}"#;
        let value = from_str::<serde_json::Value>(json).expect("valid bounded JSON");
        assert_eq!(value["text"], "雪\\");
        assert!(from_slice::<serde_json::Value>(&[b'"', 0xff, b'"']).is_err());
    }

    #[test]
    fn malformed_structure_is_rejected_before_json_parse() {
        for json in [
            b"{]".as_slice(),
            b"{\"a\": 1".as_slice(),
            b"\"unterminated".as_slice(),
        ] {
            assert!(from_slice::<serde_json::Value>(json).is_err());
        }
    }

    #[test]
    fn byte_limit_is_enforced_before_decode() {
        assert!(from_slice_with_limits::<serde_json::Value>(b"[]", 1, MAX_JSON_DEPTH).is_err());
        assert!(from_slice_with_limits::<serde_json::Value>(b"[]", 2, MAX_JSON_DEPTH).is_ok());
    }
}
