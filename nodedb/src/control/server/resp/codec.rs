// SPDX-License-Identifier: BUSL-1.1

//! RESP2 protocol codec: frame parser and response serializer.
//!
//! Implements the Redis Serialization Protocol (RESP2) as specified at
//! <https://redis.io/docs/reference/protocol-spec/>.
//!
//! Supports:
//! - Inline commands (plain text, newline-terminated)
//! - Bulk string arrays (the standard client command format)
//! - All five RESP2 response types

use std::io;

/// Maximum number of bytes buffered for one RESP frame.
pub(crate) const MAX_RESP_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Maximum payload size of a RESP bulk string.
pub(crate) const MAX_BULK_STRING_BYTES: usize = 8 * 1024 * 1024;
/// Maximum length of a newline-terminated inline command.
pub(crate) const MAX_INLINE_COMMAND_BYTES: usize = 64 * 1024;
/// Maximum number of elements in a RESP array.
pub(crate) const MAX_ARRAY_CARDINALITY: usize = 1024;
/// Maximum nesting depth of RESP arrays.
pub(crate) const MAX_NESTING_DEPTH: usize = 16;

/// A parsed RESP value (command or nested element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+OK\r\n`
    SimpleString(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n` or `$-1\r\n` (nil)
    BulkString(Option<Vec<u8>>),
    /// `*N\r\n...` (array of values)
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Create a simple string response.
    pub fn ok() -> Self {
        Self::SimpleString("OK".into())
    }

    /// Create a nil bulk string.
    pub fn nil() -> Self {
        Self::BulkString(None)
    }

    /// Create a nil array.
    pub fn nil_array() -> Self {
        Self::Array(None)
    }

    /// Create an error response.
    pub fn err(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    /// Create an error response from a typed error.
    ///
    /// Routes through the canonical error→RESP mapping so a refusal reaches the
    /// client as the code it actually is (`NOPERM` for an authorization or
    /// policy denial, `MOVED`, `TIMEOUT`, `BUSY`, …) instead of a generic `ERR`
    /// that reads like a server fault. `Error::Bridge` already carries a
    /// RESP-shaped detail (the gateway mapping, or the local `BUSY` retry
    /// hint), so it is passed through rather than mapped twice.
    pub fn from_error(error: &crate::Error) -> Self {
        match error {
            crate::Error::Bridge { detail } => Self::Error(detail.clone()),
            other => Self::Error(crate::control::gateway::GatewayErrorMap::to_resp(other)),
        }
    }

    /// Create an integer response.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create a bulk string from bytes.
    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(Some(data.into()))
    }

    /// Create a bulk string from a str.
    pub fn bulk_str(s: &str) -> Self {
        Self::BulkString(Some(s.as_bytes().to_vec()))
    }

    /// Create an array of values.
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(Some(items))
    }

    /// Serialize this value to RESP2 wire format.
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Self::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Self::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.serialize(buf);
                }
            }
        }
    }

    /// Serialize to a new Vec.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.serialize(&mut buf);
        buf
    }
}

/// RESP2 frame parser.
///
/// Incrementally parses RESP frames from a byte buffer. Returns `Ok(Some(value))`
/// when a complete frame is available, `Ok(None)` when more data is needed.
pub struct RespParser {
    buf: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    /// Feed bytes from the network into the parser buffer.
    pub fn feed(&mut self, data: &[u8]) -> io::Result<()> {
        let buffered = self
            .buf
            .len()
            .checked_add(data.len())
            .ok_or_else(|| protocol_error("RESP frame size overflow"))?;
        if buffered > MAX_RESP_FRAME_BYTES {
            return Err(protocol_error("RESP frame exceeds maximum size"));
        }
        self.buf.extend_from_slice(data);
        Ok(())
    }

    /// Try to parse a complete RESP frame from the buffer.
    ///
    /// Returns `Ok(Some(value))` if a complete frame was parsed (consumed from buffer).
    /// Returns `Ok(None)` if more data is needed.
    /// Returns `Err` on protocol errors.
    pub fn try_parse(&mut self) -> io::Result<Option<RespValue>> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        match self.buf[0] {
            b'+' | b'-' | b':' | b'$' | b'*' => {
                let (value, consumed) = match parse_value(&self.buf, 0) {
                    Ok(Some((v, n))) => (v, n),
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(e),
                };
                self.buf.drain(..consumed);
                Ok(Some(value))
            }
            // Inline command: plain text terminated by \r\n or \n.
            _ => {
                if let Some(pos) = find_line_end(&self.buf) {
                    if pos > MAX_INLINE_COMMAND_BYTES {
                        return Err(protocol_error("RESP inline command exceeds maximum length"));
                    }
                    let line = &self.buf[..pos];
                    let parts = parse_inline_command(line)?;
                    let consumed = if self.buf.get(checked_add(pos, 1)?) == Some(&b'\n') {
                        checked_add(pos, 2)?
                    } else {
                        checked_add(pos, 1)?
                    };
                    self.buf.drain(..consumed);
                    Ok(Some(parts))
                } else if self.buf.len() > MAX_INLINE_COMMAND_BYTES {
                    Err(protocol_error("RESP inline command exceeds maximum length"))
                } else {
                    Ok(None) // Need more data.
                }
            }
        }
    }

    /// Number of bytes in the parser buffer (for backpressure).
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Internal parsing
// ---------------------------------------------------------------------------

/// Find the position of `\r\n` or `\n` in the buffer.
fn find_line_end(buf: &[u8]) -> Option<usize> {
    let i = buf.iter().position(|&b| b == b'\n')?;
    Some(if i > 0 && buf[i - 1] == b'\r' {
        i - 1
    } else {
        i
    })
}

/// Find `\r\n` and return the position of `\r`. Returns None if not found.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn protocol_error(detail: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail.into())
}

fn checked_add(offset: usize, amount: usize) -> io::Result<usize> {
    offset
        .checked_add(amount)
        .ok_or_else(|| protocol_error("RESP frame offset overflow"))
}

fn with_type_byte(value: RespValue, consumed: usize) -> io::Result<(RespValue, usize)> {
    Ok((value, checked_add(consumed, 1)?))
}

/// Parse a RESP value starting at `buf[0]`. Returns `(value, bytes_consumed)`.
fn parse_value(buf: &[u8], depth: usize) -> io::Result<Option<(RespValue, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(&buf[1..])?
            .map(|(s, n)| with_type_byte(RespValue::SimpleString(s), n))
            .transpose(),
        b'-' => parse_simple_string(&buf[1..])?
            .map(|(s, n)| with_type_byte(RespValue::Error(s), n))
            .transpose(),
        b':' => parse_integer(&buf[1..])?
            .map(|(i, n)| with_type_byte(RespValue::Integer(i), n))
            .transpose(),
        b'$' => parse_bulk_string(&buf[1..])?
            .map(|(v, n)| with_type_byte(v, n))
            .transpose(),
        b'*' => parse_array(&buf[1..], depth)?
            .map(|(v, n)| with_type_byte(v, n))
            .transpose(),
        other => Err(protocol_error(format!(
            "unexpected RESP type byte: 0x{other:02x}"
        ))),
    }
}

fn parse_simple_string(buf: &[u8]) -> io::Result<Option<(String, usize)>> {
    match find_crlf(buf) {
        Some(pos) => {
            let s = std::str::from_utf8(&buf[..pos])
                .map_err(|e| protocol_error(e.to_string()))?
                .to_owned();
            Ok(Some((s, checked_add(pos, 2)?)))
        }
        None => Ok(None),
    }
}

fn parse_integer(buf: &[u8]) -> io::Result<Option<(i64, usize)>> {
    match find_crlf(buf) {
        Some(pos) => {
            let s = std::str::from_utf8(&buf[..pos]).map_err(|e| protocol_error(e.to_string()))?;
            let n: i64 = s
                .parse::<i64>()
                .map_err(|e| protocol_error(e.to_string()))?;
            Ok(Some((n, checked_add(pos, 2)?)))
        }
        None => Ok(None),
    }
}

fn parse_bulk_string(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    let crlf_pos = match find_crlf(buf) {
        Some(p) => p,
        None => return Ok(None),
    };

    let len_str =
        std::str::from_utf8(&buf[..crlf_pos]).map_err(|e| protocol_error(e.to_string()))?;
    let len: i64 = len_str
        .parse::<i64>()
        .map_err(|e| protocol_error(e.to_string()))?;

    if len < 0 {
        return Ok(Some((RespValue::nil(), checked_add(crlf_pos, 2)?)));
    }

    let len = usize::try_from(len)
        .map_err(|_| protocol_error("RESP bulk string length exceeds usize"))?;
    if len > MAX_BULK_STRING_BYTES {
        return Err(protocol_error("RESP bulk string exceeds maximum size"));
    }
    let data_start = checked_add(crlf_pos, 2)?;
    let data_end = checked_add(data_start, len)?;
    let frame_end = checked_add(data_end, 2)?; // trailing \r\n

    if buf.len() < frame_end {
        return Ok(None); // Need more data.
    }
    if &buf[data_end..frame_end] != b"\r\n" {
        return Err(protocol_error("RESP bulk string is missing trailing CRLF"));
    }

    let data = buf[data_start..data_end].to_vec();
    Ok(Some((RespValue::BulkString(Some(data)), frame_end)))
}

fn parse_array(buf: &[u8], depth: usize) -> io::Result<Option<(RespValue, usize)>> {
    if depth >= MAX_NESTING_DEPTH {
        return Err(protocol_error("RESP nesting depth exceeds maximum"));
    }

    let crlf_pos = match find_crlf(buf) {
        Some(p) => p,
        None => return Ok(None),
    };

    let count_str =
        std::str::from_utf8(&buf[..crlf_pos]).map_err(|e| protocol_error(e.to_string()))?;
    let count: i64 = count_str
        .parse::<i64>()
        .map_err(|e| protocol_error(e.to_string()))?;

    if count < 0 {
        return Ok(Some((RespValue::nil_array(), checked_add(crlf_pos, 2)?)));
    }

    let count =
        usize::try_from(count).map_err(|_| protocol_error("RESP array count exceeds usize"))?;
    if count > MAX_ARRAY_CARDINALITY {
        return Err(protocol_error("RESP array cardinality exceeds maximum"));
    }
    let mut offset = checked_add(crlf_pos, 2)?;
    let minimum_bytes = count
        .checked_mul(3)
        .ok_or_else(|| protocol_error("RESP array count overflow"))?;
    if buf.len().saturating_sub(offset) < minimum_bytes {
        return Ok(None);
    }
    let mut items = Vec::new();

    for _ in 0..count {
        match parse_value(&buf[offset..], checked_add(depth, 1)?)? {
            Some((value, consumed)) => {
                items.push(value);
                offset = checked_add(offset, consumed)?;
            }
            None => return Ok(None), // Need more data.
        }
    }

    Ok(Some((RespValue::Array(Some(items)), offset)))
}

/// Parse an inline command (plain text) into a RESP array of bulk strings.
fn parse_inline_command(line: &[u8]) -> io::Result<RespValue> {
    let text = std::str::from_utf8(line).map_err(|e| protocol_error(e.to_string()))?;
    let parts: Vec<RespValue> = text
        .split_whitespace()
        .map(|s| RespValue::bulk(s.as_bytes().to_vec()))
        .collect();
    Ok(RespValue::Array(Some(parts)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_simple_string() {
        assert_eq!(RespValue::ok().to_bytes(), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        assert_eq!(
            RespValue::err("ERR unknown command").to_bytes(),
            b"-ERR unknown command\r\n"
        );
    }

    #[test]
    fn serialize_integer() {
        assert_eq!(RespValue::integer(42).to_bytes(), b":42\r\n");
        assert_eq!(RespValue::integer(-1).to_bytes(), b":-1\r\n");
    }

    #[test]
    fn serialize_bulk_string() {
        assert_eq!(RespValue::bulk_str("hello").to_bytes(), b"$5\r\nhello\r\n");
        assert_eq!(RespValue::nil().to_bytes(), b"$-1\r\n");
    }

    #[test]
    fn serialize_array() {
        let arr = RespValue::array(vec![
            RespValue::bulk_str("SET"),
            RespValue::bulk_str("key"),
            RespValue::bulk_str("value"),
        ]);
        assert_eq!(
            arr.to_bytes(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn parse_bulk_string_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n").unwrap();

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::bulk(b"GET".to_vec()));
                assert_eq!(items[1], RespValue::bulk(b"mykey".to_vec()));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn parse_inline_command() {
        let mut parser = RespParser::new();
        parser.feed(b"PING\r\n").unwrap();

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], RespValue::bulk(b"PING".to_vec()));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_utf8_simple_string() {
        let mut parser = RespParser::new();
        parser.feed(b"+\xff\r\n").unwrap();

        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_invalid_utf8_error_string() {
        let mut parser = RespParser::new();
        parser.feed(b"-\xff\r\n").unwrap();

        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_invalid_utf8_inline_command() {
        let mut parser = RespParser::new();
        parser.feed(b"PING \xff\r\n").unwrap();

        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_bulk_string_without_trailing_crlf() {
        let mut parser = RespParser::new();
        parser.feed(b"$3\r\nabcXX").unwrap();

        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_invalid_bulk_delimiter_without_desynchronizing_pipeline() {
        let frame = b"$3\r\nabcXX+OK\r\n";
        let mut parser = RespParser::new();
        parser.feed(frame).unwrap();

        assert!(parser.try_parse().is_err());
        assert_eq!(parser.buffered(), frame.len());
    }

    #[test]
    fn parse_set_command() {
        let mut parser = RespParser::new();
        parser
            .feed(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .unwrap();

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn rejects_declared_oversized_bulk_and_array() {
        let mut bulk_parser = RespParser::new();
        bulk_parser
            .feed(format!("${}\r\n", MAX_BULK_STRING_BYTES + 1).as_bytes())
            .unwrap();
        assert!(bulk_parser.try_parse().is_err());

        let mut array_parser = RespParser::new();
        array_parser
            .feed(format!("*{}\r\n", MAX_ARRAY_CARDINALITY + 1).as_bytes())
            .unwrap();
        assert!(array_parser.try_parse().is_err());
    }

    #[test]
    fn rejects_chunked_total_frame_overflow() {
        let mut parser = RespParser::new();
        parser.feed(&vec![b'x'; MAX_RESP_FRAME_BYTES - 1]).unwrap();
        parser.feed(b"x").unwrap();
        assert_eq!(parser.buffered(), MAX_RESP_FRAME_BYTES);
        assert!(parser.feed(b"x").is_err());
    }

    #[test]
    fn rejects_oversized_inline_command_without_newline() {
        let mut parser = RespParser::new();
        parser
            .feed(&vec![b'x'; MAX_INLINE_COMMAND_BYTES + 1])
            .unwrap();
        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_excessive_array_nesting() {
        let mut frame = Vec::new();
        for _ in 0..=MAX_NESTING_DEPTH {
            frame.extend_from_slice(b"*1\r\n");
        }
        frame.extend_from_slice(b"+ok\r\n");

        let mut parser = RespParser::new();
        parser.feed(&frame).unwrap();
        assert!(parser.try_parse().is_err());
    }

    #[test]
    fn rejects_arithmetic_hostile_declarations() {
        for declaration in [
            b"$9223372036854775807\r\n".as_slice(),
            b"*9223372036854775807\r\n",
        ] {
            let mut parser = RespParser::new();
            parser.feed(declaration).unwrap();
            assert!(parser.try_parse().is_err());
        }
    }

    #[test]
    fn parse_incomplete_returns_none() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$5\r\nmy").unwrap(); // Incomplete.

        assert!(parser.try_parse().unwrap().is_none());

        // Feed the rest.
        parser.feed(b"key\r\n").unwrap();
        assert!(parser.try_parse().unwrap().is_some());
    }

    #[test]
    fn parse_nil_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n").unwrap();

        let result = parser.try_parse().unwrap().unwrap();
        assert_eq!(result, RespValue::nil());
    }

    #[test]
    fn parse_multiple_commands() {
        let mut parser = RespParser::new();
        parser
            .feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n")
            .unwrap();

        let first = parser.try_parse().unwrap().unwrap();
        assert!(matches!(first, RespValue::Array(Some(ref v)) if v.len() == 1));

        let second = parser.try_parse().unwrap().unwrap();
        assert!(matches!(second, RespValue::Array(Some(ref v)) if v.len() == 2));
    }

    #[test]
    fn roundtrip() {
        let values = vec![
            RespValue::ok(),
            RespValue::err("ERR test"),
            RespValue::integer(99),
            RespValue::bulk_str("hello"),
            RespValue::nil(),
            RespValue::array(vec![RespValue::bulk_str("a"), RespValue::integer(1)]),
        ];

        for original in &values {
            let bytes = original.to_bytes();
            let mut parser = RespParser::new();
            parser.feed(&bytes).unwrap();
            let parsed = parser.try_parse().unwrap().unwrap();
            assert_eq!(&parsed, original, "roundtrip failed for {original:?}");
        }
    }
}
