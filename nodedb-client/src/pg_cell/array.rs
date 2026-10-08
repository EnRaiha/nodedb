// SPDX-License-Identifier: Apache-2.0

//! Decoder for the PostgreSQL text form of an array: `{1.5,NaN,NULL}`.
//!
//! The server sends array columns in text format. Each dimension is a
//! `{...}` list of comma-separated elements. An element is a nested list, a
//! double-quoted string with backslash escapes, the unquoted word `NULL`, or
//! unquoted text. Every element decodes through the text decoder of the
//! element type, so a `float4[]` element reads `NaN`, `Infinity` and
//! `-Infinity` the way a `float4` cell does.

use nodedb_types::Value;
use tokio_postgres::types::Type;

use super::error::DecodeReason;
use super::text;

/// Decode `text` as an array literal whose elements have PG type `element`.
/// A multi-dimensional array decodes as nested `Value::Array`s.
pub(super) fn array(text: &str, element: &Type) -> Result<Value, DecodeReason> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        pos: 0,
        index: 0,
        element,
    };
    let value = parser.list()?;
    parser.skip_whitespace();
    if parser.pos != parser.bytes.len() {
        return Err(DecodeReason::ArraySyntax("text after the closing brace"));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
    /// Position of the next element in flattened order, for errors.
    index: usize,
    element: &'a Type,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    /// One `{...}` list, nested lists included.
    fn list(&mut self) -> Result<Value, DecodeReason> {
        self.skip_whitespace();
        if self.peek() != Some(b'{') {
            return Err(DecodeReason::ArraySyntax("a list must start with '{'"));
        }
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Array(items));
        }
        loop {
            self.skip_whitespace();
            let item = match self.peek() {
                Some(b'{') => self.list()?,
                Some(b'"') => {
                    let quoted = self.quoted()?;
                    self.decode(quoted)?
                }
                _ => self.unquoted()?,
            };
            items.push(item);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Value::Array(items));
                }
                Some(_) => return Err(DecodeReason::ArraySyntax("expected ',' or '}'")),
                None => return Err(DecodeReason::ArraySyntax("missing closing brace")),
            }
        }
    }

    /// A double-quoted element. A backslash takes the next byte literally.
    fn quoted(&mut self) -> Result<String, DecodeReason> {
        // Skip the opening quote.
        self.pos += 1;
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => return Err(DecodeReason::ArraySyntax("unterminated quoted element")),
                Some(b'"') => {
                    self.pos += 1;
                    break;
                }
                Some(b'\\') => {
                    let escaped = self
                        .bytes
                        .get(self.pos + 1)
                        .copied()
                        .ok_or(DecodeReason::ArraySyntax("dangling backslash"))?;
                    out.push(escaped);
                    self.pos += 2;
                }
                Some(byte) => {
                    out.push(byte);
                    self.pos += 1;
                }
            }
        }
        // Only ASCII quotes and backslashes are removed from UTF-8 input, so
        // the rest stays UTF-8.
        String::from_utf8(out).map_err(|_| DecodeReason::NotUtf8)
    }

    /// An unquoted element: the text up to the next `,` or `}`, with
    /// surrounding whitespace removed. The word `NULL` in any case is SQL
    /// NULL.
    fn unquoted(&mut self) -> Result<Value, DecodeReason> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| !matches!(b, b',' | b'}' | b'{' | b'"'))
        {
            self.pos += 1;
        }
        let raw = self.bytes.get(start..self.pos).unwrap_or_default();
        let word = std::str::from_utf8(raw)
            .map_err(|_| DecodeReason::NotUtf8)?
            .trim();
        if word.is_empty() {
            return Err(DecodeReason::ArraySyntax("empty element"));
        }
        if word.eq_ignore_ascii_case("NULL") {
            self.index += 1;
            return Ok(Value::Null);
        }
        self.decode(word.to_owned())
    }

    /// Decode one element's text as the element type.
    fn decode(&mut self, element_text: String) -> Result<Value, DecodeReason> {
        let index = self.index;
        self.index += 1;
        text::scalar(self.element, &element_text).map_err(|source| DecodeReason::Element {
            index,
            text: element_text,
            element_type: self.element.name().to_owned(),
            source: Box::new(source),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floats(values: &[f64]) -> Value {
        Value::Array(values.iter().copied().map(Value::Float).collect())
    }

    #[test]
    fn decodes_float_arrays_including_non_finite() {
        assert_eq!(
            array("{1.5,-2,Infinity,-Infinity}", &Type::FLOAT8),
            Ok(floats(&[1.5, -2.0, f64::INFINITY, f64::NEG_INFINITY]))
        );
        let Ok(Value::Array(items)) = array("{NaN, 0.1}", &Type::FLOAT4) else {
            panic!("a float4 array decodes as an Array");
        };
        assert!(matches!(items.first(), Some(Value::Float(f)) if f.is_nan()));
        assert_eq!(items.get(1), Some(&Value::Float(f64::from(0.1f32))));
        assert_eq!(array("{}", &Type::FLOAT8), Ok(Value::Array(Vec::new())));
    }

    #[test]
    fn decodes_nulls_quotes_and_nesting() {
        assert_eq!(
            array(r#"{"a,b",NULL,"say \"hi\"",null}"#, &Type::TEXT),
            Ok(Value::Array(vec![
                Value::String("a,b".into()),
                Value::Null,
                Value::String("say \"hi\"".into()),
                Value::Null,
            ]))
        );
        assert_eq!(
            array(r#"{"NULL"}"#, &Type::TEXT),
            Ok(Value::Array(vec![Value::String("NULL".into())])),
            "a quoted NULL is the string"
        );
        assert_eq!(
            array("{{1,2},{3,4}}", &Type::INT4),
            Ok(Value::Array(vec![
                Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
                Value::Array(vec![Value::Integer(3), Value::Integer(4)]),
            ]))
        );
    }

    #[test]
    fn names_the_element_that_does_not_decode() {
        assert_eq!(
            array("{1.5,abc}", &Type::FLOAT8),
            Err(DecodeReason::Element {
                index: 1,
                text: "abc".into(),
                element_type: "float8".into(),
                source: Box::new(DecodeReason::Invalid { expected: "float8" }),
            })
        );
    }

    #[test]
    fn refuses_malformed_literals() {
        for text in [
            "[1.5,2]",
            "1.5,2",
            "{1.5,2",
            "{1.5,,2}",
            "{1.5} x",
            "{\"open}",
            "{1.5 2.5}",
        ] {
            assert!(
                array(text, &Type::FLOAT8).is_err(),
                "{text:?} must be refused"
            );
        }
    }
}
