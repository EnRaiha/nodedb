// SPDX-License-Identifier: Apache-2.0

//! The error for a cell the client cannot decode.

use nodedb_types::error::NodeDbError;
use tokio_postgres::types::Type;

/// Why the bytes of one cell do not decode.
#[derive(Debug, Clone, thiserror::Error, PartialEq)]
pub(super) enum DecodeReason {
    /// A binary scalar with the wrong byte count.
    #[error("binary form needs {expected} bytes, found {found}")]
    Width { expected: usize, found: usize },
    /// A binary `bool` byte other than 0 or 1.
    #[error("binary bool byte must be 0 or 1")]
    BoolByte,
    /// Text bytes that are not UTF-8.
    #[error("text is not UTF-8")]
    NotUtf8,
    /// Text that is not the PostgreSQL text form of `expected`.
    #[error("text is not a valid {expected}")]
    Invalid { expected: &'static str },
    /// A value outside the range `Value` can hold.
    #[error("{what} is out of range")]
    OutOfRange { what: &'static str },
    /// A PG type the server never sends and the client cannot decode.
    #[error("the client has no decoder for this type")]
    Unsupported,
    /// An array literal that breaks the PostgreSQL array syntax.
    #[error("array literal syntax: {0}")]
    ArraySyntax(&'static str),
    /// One array element that does not decode as the element type.
    #[error("array element {index} ({text:?}) of type {element_type}: {source}")]
    Element {
        index: usize,
        text: String,
        element_type: String,
        source: Box<DecodeReason>,
    },
}

/// The error for a cell of column `column` and PG type `ty` whose bytes
/// `raw` do not decode. It names the column, the PG type, the cell text and
/// the reason. Bytes that are not printable UTF-8 show as `\x` hex.
pub(super) fn cell_error(
    column: &str,
    ty: &Type,
    raw: &[u8],
    reason: &DecodeReason,
) -> NodeDbError {
    NodeDbError::serialization(
        "pgwire",
        format!(
            "column \"{column}\" of type {}: cannot decode {}: {reason}",
            ty.name(),
            cell_text(raw)
        ),
    )
}

/// The cell as the error shows it: quoted UTF-8 text, or `\x` hex for bytes
/// that are not printable UTF-8.
fn cell_text(raw: &[u8]) -> String {
    match std::str::from_utf8(raw) {
        Ok(text) if !text.chars().any(char::is_control) => format!("{text:?}"),
        _ => {
            let mut hex = String::with_capacity(2 + raw.len() * 2);
            hex.push_str("\\x");
            for byte in raw {
                hex.push_str(&format!("{byte:02x}"));
            }
            hex
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_column_type_text_and_reason() {
        let reason = DecodeReason::Invalid {
            expected: "numeric",
        };
        let message = cell_error("price", &Type::NUMERIC, b"abc", &reason).to_string();
        assert!(message.contains("\"price\""), "{message}");
        assert!(message.contains("type numeric"), "{message}");
        assert!(message.contains("\"abc\""), "{message}");
        assert!(message.contains("not a valid numeric"), "{message}");
    }

    #[test]
    fn shows_binary_bytes_as_hex() {
        let reason = DecodeReason::Width {
            expected: 4,
            found: 3,
        };
        let message = cell_error("n", &Type::INT4, &[0, 1, 0xff], &reason).to_string();
        assert!(message.contains("\\x0001ff"), "{message}");
        assert!(message.contains("needs 4 bytes, found 3"), "{message}");
    }
}
