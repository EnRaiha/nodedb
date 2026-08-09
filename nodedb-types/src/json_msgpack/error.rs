// SPDX-License-Identifier: Apache-2.0

//! Error type for the msgpack readers in this module.
//!
//! The readers wrap `zerompk`'s codec errors and add the one failure mode a
//! plain codec error cannot express: the bytes decoded to a complete top-level
//! value, but the input was not fully consumed.
//!
//! A stored body holds exactly one top-level value. A non-empty remainder
//! therefore means the slot does not hold what it claims to: a truncated body
//! concatenated with another, a stray suffix, or two documents written into one
//! slot. Decoding the leading value and discarding the rest would report
//! success and make that corruption permanently invisible, so the remainder is
//! a decode failure and carries how much was left.

use core::fmt;

/// A failure while reading MessagePack bytes.
#[derive(Debug)]
pub enum MsgpackError {
    /// The bytes are not well-formed MessagePack.
    Codec(zerompk::Error),
    /// A complete top-level value was decoded, but bytes remained after it.
    TrailingBytes {
        /// Bytes consumed by the top-level value.
        consumed: usize,
        /// Total length of the input.
        total: usize,
    },
}

/// Result alias for the msgpack readers.
pub type MsgpackResult<T> = Result<T, MsgpackError>;

impl fmt::Display for MsgpackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(e) => write!(f, "{e}"),
            Self::TrailingBytes { consumed, total } => write!(
                f,
                "trailing bytes after top-level msgpack value: consumed {consumed} of {total}"
            ),
        }
    }
}

impl std::error::Error for MsgpackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Codec(e) => Some(e),
            Self::TrailingBytes { .. } => None,
        }
    }
}

impl From<zerompk::Error> for MsgpackError {
    fn from(e: zerompk::Error) -> Self {
        Self::Codec(e)
    }
}
