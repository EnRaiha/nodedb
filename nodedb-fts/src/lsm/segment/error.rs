// SPDX-License-Identifier: Apache-2.0

//! Typed errors for segment I/O and validation.

use thiserror::Error;

/// All failure modes for reading or writing an FTS segment file.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SegmentError {
    /// Magic bytes at offset 0..4 do not match `b"FTSS"`.
    #[error("bad segment magic: expected b\"FTSS\"")]
    BadMagic,

    /// The segment was written by an incompatible format version.
    #[error("unsupported segment version {found} (current: {expected})")]
    UnsupportedVersion { found: u16, expected: u16 },

    /// The footer CRC does not match the re-computed CRC of the preceding bytes.
    #[error("segment checksum mismatch: expected {expected:#010x}, actual {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// The file is shorter than the minimum valid segment size.
    #[error("segment data is truncated")]
    Truncated,

    /// A term's posting data lies outside the segment body or does not
    /// decode, although the segment checksum matched.
    #[error("posting data of term '{term}' is corrupt")]
    CorruptPostings { term: String },

    /// A term exceeds the maximum encodable length (u16::MAX bytes).
    #[error("term length {term_len} exceeds maximum {max}")]
    TermTooLong { term_len: usize, max: usize },
}
