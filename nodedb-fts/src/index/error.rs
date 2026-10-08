// SPDX-License-Identifier: Apache-2.0

//! Typed errors for `FtsIndex` operations.
//!
//! `FtsIndexError<E>` wraps both FTS-layer errors (e.g. surrogate out of range)
//! and backend storage errors (`E = B::Error`). Callers that only use in-memory
//! backends (tests, WASM) will have `E = std::convert::Infallible`.

use thiserror::Error;

use nodedb_types::Surrogate;

use crate::search::query_parser::InvalidQuery;

use nodedb_mem::MemError;

/// Maximum `Surrogate` value that can be safely indexed.
///
/// FTS posting blocks store doc IDs as `u32` on disk (delta-encoded, bitpacked).
/// The in-memory memtable uses the surrogate's raw `u32` as a direct index into
/// per-doc fieldnorm arrays (`fieldnorms[surrogate.0 as usize]`). A surrogate
/// near `u32::MAX` would cause the fieldnorm array to be resized to ~4 GiB,
/// exhausting process memory.
///
/// The cap is set to `u32::MAX - 1` — the same ceiling as the graph CSR node-id
/// policy. `u32::MAX` itself is reserved to make "invalid" sentinels
/// representable in the `u32` space without aliasing a real doc. The shared
/// `Surrogate::ZERO` sentinel (value 0) is also rejected at indexing time because
/// it is the "unassigned" marker used by the Control Plane.
///
/// 4 billion documents per FTS index per collection is well beyond any practical
/// workload. Collections approaching this limit should be partitioned.
pub const MAX_INDEXABLE_SURROGATE: u32 = u32::MAX - 1;

/// Errors returned by `FtsIndex` write operations.
///
/// `E` is the backend error type (`B::Error`). Backend errors are wrapped in
/// `FtsIndexError::Backend` so callers get a single error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FtsIndexError<E: std::fmt::Display> {
    /// The supplied `Surrogate` is outside the indexable range `1..=MAX_INDEXABLE_SURROGATE`.
    ///
    /// Either the zero sentinel (`Surrogate::ZERO`, meaning "not yet assigned")
    /// or a value at `u32::MAX` was passed to `index_document`. The Control
    /// Plane surrogate allocator must ensure surrogates are in range before
    /// dispatching indexing operations.
    #[error(
        "surrogate {surrogate} is out of the indexable range \
         1..={MAX_INDEXABLE_SURROGATE}; \
         the zero value is the unassigned sentinel and u32::MAX is reserved"
    )]
    SurrogateOutOfRange { surrogate: Surrogate },

    /// A term in the document exceeds the on-disk `u16` length cap.
    ///
    /// The FTS segment format encodes term lengths as `u16` (see
    /// `lsm/segment/format.rs::MAX_TERM_LEN`). Terms longer than
    /// `u16::MAX` (65 535 bytes) cannot be persisted. After analysis,
    /// real-world terms are typically 2-20 bytes — exceeding this cap
    /// indicates a malformed analyzer or adversarial input.
    #[error("term length {len} exceeds maximum {max} bytes (FTS segment format limit)")]
    TermTooLong { len: usize, max: usize },

    /// The FTS query string is invalid (e.g. NOT-only, unsupported parentheses).
    #[error("invalid FTS query: {0}")]
    InvalidQuery(#[from] InvalidQuery),

    /// An underlying backend storage operation failed.
    #[error("FTS backend error: {0}")]
    Backend(E),

    /// A segment I/O or validation error not otherwise classified.
    ///
    /// Read-side variants (`BadMagic`, `UnsupportedVersion`, `ChecksumMismatch`,
    /// `Truncated`) are not expected on the write/flush path but are propagated
    /// here rather than panicking, so corrupt-state surprises surface as typed
    /// errors at the public API boundary.
    #[error("FTS segment error: {0}")]
    Segment(crate::lsm::segment::error::SegmentError),

    /// A stored segment of the index fails validation when it is opened.
    ///
    /// The segment's postings cannot be read, so a read that skipped it
    /// would answer from part of the index.
    #[error("FTS segment {segment_id} is corrupt: {source}")]
    CorruptSegment {
        segment_id: String,
        source: crate::lsm::segment::error::SegmentError,
    },

    /// The backend lists a segment of the index that it does not hold.
    #[error("FTS segment {segment_id} is listed but missing")]
    MissingSegment { segment_id: String },

    /// A stored index-state blob (backend metadata `subkey`) fails to decode.
    #[error("FTS index state '{subkey}' is corrupt: {detail}")]
    CorruptState {
        subkey: &'static str,
        detail: String,
    },

    /// An index-state blob (backend metadata `subkey`) fails to encode.
    #[error("FTS index state '{subkey}' failed to encode: {detail}")]
    StateEncode {
        subkey: &'static str,
        detail: String,
    },

    /// Memory budget exhausted for the FTS engine.
    ///
    /// The operation requires more memory than the engine's remaining budget
    /// allows. Callers should backpressure, spill, or reject the request.
    #[error("FTS memory budget exhausted: {0}")]
    BudgetExhausted(MemError),
}

impl<E: std::fmt::Display> From<crate::lsm::segment::error::SegmentError> for FtsIndexError<E> {
    fn from(err: crate::lsm::segment::error::SegmentError) -> Self {
        use crate::lsm::segment::error::SegmentError;
        match err {
            SegmentError::TermTooLong { term_len, max } => {
                FtsIndexError::TermTooLong { len: term_len, max }
            }
            other => FtsIndexError::Segment(other),
        }
    }
}

impl<E: std::fmt::Display> FtsIndexError<E> {
    /// Wrap a backend error.
    pub(crate) fn backend(e: E) -> Self {
        Self::Backend(e)
    }
}

impl<E: std::fmt::Display> From<MemError> for FtsIndexError<E> {
    fn from(e: MemError) -> Self {
        Self::BudgetExhausted(e)
    }
}
