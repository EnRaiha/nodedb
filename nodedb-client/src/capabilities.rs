// SPDX-License-Identifier: Apache-2.0

//! `Capabilities` newtype for typed capability bit-test accessors.
//!
//! Wraps the raw `u64` bitfield advertised by the server in `HelloAckFrame`
//! and exposes named predicates so SDK consumers can ask
//! "does this server support GraphRAG fusion?" without knowing which bit to mask.

use nodedb_types::protocol::{
    CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_SPATIAL, CAP_STREAMING, CAP_TIMESERIES,
};

/// Typed wrapper around the raw capability bitfield from `HelloAckFrame`.
///
/// Obtained via `NodeDb::capabilities()`. Use the predicate methods to
/// test for specific server features before calling the corresponding
/// operations.
///
/// # Example
/// ```no_run
/// # use nodedb_client::capabilities::Capabilities;
/// let caps = Capabilities::from_raw(0x07); // streaming + graphrag + fts
/// assert!(caps.supports_streaming());
/// assert!(caps.supports_graphrag());
/// assert!(!caps.supports_crdt());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Capabilities(pub u64);

impl Capabilities {
    /// Create from a raw capability bitfield.
    pub fn from_raw(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns the underlying raw bitfield.
    pub fn raw(self) -> u64 {
        self.0
    }

    /// Whether the server supports response streaming
    /// (partial-response chunking via `ResponseStatus::Partial`).
    pub fn supports_streaming(self) -> bool {
        self.0 & CAP_STREAMING != 0
    }

    /// Whether the server supports the `GraphRagFusion` opcode.
    pub fn supports_graphrag(self) -> bool {
        self.0 & CAP_GRAPHRAG != 0
    }

    /// Whether the server supports full-text search opcodes
    /// (`TextSearch`, `HybridSearch`).
    pub fn supports_fts(self) -> bool {
        self.0 & CAP_FTS != 0
    }

    /// Whether the server supports CRDT operations (`CrdtRead`, `CrdtApply`).
    pub fn supports_crdt(self) -> bool {
        self.0 & CAP_CRDT != 0
    }

    /// Whether the server supports spatial scan (`SpatialScan`).
    pub fn supports_spatial(self) -> bool {
        self.0 & CAP_SPATIAL != 0
    }

    /// Whether the server supports timeseries operations
    /// (`TimeseriesScan`, `TimeseriesIngest`).
    pub fn supports_timeseries(self) -> bool {
        self.0 & CAP_TIMESERIES != 0
    }

    /// Whether the server supports columnar operations
    /// (`ColumnarScan`, `ColumnarInsert`).
    pub fn supports_columnar(self) -> bool {
        self.0 & CAP_COLUMNAR != 0
    }

    /// Test an arbitrary capability bit.
    pub fn has(self, bit: u64) -> bool {
        self.0 & bit != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::{CAP_FTS, CAP_STREAMING};

    #[test]
    fn all_bits_set() {
        let all = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;
        let caps = Capabilities::from_raw(all);
        assert!(caps.supports_streaming());
        assert!(caps.supports_graphrag());
        assert!(caps.supports_fts());
        assert!(caps.supports_crdt());
        assert!(caps.supports_spatial());
        assert!(caps.supports_timeseries());
        assert!(caps.supports_columnar());
    }

    #[test]
    fn no_bits_set() {
        let caps = Capabilities::from_raw(0);
        assert!(!caps.supports_streaming());
        assert!(!caps.supports_graphrag());
        assert!(!caps.supports_fts());
        assert!(!caps.supports_crdt());
        assert!(!caps.supports_spatial());
        assert!(!caps.supports_timeseries());
        assert!(!caps.supports_columnar());
    }

    #[test]
    fn selective_bits() {
        let caps = Capabilities::from_raw(CAP_STREAMING | CAP_CRDT);
        assert!(caps.supports_streaming());
        assert!(caps.supports_crdt());
        assert!(!caps.supports_graphrag());
        assert!(!caps.supports_fts());
    }

    #[test]
    fn has_arbitrary_bit() {
        let caps = Capabilities::from_raw(1 << 10);
        assert!(caps.has(1 << 10));
        assert!(!caps.has(1 << 11));
    }

    #[test]
    fn raw_roundtrip() {
        let bits = CAP_GRAPHRAG | CAP_FTS;
        let caps = Capabilities::from_raw(bits);
        assert_eq!(caps.raw(), bits);
    }

    #[test]
    fn default_is_zero() {
        let caps = Capabilities::default();
        assert_eq!(caps.raw(), 0);
        assert!(!caps.supports_streaming());
    }
}
