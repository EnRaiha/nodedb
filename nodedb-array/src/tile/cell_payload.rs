// SPDX-License-Identifier: Apache-2.0

//! Bitemporal cell payload for the array engine.
//!
//! `CellPayload` wraps the cell's attribute values with valid-time bounds and
//! a Control-Plane-allocated surrogate. The sentinel constants mirror the
//! graph engine's key sentinels so storage layer code can apply the same
//! tombstone / GDPR-erasure distinction to array cells.

pub use nodedb_types::OPEN_UPPER;
use nodedb_types::Surrogate;
use serde::{Deserialize, Serialize};

use crate::error::{ArrayError, ArrayResult};
use crate::types::cell_value::value::CellValue;

/// Soft-delete marker for a cell version.
pub const CELL_TOMBSTONE_SENTINEL: &[u8] = &[0xFF];

/// GDPR erasure marker — preserves coordinate existence, removes content.
pub const CELL_GDPR_ERASURE_SENTINEL: &[u8] = &[0xFE];

/// Bitemporal cell value payload.
///
/// `valid_until_ms` uses [`OPEN_UPPER`] (`i64::MAX`) as the open-upper sentinel.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CellPayload {
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub attrs: Vec<CellValue>,
    pub surrogate: Surrogate,
}

impl CellPayload {
    /// Encode a live cell. A live cell always holds a bound surrogate, so a
    /// payload that carries `Surrogate::ZERO` is refused.
    pub fn encode(&self) -> ArrayResult<Vec<u8>> {
        self.check_bound()?;
        zerompk::to_msgpack_vec(self).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("encode CellPayload: {e}"),
        })
    }

    /// Decode a live cell, refusing one that carries `Surrogate::ZERO`.
    pub fn decode(bytes: &[u8]) -> ArrayResult<Self> {
        let payload: Self =
            zerompk::from_msgpack(bytes).map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("decode CellPayload: {e}"),
            })?;
        payload.check_bound()?;
        Ok(payload)
    }

    fn check_bound(&self) -> ArrayResult<()> {
        if self.surrogate == Surrogate::ZERO {
            return Err(ArrayError::SegmentCorruption {
                detail: "live cell payload carries Surrogate::ZERO, which names no row".into(),
            });
        }
        Ok(())
    }
}

/// Returns true if `bytes` is any non-payload sentinel byte sequence.
pub fn is_cell_sentinel(bytes: &[u8]) -> bool {
    is_cell_tombstone(bytes) || is_cell_gdpr_erasure(bytes)
}

/// Returns true if `bytes` is the soft-delete tombstone sentinel.
pub fn is_cell_tombstone(bytes: &[u8]) -> bool {
    bytes == CELL_TOMBSTONE_SENTINEL
}

/// Returns true if `bytes` is the GDPR erasure sentinel.
pub fn is_cell_gdpr_erasure(bytes: &[u8]) -> bool {
    bytes == CELL_GDPR_ERASURE_SENTINEL
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Surrogate;

    fn sample_payload() -> CellPayload {
        CellPayload {
            valid_from_ms: 1_000,
            valid_until_ms: OPEN_UPPER,
            attrs: vec![CellValue::Int64(42)],
            surrogate: Surrogate::new(7),
        }
    }

    #[test]
    fn a_payload_under_zero_is_refused() {
        let mut p = sample_payload();
        p.surrogate = Surrogate::ZERO;
        assert!(p.encode().is_err());
        let raw = zerompk::to_msgpack_vec(&p).unwrap();
        assert!(CellPayload::decode(&raw).is_err());
    }

    #[test]
    fn payload_msgpack_roundtrip() {
        let p = sample_payload();
        let bytes = p.encode().unwrap();
        let decoded = CellPayload::decode(&bytes).unwrap();
        assert_eq!(decoded.valid_from_ms, 1_000);
        assert_eq!(decoded.valid_until_ms, OPEN_UPPER);
        assert_eq!(decoded.attrs, vec![CellValue::Int64(42)]);
        assert_eq!(decoded.surrogate, Surrogate::new(7));
    }

    #[test]
    fn encoded_payload_first_byte_is_fixarray() {
        let bytes = sample_payload().encode().unwrap();
        // zerompk encodes a 4-field struct as fixarray 0x94.
        assert_eq!(bytes[0], 0x94);
    }

    #[test]
    fn sentinels_are_disjoint_from_payload() {
        let bytes = sample_payload().encode().unwrap();
        assert!(is_cell_sentinel(&[0xFF]));
        assert!(is_cell_sentinel(&[0xFE]));
        assert!(is_cell_tombstone(&[0xFF]));
        assert!(is_cell_gdpr_erasure(&[0xFE]));
        assert!(!is_cell_sentinel(&bytes[..1]));
    }

    #[test]
    fn tombstone_and_erasure_are_distinct() {
        assert!(!is_cell_tombstone(CELL_GDPR_ERASURE_SENTINEL));
        assert!(!is_cell_gdpr_erasure(CELL_TOMBSTONE_SENTINEL));
    }
}
