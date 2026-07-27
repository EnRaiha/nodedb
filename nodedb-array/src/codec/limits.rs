// SPDX-License-Identifier: Apache-2.0

// Defensive size caps for codec decoders.
//
// Every length field read from segment bytes drives an allocation or a
// loop. A corrupted segment can declare values up to `u32::MAX` and trick
// the decoder into allocating tens of GB or running for billions of
// iterations. These caps reject impossibly large values as
// `SegmentCorruption` errors before the allocation happens.
//
// The caps are sized comfortably above legitimate workloads:
//   * Memtable flush threshold is 4096 cells; ~1M cells per tile is
//     already 250x the typical case.
//   * Schemas have <=64 dims and <=256 attrs in practice.
//   * Dictionary cardinality is bounded by cell count.
//
// If a real workload ever needs more, the cap is raised in this one
// place and the rationale stays auditable.

use crate::error::{ArrayError, ArrayResult};

/// Maximum cells in a single sparse tile.
pub const MAX_CELLS_PER_TILE: usize = 8_000_000;

/// Maximum coordinate axes (dimensions) per tile.
pub const MAX_AXES_PER_TILE: usize = 1_024;

/// Maximum attribute columns per tile.
pub const MAX_ATTRS_PER_TILE: usize = 1_024;

/// Maximum distinct values in a per-axis dictionary.
pub const MAX_DICT_CARDINALITY: usize = 8_000_000;

/// Maximum runs in an RLE-encoded index stream.
pub const MAX_RLE_RUNS: usize = 8_000_000;

/// Maximum length of a single RLE run (bounded by tile cell count).
pub const MAX_RLE_RUN_LEN: usize = MAX_CELLS_PER_TILE;

/// Maximum entries in any column codec output (timestamps, surrogates, attrs).
pub const MAX_COLUMN_ENTRIES: usize = MAX_CELLS_PER_TILE;

/// Reject a length value that exceeds `cap`. Used everywhere a `usize`
/// length is decoded from segment bytes.
pub fn check_decoded_size(value: usize, cap: usize, what: &str) -> ArrayResult<()> {
    checked_capacity(value, cap, 0, 0, what).map(|_| ())
}

/// Validate a decoded collection count before it drives an allocation.
///
/// `min_encoded_bytes` is the smallest on-wire representation per entry. It
/// rejects a tiny frame carrying a huge count before reserving memory, while
/// the caller still validates each variable-sized entry as it is consumed.
pub fn checked_capacity(
    value: usize,
    cap: usize,
    remaining_bytes: usize,
    min_encoded_bytes: usize,
    what: &str,
) -> ArrayResult<usize> {
    if value > cap {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "decoded {what} = {value} exceeds hard cap {cap} (segment likely corrupt)"
            ),
        });
    }
    let minimum_bytes =
        value
            .checked_mul(min_encoded_bytes)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!("decoded {what} minimum encoded size overflows"),
            })?;
    if minimum_bytes > remaining_bytes {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "decoded {what} requires at least {minimum_bytes} encoded bytes, only {remaining_bytes} remain"
            ),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_capacity_rejects_count_without_minimum_encoded_bytes() {
        assert!(checked_capacity(2, 8, 7, 4, "test entries").is_err());
    }

    #[test]
    fn checked_capacity_rejects_count_over_hard_cap() {
        assert!(checked_capacity(9, 8, 36, 4, "test entries").is_err());
    }

    #[test]
    fn checked_capacity_returns_validated_count() {
        assert_eq!(checked_capacity(2, 8, 8, 4, "test entries").unwrap(), 2);
    }
}
