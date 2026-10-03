// SPDX-License-Identifier: Apache-2.0

//! Coordinate-level Ceiling resolution for bitemporal cell versions.
//!
//! `ceiling_resolve_cell` finds the latest system-time version of a cell
//! at or before `system_as_of`, optionally filtered by a valid-time
//! point `valid_at_ms`.  The caller supplies the version iterator
//! newest-first; this function performs no I/O.

use crate::error::{ArrayError, ArrayResult};
use crate::tile::cell_payload::{CellPayload, is_cell_gdpr_erasure, is_cell_tombstone};
use crate::types::TileId;
use crate::types::coord::value::CoordValue;

/// Result of a Ceiling resolution for a single cell coordinate.
#[derive(Debug, Clone, PartialEq)]
pub enum CeilingResult {
    /// A live (non-sentinel) payload whose valid-time bounds satisfy the query.
    Live(CellPayload),
    /// The most recent qualifying version is a soft-delete tombstone.
    Tombstoned,
    /// The most recent qualifying version is a GDPR erasure marker.
    Erased,
    /// No version exists at or before `system_as_of`, or all versions failed
    /// the valid-time filter and no older version qualifies.
    NotFound,
}

/// Parameters for [`ceiling_resolve_cell`] bundled to avoid a long argument list.
pub struct CeilingParams {
    /// Upper bound (inclusive) on `TileId::system_from_ms`. Versions newer
    /// than this cutoff are skipped.
    pub system_as_of: i64,
    /// When `Some(vt)`, a live payload is only returned if
    /// `valid_from_ms <= vt < valid_until_ms`.  Tombstone and erasure
    /// sentinels short-circuit before this check.
    pub valid_at_ms: Option<i64>,
}

/// Resolve the Ceiling for a single cell coordinate.
///
/// `tile_versions_newest_first` — `(TileId, raw_cell_bytes)` pairs ordered
/// newest-first by `system_from_ms`.  The caller is responsible for the
/// ordering; this function additionally defends against misuse with a guard
/// that skips entries where `tile_id.system_from_ms > params.system_as_of`.
///
/// For each version the function applies:
/// 1. System-time guard: skip if `tile_id.system_from_ms > system_as_of`.
/// 2. Sentinel check: tombstone → `Tombstoned`; GDPR erasure → `Erased`.
/// 3. Valid-time filter (when `valid_at_ms` is `Some`): if the decoded
///    payload's valid-time interval does not contain `vt`, continue to the
///    next older version.
/// 4. Return `Live(payload)`.
///
/// If the iterator is exhausted without a match, returns `NotFound`.
pub fn ceiling_resolve_cell<'a, I>(
    tile_versions_newest_first: I,
    _coord: &[CoordValue],
    params: &CeilingParams,
) -> ArrayResult<CeilingResult>
where
    I: IntoIterator<Item = (TileId, &'a [u8])>,
{
    for (tile_id, bytes) in tile_versions_newest_first {
        if tile_id.system_from_ms > params.system_as_of {
            continue;
        }
        if is_cell_tombstone(bytes) {
            return Ok(CeilingResult::Tombstoned);
        }
        if is_cell_gdpr_erasure(bytes) {
            return Ok(CeilingResult::Erased);
        }
        let payload = CellPayload::decode(bytes).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("ceiling_resolve_cell: {e}"),
        })?;
        match params.valid_at_ms {
            Some(vt) if !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms) => {
                continue;
            }
            _ => return Ok(CeilingResult::Live(payload)),
        }
    }
    Ok(CeilingResult::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tile::cell_payload::{
        CELL_GDPR_ERASURE_SENTINEL, CELL_TOMBSTONE_SENTINEL, CellPayload, OPEN_UPPER,
    };
    use crate::types::TileId;
    use crate::types::cell_value::value::CellValue;
    use nodedb_types::Surrogate;

    fn payload(valid_from: i64, valid_until: i64, val: i64) -> Vec<u8> {
        CellPayload {
            valid_from_ms: valid_from,
            valid_until_ms: valid_until,
            attrs: vec![CellValue::Int64(val)],
            surrogate: Surrogate::new(1),
        }
        .encode()
        .unwrap()
    }

    fn live_params(system_as_of: i64, valid_at_ms: Option<i64>) -> CeilingParams {
        CeilingParams {
            system_as_of,
            valid_at_ms,
        }
    }

    #[test]
    fn live_ceiling_at_cutoff() {
        // Three versions: v1@100, v2@200, v3@300.
        // Query at system_as_of=250 → v2.
        let v1 = payload(0, OPEN_UPPER, 1);
        let v2 = payload(0, OPEN_UPPER, 2);
        let v3 = payload(0, OPEN_UPPER, 3);

        let versions = vec![
            (TileId::new(1, 300), v3.as_slice()),
            (TileId::new(1, 200), v2.as_slice()),
            (TileId::new(1, 100), v1.as_slice()),
        ];

        let result = ceiling_resolve_cell(versions, &[], &live_params(250, None)).unwrap();
        match result {
            CeilingResult::Live(p) => assert_eq!(p.attrs[0], CellValue::Int64(2)),
            other => panic!("expected Live(2), got {other:?}"),
        }
    }

    #[test]
    fn tombstone_shadows_earlier_versions() {
        let v1 = payload(0, OPEN_UPPER, 1);
        let versions_at_300 = vec![
            (TileId::new(1, 200), CELL_TOMBSTONE_SENTINEL),
            (TileId::new(1, 100), v1.as_slice()),
        ];
        let result = ceiling_resolve_cell(versions_at_300, &[], &live_params(300, None)).unwrap();
        assert!(matches!(result, CeilingResult::Tombstoned));

        // Even at cutoff equal to the tombstone's system_from_ms.
        let v1b = payload(0, OPEN_UPPER, 1);
        let versions_at_200 = vec![
            (TileId::new(1, 200), CELL_TOMBSTONE_SENTINEL),
            (TileId::new(1, 100), v1b.as_slice()),
        ];
        let result2 = ceiling_resolve_cell(versions_at_200, &[], &live_params(200, None)).unwrap();
        assert!(matches!(result2, CeilingResult::Tombstoned));
    }

    #[test]
    fn gdpr_erasure_distinct_from_tombstone() {
        let v1 = payload(0, OPEN_UPPER, 1);
        let versions = vec![
            (TileId::new(1, 200), CELL_GDPR_ERASURE_SENTINEL),
            (TileId::new(1, 100), v1.as_slice()),
        ];
        let result = ceiling_resolve_cell(versions, &[], &live_params(300, None)).unwrap();
        assert!(matches!(result, CeilingResult::Erased));
        assert!(!matches!(result, CeilingResult::Tombstoned));
    }

    #[test]
    fn valid_time_filter_falls_back_to_older_version() {
        // v1: valid [0,100), v2: valid [200,300).
        let v1 = payload(0, 100, 1);
        let v2 = payload(200, 300, 2);

        let make_versions = || {
            vec![
                (TileId::new(1, 200), v2.clone()),
                (TileId::new(1, 100), v1.clone()),
            ]
        };

        // valid_at=50 → falls through v2 (not in [200,300)) → returns v1.
        let r1 = ceiling_resolve_cell(
            make_versions().iter().map(|(id, b)| (*id, b.as_slice())),
            &[],
            &live_params(300, Some(50)),
        )
        .unwrap();
        match r1 {
            CeilingResult::Live(p) => assert_eq!(p.attrs[0], CellValue::Int64(1)),
            other => panic!("expected Live(1) for valid_at=50, got {other:?}"),
        }

        // valid_at=150 → v2 not in [200,300), v1 not in [0,100) → NotFound.
        let r2 = ceiling_resolve_cell(
            make_versions().iter().map(|(id, b)| (*id, b.as_slice())),
            &[],
            &live_params(300, Some(150)),
        )
        .unwrap();
        assert!(matches!(r2, CeilingResult::NotFound));

        // valid_at=250 → v2 in [200,300) → Live(2).
        let r3 = ceiling_resolve_cell(
            make_versions().iter().map(|(id, b)| (*id, b.as_slice())),
            &[],
            &live_params(300, Some(250)),
        )
        .unwrap();
        match r3 {
            CeilingResult::Live(p) => assert_eq!(p.attrs[0], CellValue::Int64(2)),
            other => panic!("expected Live(2) for valid_at=250, got {other:?}"),
        }
    }

    #[test]
    fn not_found_below_horizon() {
        let v1 = payload(0, OPEN_UPPER, 1);
        // Only version at system_from_ms=100; query at system_as_of=50.
        let versions = vec![(TileId::new(1, 100), v1.as_slice())];
        let result = ceiling_resolve_cell(versions, &[], &live_params(50, None)).unwrap();
        assert!(matches!(result, CeilingResult::NotFound));
    }
}
