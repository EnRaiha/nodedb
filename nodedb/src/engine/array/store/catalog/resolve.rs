// SPDX-License-Identifier: BUSL-1.1

//! One-pass bitemporal resolution of every cell under one tile prefix.
//!
//! A prefix holds tile versions in the memtable and in segments. The
//! ceiling of a cell is its first version, in reading order, at or before
//! the cutoff that is a tombstone, an erasure, or a live payload inside the
//! valid-time point. Reading order is the memtable's versions newest first,
//! then the segments' versions newest first by `system_from_ms`.
//!
//! Each tile version is read once, in reading order. A cell is decided by
//! the first version that settles it, and later versions skip it. Segment
//! rows are read from the decoded tile's columns: no cell is encoded to bytes
//! and decoded again.

use std::cmp::Reverse;

use nodedb_array::query::ceiling::CeilingParams;
use nodedb_array::query::region::ArrayRegion;
use nodedb_array::segment::TilePayload;
use nodedb_array::tile::cell_payload::{CellPayload, is_cell_sentinel};
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::{ArrayError, ArrayResult};
use rustc_hash::FxHashSet;

use super::{ArrayStore, CellVersion};
use crate::engine::array::memtable::{Memtable, TileBuffer};

/// Where one tile version of a prefix is held.
enum VersionSource<'a> {
    Memtable(&'a TileBuffer),
    Segment(Box<SparseTile>),
}

/// One tile version of a prefix.
struct PrefixVersion<'a> {
    tile_id: TileId,
    source: VersionSource<'a>,
}

impl ArrayStore {
    /// Whether `prefix` can resolve to a live cell inside `region` at
    /// `system_as_of`. Read from the segment footers' MBRs, so no tile is
    /// decoded.
    ///
    /// Every version of a cell sits under the cell's prefix, and a footer
    /// MBR bounds the live rows of its tile. A tombstone only hides a cell.
    /// So when no qualifying version holds a live row the region can contain,
    /// the prefix resolves to no cell in the region. A version is never
    /// skipped on its own: its tombstones can hide an older cell in the
    /// region. The memtable keeps no MBR, so a prefix with a qualifying
    /// memtable version is always read.
    pub(super) fn prefix_may_hold(
        &self,
        prefix: u64,
        system_as_of: i64,
        region: &ArrayRegion,
    ) -> bool {
        if self
            .memtable
            .iter_tile_versions(prefix, system_as_of)
            .next()
            .is_some()
        {
            return true;
        }
        self.segments.values().any(|h| {
            let reader = h.reader();
            let tiles = reader.tiles();
            let first = tiles.partition_point(|e| e.tile_id.hilbert_prefix < prefix);
            let end = tiles.partition_point(|e| e.tile_id.hilbert_prefix <= prefix);
            tiles[first..end].iter().any(|e| {
                e.tile_id.system_from_ms <= system_as_of && region.tile_may_contain(&e.mbr)
            })
        })
    }

    /// Every tile version of `prefix`, in reading order. Each segment tile is
    /// decoded once.
    fn prefix_versions(&self, prefix: u64) -> ArrayResult<Vec<PrefixVersion<'_>>> {
        let mut versions: Vec<PrefixVersion<'_>> = self
            .memtable
            .iter_tile_versions(prefix, i64::MAX)
            .map(|(tile_id, buf)| PrefixVersion {
                tile_id,
                source: VersionSource::Memtable(buf),
            })
            .collect();
        let mut segment_versions = Vec::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for item in reader.iter_tile_versions(prefix, i64::MAX)? {
                let (tile_id, payload) = item?;
                if let TilePayload::Sparse(tile) = payload {
                    segment_versions.push(PrefixVersion {
                        tile_id,
                        source: VersionSource::Segment(Box::new(tile)),
                    });
                }
            }
        }
        // Stable: segments with one system time keep their read order.
        segment_versions.sort_by_key(|v| Reverse(v.tile_id.system_from_ms));
        versions.extend(segment_versions);
        Ok(versions)
    }

    /// The live cells of `prefix` at `params`, as one tile, or `None` when no
    /// version is at or before the cutoff.
    pub(super) fn resolve_prefix_at(
        &self,
        prefix: u64,
        params: &CeilingParams,
    ) -> ArrayResult<Option<SparseTile>> {
        let mut qualifying: Vec<PrefixVersion<'_>> = self
            .prefix_versions(prefix)?
            .into_iter()
            .filter(|v| v.tile_id.system_from_ms <= params.system_as_of)
            .collect();
        if qualifying.is_empty() {
            return Ok(None);
        }
        // One stored tile of live rows only, read with no valid-time point,
        // is its own answer.
        if params.valid_at_ms.is_none()
            && let [
                PrefixVersion {
                    source: VersionSource::Segment(tile),
                    ..
                },
            ] = qualifying.as_mut_slice()
            && tile.row_kinds.iter().all(|&k| k == RowKind::Live.as_u8())
        {
            let tile = std::mem::replace(&mut **tile, SparseTile::empty(&self.schema));
            return Ok(Some(tile));
        }

        // Within one version a cell appears once, so only a prefix with
        // several versions tracks which cells are decided.
        let mut decided = Decided::new(qualifying.len() > 1);
        let mut builder = SparseTileBuilder::new(&self.schema);
        for version in &qualifying {
            match &version.source {
                VersionSource::Memtable(buf) => {
                    for (key, bytes) in buf.iter_raw() {
                        let coord = Memtable::decode_coord_key(key)?;
                        if decided.contains(&coord) {
                            continue;
                        }
                        if is_cell_sentinel(bytes) {
                            decided.insert(coord);
                            continue;
                        }
                        let payload = CellPayload::decode(bytes)?;
                        if !valid_at(params, payload.valid_from_ms, payload.valid_until_ms) {
                            continue;
                        }
                        builder.push_row(SparseRow::live(
                            &coord,
                            &payload.attrs,
                            payload.surrogate,
                            payload.valid_from_ms,
                            payload.valid_until_ms,
                        ))?;
                        decided.insert(coord);
                    }
                }
                VersionSource::Segment(tile) => {
                    let mut live = 0usize;
                    for row in 0..tile.row_count() {
                        let kind = tile.row_kind(row)?;
                        let live_row = live;
                        if kind == RowKind::Live {
                            live += 1;
                        }
                        let coord = row_coord(tile, row)?;
                        if decided.contains(&coord) {
                            continue;
                        }
                        if kind != RowKind::Live {
                            decided.insert(coord);
                            continue;
                        }
                        let (from, until) = row_valid_time(tile, row)?;
                        if !valid_at(params, from, until) {
                            continue;
                        }
                        builder.push_tile_row(tile, row, live_row, &coord)?;
                        decided.insert(coord);
                    }
                }
            }
        }
        Ok(Some(builder.build()))
    }

    /// Every live cell version of `prefix` inside the valid-time point, each
    /// as `(prefix, coord, system_from_ms, payload)`.
    pub(super) fn prefix_live_versions(
        &self,
        prefix: u64,
        valid_at_ms: Option<i64>,
        out: &mut Vec<CellVersion>,
    ) -> ArrayResult<()> {
        let params = CeilingParams {
            system_as_of: i64::MAX,
            valid_at_ms,
        };
        for version in self.prefix_versions(prefix)? {
            let system_from_ms = version.tile_id.system_from_ms;
            match &version.source {
                VersionSource::Memtable(buf) => {
                    for (key, bytes) in buf.iter_raw() {
                        if is_cell_sentinel(bytes) {
                            continue;
                        }
                        let payload = CellPayload::decode(bytes)?;
                        if !valid_at(&params, payload.valid_from_ms, payload.valid_until_ms) {
                            continue;
                        }
                        out.push((
                            prefix,
                            Memtable::decode_coord_key(key)?,
                            system_from_ms,
                            payload,
                        ));
                    }
                }
                VersionSource::Segment(tile) => {
                    let mut live = 0usize;
                    for row in 0..tile.row_count() {
                        if tile.row_kind(row)? != RowKind::Live {
                            continue;
                        }
                        let live_row = live;
                        live += 1;
                        let (from, until) = row_valid_time(tile, row)?;
                        if !valid_at(&params, from, until) {
                            continue;
                        }
                        let attrs = tile
                            .attr_cols
                            .iter()
                            .map(|col| {
                                col.get(live_row).cloned().ok_or_else(|| {
                                    corrupt(format!(
                                        "tile live row {live_row} has no value in an attr column"
                                    ))
                                })
                            })
                            .collect::<ArrayResult<Vec<_>>>()?;
                        out.push((
                            prefix,
                            row_coord(tile, row)?,
                            system_from_ms,
                            CellPayload {
                                valid_from_ms: from,
                                valid_until_ms: until,
                                attrs,
                                surrogate: tile.live_surrogate(row)?,
                            },
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

/// The cells already settled by an earlier version of the prefix. A prefix
/// with one version settles each cell once, so it tracks none.
struct Decided {
    seen: Option<FxHashSet<Vec<CoordValue>>>,
}

impl Decided {
    fn new(track: bool) -> Self {
        Self {
            seen: track.then(FxHashSet::default),
        }
    }

    fn contains(&self, coord: &[CoordValue]) -> bool {
        self.seen.as_ref().is_some_and(|seen| seen.contains(coord))
    }

    fn insert(&mut self, coord: Vec<CoordValue>) {
        if let Some(seen) = self.seen.as_mut() {
            seen.insert(coord);
        }
    }
}

/// Whether a live payload valid over `[from, until)` answers `params`.
fn valid_at(params: &CeilingParams, from: i64, until: i64) -> bool {
    match params.valid_at_ms {
        Some(vt) => from <= vt && vt < until,
        None => true,
    }
}

fn row_coord(tile: &SparseTile, row: usize) -> ArrayResult<Vec<CoordValue>> {
    tile.dim_dicts
        .iter()
        .map(|dict| {
            dict.indices
                .get(row)
                .and_then(|&i| dict.values.get(i as usize))
                .cloned()
                .ok_or_else(|| corrupt(format!("tile row {row} has no coordinate on an axis")))
        })
        .collect()
}

fn row_valid_time(tile: &SparseTile, row: usize) -> ArrayResult<(i64, i64)> {
    match (tile.valid_from_ms.get(row), tile.valid_until_ms.get(row)) {
        (Some(&from), Some(&until)) => Ok((from, until)),
        _ => Err(corrupt(format!("tile row {row} has no valid time"))),
    }
}

fn corrupt(detail: String) -> ArrayError {
    ArrayError::SegmentCorruption { detail }
}
