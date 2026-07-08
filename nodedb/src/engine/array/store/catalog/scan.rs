// SPDX-License-Identifier: BUSL-1.1

//! Tile/cell scanning and bitemporal ceiling resolution for [`ArrayStore`].

use nodedb_array::query::ceiling::{CeilingParams, CeilingResult, ceiling_resolve_cell};
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::cell_payload::{CellPayload, is_cell_sentinel};
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::types::coord::value::CoordValue;

use super::{ArrayStore, CellVersion};

impl ArrayStore {
    /// Run the MBR predicate against every segment + the memtable.
    /// Returns decoded tile payloads in segment-then-memtable order.
    pub fn scan_tiles(
        &self,
        pred: &MbrQueryPredicate,
    ) -> Result<Vec<TilePayload>, nodedb_array::ArrayError> {
        Ok(self
            .scan_tiles_with_hilbert_prefix(pred)?
            .into_iter()
            .map(|(_hp, tile)| tile)
            .collect())
    }

    /// Like `scan_tiles` but also returns the tile's `hilbert_prefix` so
    /// callers can apply per-shard Hilbert-range filters (distributed agg).
    pub fn scan_tiles_with_hilbert_prefix(
        &self,
        pred: &MbrQueryPredicate,
    ) -> Result<Vec<(u64, TilePayload)>, nodedb_array::ArrayError> {
        let mut out = Vec::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for idx in h.rtree().query(pred) {
                let hilbert_prefix = reader
                    .tiles()
                    .get(idx)
                    .map(|e| e.tile_id.hilbert_prefix)
                    .unwrap_or(0);
                out.push((hilbert_prefix, reader.read_tile(idx)?));
            }
        }
        for (tile_id, buf) in self.memtable.iter() {
            if buf.entry_count() == 0 {
                continue;
            }
            out.push((
                tile_id.hilbert_prefix,
                TilePayload::Sparse(buf.materialise(&self.schema)?),
            ));
        }
        Ok(out)
    }

    /// Bitemporal scan: resolve the ceiling for every cell coordinate at the
    /// given `system_as_of` and optional `valid_at_ms` point.
    ///
    /// Returns one `(hilbert_prefix, SparseTile)` pair per prefix that has at
    /// least one `Live` cell after ceiling resolution. Tombstoned and erased
    /// coords are omitted.
    ///
    /// Also returns `truncated_before_horizon`: `true` when the store contains
    /// at least one tile version but the `system_as_of` cutoff is below every
    /// version's `system_from_ms` (i.e., the cutoff predates all data).
    pub fn scan_tiles_at(
        &self,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> Result<(Vec<(u64, SparseTile)>, bool), nodedb_array::ArrayError> {
        let params = CeilingParams {
            system_as_of,
            valid_at_ms,
        };

        // Collect all distinct hilbert_prefix values present in any version.
        let all_prefixes = self.all_hilbert_prefixes();

        // Did any version exist at all in the store?
        let any_versions = !all_prefixes.is_empty();

        let mut out: Vec<(u64, SparseTile)> = Vec::new();
        let mut any_qualifying = false;

        for prefix in all_prefixes {
            // Collect all distinct coords across every version for this prefix.
            let coords = self.distinct_coords_for_prefix(prefix)?;

            let mut builder = SparseTileBuilder::new(&self.schema);
            for coord in &coords {
                // Build the version iterator for this coord across all sources.
                // Memtable versions (newer) first, then segment versions (older).
                let cell_versions = self.cell_versions_for_coord(prefix, coord, i64::MAX)?;

                // Check if there are any versions at or before the cutoff.
                if cell_versions
                    .iter()
                    .any(|(tid, _)| tid.system_from_ms <= system_as_of)
                {
                    any_qualifying = true;
                }

                let iter = cell_versions
                    .iter()
                    .map(|(tid, bytes)| (*tid, bytes.as_slice()));
                match ceiling_resolve_cell(iter, coord, &params)? {
                    CeilingResult::Live(payload) => {
                        builder
                            .push_row(nodedb_array::tile::sparse_tile::SparseRow {
                                coord,
                                attrs: &payload.attrs,
                                surrogate: payload.surrogate,
                                valid_from_ms: payload.valid_from_ms,
                                valid_until_ms: payload.valid_until_ms,
                                kind: nodedb_array::tile::sparse_tile::RowKind::Live,
                            })
                            .map_err(|e| nodedb_array::ArrayError::SegmentCorruption {
                                detail: format!("scan_tiles_at builder: {e}"),
                            })?;
                    }
                    CeilingResult::Tombstoned | CeilingResult::Erased | CeilingResult::NotFound => {
                    }
                }
            }

            let tile = builder.build();
            if tile.nnz() > 0 {
                out.push((prefix, tile));
            }
        }

        let truncated_before_horizon = any_versions && !any_qualifying;
        Ok((out, truncated_before_horizon))
    }

    /// Audit-log scan: return every **live** cell-version across all system times.
    ///
    /// Each returned entry is `(hilbert_prefix, coord, system_from_ms, payload)`.
    /// Tombstoned and erased versions are skipped — mirrors `versioned_scan_all`
    /// in the document engine.
    ///
    /// When `valid_at_ms` is `Some(vt)`, only versions whose
    /// `valid_from_ms <= vt < valid_until_ms` are included.
    ///
    /// The caller is responsible for sorting and applying limits.
    pub fn scan_tiles_all_versions(
        &self,
        valid_at_ms: Option<i64>,
    ) -> Result<Vec<CellVersion>, nodedb_array::ArrayError> {
        // Collect all distinct (hilbert_prefix, coord) pairs.
        let all_prefixes = self.all_hilbert_prefixes();

        let mut out: Vec<CellVersion> = Vec::new();

        for prefix in all_prefixes {
            // Collect all distinct coords for this prefix (across all versions).
            let coords = self.distinct_coords_for_prefix(prefix)?;

            for coord in &coords {
                // All versions for this coord across memtable + segments,
                // newest-first by system_from_ms.
                let versions = self.cell_versions_for_coord(prefix, coord, i64::MAX)?;
                for (tile_id, bytes) in &versions {
                    // Skip tombstones and erasures — emit only live payloads.
                    if is_cell_sentinel(bytes) {
                        continue;
                    }
                    let payload = CellPayload::decode(bytes)?;
                    // Apply valid-time point filter if requested.
                    if let Some(vt) = valid_at_ms
                        && !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms)
                    {
                        continue;
                    }
                    out.push((prefix, coord.clone(), tile_id.system_from_ms, payload));
                }
            }
        }

        Ok(out)
    }

    /// Resolve the ceiling for a specific cell coordinate.
    ///
    /// Returns the raw `CeilingResult` so callers can distinguish between
    /// `Live`, `Tombstoned`, `Erased`, and `NotFound` — unlike `scan_tiles_at`
    /// which collapses Tombstoned/Erased/NotFound into "no row in output tile".
    ///
    /// Useful for testing and diagnostic code that needs the exact sentinel type.
    pub fn ceiling_for_coord(
        &self,
        coord: &[CoordValue],
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> nodedb_array::ArrayResult<nodedb_array::query::ceiling::CeilingResult> {
        use nodedb_array::query::ceiling::CeilingParams;
        // Find the hilbert_prefix for this coord.
        let hilbert_prefix = {
            use nodedb_array::tile::tile_id_for_cell;
            let tile = tile_id_for_cell(&self.schema, coord, 0).map_err(|e| {
                nodedb_array::ArrayError::SegmentCorruption {
                    detail: format!("ceiling_for_coord: tile id: {e}"),
                }
            })?;
            tile.hilbert_prefix
        };
        let versions = self.cell_versions_for_coord(hilbert_prefix, coord, system_as_of)?;
        let params = CeilingParams {
            system_as_of,
            valid_at_ms,
        };
        ceiling_resolve_cell(
            versions.iter().map(|(tid, b)| (*tid, b.as_slice())),
            coord,
            &params,
        )
    }
}
