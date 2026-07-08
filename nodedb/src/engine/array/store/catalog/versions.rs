// SPDX-License-Identifier: BUSL-1.1

//! Cross-source (memtable + segment) version resolution helpers for
//! [`ArrayStore`]'s bitemporal scans.

use std::collections::HashSet;

use nodedb_array::segment::{TilePayload, extract_cell_bytes};
use nodedb_array::types::TileId;
use nodedb_array::types::coord::value::CoordValue;

use super::ArrayStore;
use crate::engine::array::memtable::Memtable;

impl ArrayStore {
    /// Collect every distinct `hilbert_prefix` present in any tile version,
    /// across both the memtable and all open segments.
    pub(super) fn all_hilbert_prefixes(&self) -> HashSet<u64> {
        let mut all_prefixes: HashSet<u64> = HashSet::new();
        for h in self.segments.values() {
            for entry in h.reader().tiles() {
                all_prefixes.insert(entry.tile_id.hilbert_prefix);
            }
        }
        for (tile_id, _) in self.memtable.iter() {
            all_prefixes.insert(tile_id.hilbert_prefix);
        }
        all_prefixes
    }

    /// Collect every distinct cell coordinate for a given `hilbert_prefix`,
    /// scanning all versions across the memtable and all segments. Order is
    /// insertion order (memtable coords first); callers that need a stable
    /// ordering must sort the result themselves.
    pub(super) fn distinct_coords_for_prefix(
        &self,
        prefix: u64,
    ) -> Result<Vec<Vec<CoordValue>>, nodedb_array::ArrayError> {
        let mut coords: Vec<Vec<CoordValue>> = Vec::new();

        // From memtable versions.
        for (_, buf) in self.memtable.iter_tile_versions(prefix, i64::MAX) {
            for coord_key in buf.all_coord_keys() {
                let coord = Memtable::decode_coord_key(coord_key)?;
                if !coords.contains(&coord) {
                    coords.push(coord);
                }
            }
        }

        // From segment versions (newest-first per segment, but we only need
        // coords here so order within a segment doesn't matter).
        for h in self.segments.values() {
            let reader = h.reader();
            for item in reader.iter_tile_versions(prefix, i64::MAX)? {
                let (_, tile_payload) = item?;
                if let TilePayload::Sparse(sparse) = &tile_payload {
                    let n = sparse.nnz() as usize;
                    for row in 0..n {
                        let coord: Vec<CoordValue> = sparse
                            .dim_dicts
                            .iter()
                            .map(|d| d.values[d.indices[row] as usize].clone())
                            .collect();
                        if !coords.contains(&coord) {
                            coords.push(coord);
                        }
                    }
                }
            }
        }

        Ok(coords)
    }

    /// Build a `(TileId, raw_bytes)` list for a specific `coord` across all
    /// versions (memtable + segments), ordered newest-first by `system_from_ms`.
    pub(super) fn cell_versions_for_coord(
        &self,
        hilbert_prefix: u64,
        coord: &[CoordValue],
        system_as_of: i64,
    ) -> Result<Vec<(TileId, Vec<u8>)>, nodedb_array::ArrayError> {
        let mut versions: Vec<(TileId, Vec<u8>)> = Vec::new();

        // Memtable (most recent writes, already newest-first from iter_tile_versions).
        for (tile_id, buf) in self
            .memtable
            .iter_tile_versions(hilbert_prefix, system_as_of)
        {
            if let Some(bytes) = buf.get_cell_bytes(coord) {
                versions.push((tile_id, bytes.to_vec()));
            }
        }

        // Segment versions — gather all qualifying versions across all segments,
        // then sort newest-first so memtable + segment ordering is correct.
        let mut seg_versions: Vec<(TileId, Vec<u8>)> = Vec::new();
        for h in self.segments.values() {
            let reader = h.reader();
            for item in reader.iter_tile_versions(hilbert_prefix, system_as_of)? {
                let (tile_id, tile_payload) = item?;
                if let TilePayload::Sparse(sparse) = &tile_payload
                    && let Some(bytes) = extract_cell_bytes(sparse, coord)?
                {
                    seg_versions.push((tile_id, bytes));
                }
            }
        }
        // Sort segment versions newest-first by system_from_ms.
        seg_versions.sort_by_key(|(a, _)| std::cmp::Reverse(a.system_from_ms));
        versions.extend(seg_versions);

        Ok(versions)
    }
}
