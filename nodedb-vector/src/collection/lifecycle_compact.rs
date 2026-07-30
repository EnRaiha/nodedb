// SPDX-License-Identifier: Apache-2.0

//! Compact and snapshot operations for `VectorCollection`.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;

/// One exported vector: global node id, full-precision data, optional surrogate.
pub type ExportedVector = (u32, Vec<f32>, Option<Surrogate>);

impl VectorCollection {
    /// Compact sealed segments by removing tombstoned nodes.
    ///
    /// Rewrites `surrogate_map` and `multi_doc_map` for every sealed
    /// segment so that global ids continue to resolve to the correct
    /// surrogate after local-id renumbering.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            let base_id = seg.base_id;
            let (removed, id_map) = seg.index.compact_with_map();
            total_removed += removed;
            if removed == 0 {
                continue;
            }

            let segment_end = base_id as u64 + id_map.len() as u64;
            let global_keys: Vec<u32> = self
                .surrogate_map
                .keys()
                .copied()
                .filter(|&k| (k as u64) >= base_id as u64 && (k as u64) < segment_end)
                .collect();
            // Two-phase: remove old entries first, then insert new ones
            // so we don't clobber a freshly-remapped entry with a later
            // tombstone removal.
            let mut new_entries: Vec<(u32, Surrogate)> = Vec::with_capacity(global_keys.len());
            for old_global in &global_keys {
                let surrogate = self.surrogate_map.remove(old_global);
                let old_local = (old_global - base_id) as usize;
                let new_local = id_map[old_local];
                if new_local != u32::MAX
                    && let Some(s) = surrogate
                {
                    new_entries.push((base_id + new_local, s));
                } else if let Some(s) = surrogate {
                    // Tombstoned — drop reverse mapping too.
                    self.surrogate_to_local.remove(&s);
                }
            }
            for (k, s) in new_entries {
                self.surrogate_map.insert(k, s);
                self.surrogate_to_local.insert(s, k);
            }

            // Rewrite multi_doc_map entries for this segment.
            for ids in self.multi_doc_map.values_mut() {
                ids.retain_mut(|vid| {
                    let v = *vid;
                    if (v as u64) >= base_id as u64 && (v as u64) < segment_end {
                        let old_local = (v - base_id) as usize;
                        let new_local = id_map[old_local];
                        if new_local == u32::MAX {
                            false
                        } else {
                            *vid = base_id + new_local;
                            true
                        }
                    } else {
                        true
                    }
                });
            }
        }
        total_removed
    }

    /// Export all live vectors for snapshot.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::HnswIndex::export_vectors`] if a sealed segment's
    /// vectors cannot be materialized. A snapshot that silently dropped or
    /// zeroed those vectors would restore an index with missing data.
    pub fn export_snapshot(&self) -> Result<Vec<ExportedVector>, crate::error::VectorError> {
        let mut result = Vec::new();

        for i in 0..self.growing.len() as u32 {
            let vid = self.growing_base_id + i;
            if let Some(data) = self.growing.get_vector(i) {
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, data.to_vec(), surrogate));
            }
        }

        for seg in &self.sealed {
            let vectors = seg.index.export_vectors()?;
            for (i, vec_data) in vectors.into_iter().enumerate() {
                let vid = seg.base_id + i as u32;
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, vec_data, surrogate));
            }
        }

        for seg in &self.building {
            for i in 0..seg.flat.len() as u32 {
                let vid = seg.base_id + i;
                if let Some(data) = seg.flat.get_vector(i) {
                    let surrogate = self.surrogate_map.get(&vid).copied();
                    result.push((vid, data.to_vec(), surrogate));
                }
            }
        }

        Ok(result)
    }
}
