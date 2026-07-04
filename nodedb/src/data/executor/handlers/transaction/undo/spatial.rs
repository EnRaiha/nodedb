// SPDX-License-Identifier: BUSL-1.1

//! Spatial-engine undo entry application logic.
//!
//! Spatial index mutations are IN-MEMORY (the per-field R-tree in
//! `spatial_indexes` plus the reverse `spatial_doc_map`), so an aborted redb
//! write transaction does NOT reverse them — they require explicit undo. This
//! mirrors the vector-index undo path (`apply_undo_vector`).
//!
//! Returns `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use crate::data::executor::core_loop::CoreLoop;

use super::UndoEntry;

impl CoreLoop {
    pub(super) fn apply_undo_spatial(
        &mut self,
        _entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::SpatialInsert { key, entry_id } => {
                // Reverse a forward spatial insert: drop the R-tree entry and
                // its reverse map record. A missing index means the entry was
                // never created (nothing to undo) — safe no-op.
                if let Some(rtree) = self.spatial_indexes.get_mut(&key) {
                    rtree.delete(entry_id);
                }
                self.spatial_doc_map
                    .remove(&(key.0, key.1, key.2, key.3, entry_id));
                Ok(())
            }
            UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
            } => {
                // Reverse a forward spatial removal: re-insert the entry with
                // its captured bbox and re-populate the reverse map, matching
                // the forward `apply_point_put_spatial` insert shape.
                let rtree = self.spatial_indexes.entry(key.clone()).or_default();
                rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                self.spatial_doc_map
                    .insert((key.0, key.1, key.2, key.3, entry_id), document_id);
                Ok(())
            }
            _ => unreachable!("apply_undo_spatial called with non-spatial entry"),
        }
    }
}
