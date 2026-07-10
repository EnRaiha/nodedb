// SPDX-License-Identifier: Apache-2.0

//! Version-history operations: version vectors, time-travel reads, targeted compaction, restore.

use std::collections::HashSet;

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::restore_containers;

impl CrdtState {
    /// Get the current oplog version vector.
    pub fn oplog_version_vector(&self) -> loro::VersionVector {
        self.doc.oplog_vv()
    }

    /// Read the document state at a historical version.
    ///
    /// Uses `fork_at` to create a lightweight copy at the target version
    /// and reads the specified row. Returns `None` if the row didn't exist.
    ///
    /// Cost: O(oplog_size) for the fork — not for hot-path queries.
    pub fn read_at_version(
        &self,
        collection: &str,
        row_id: &str,
        version: &loro::VersionVector,
    ) -> Result<Option<LoroValue>> {
        let frontiers = self.doc.vv_to_frontiers(version);
        let forked = self
            .doc
            .fork_at(&frontiers)
            .map_err(|e| CrdtError::Loro(format!("fork at version: {e}")))?;

        let coll = forked.get_map(collection);
        match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => Ok(Some(m.get_value())),
            Some(ValueOrContainer::Container(loro::Container::List(l))) => Ok(Some(l.get_value())),
            Some(ValueOrContainer::Value(v)) => Ok(Some(v)),
            Some(ValueOrContainer::Container(_)) => Ok(Some(LoroValue::Null)),
            None => Ok(None),
        }
    }

    /// Export the oplog delta from a version to the current state.
    ///
    /// Returns the operations that transform `from_version` into current state.
    /// Used for DIFF rendering and delta sync.
    pub fn export_updates_since(&self, from_version: &loro::VersionVector) -> Result<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::updates(from_version))
            .map_err(|e| CrdtError::Loro(format!("delta export: {e}")))
    }

    /// Compact history at a specific version (not just current frontiers).
    ///
    /// Discards oplog entries before the target version. Current state and
    /// all versions after the target are preserved.
    pub fn compact_at_version(&mut self, version: &loro::VersionVector) -> Result<()> {
        let frontiers = self.doc.vv_to_frontiers(version);
        let snapshot = self
            .doc
            .export(loro::ExportMode::shallow_snapshot(&frontiers))
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot export: {e}")))?;

        let new_doc = LoroDoc::new();
        new_doc
            .set_peer_id(self.peer_id)
            .map_err(|e| CrdtError::Loro(format!("set peer_id on compacted doc: {e}")))?;
        new_doc
            .import(&snapshot)
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot import: {e}")))?;

        self.doc = new_doc;
        Ok(())
    }

    /// Restore a document to a historical version by creating a forward delta.
    ///
    /// Reads the state at the target version, then generates a new mutation
    /// that sets the current state to match the historical state. History is
    /// preserved — this is a forward operation, not a rollback.
    ///
    /// Short-circuits before mutating when the historical row projection
    /// already equals the live row: `doc.export(ExportMode::updates(vv))`
    /// always writes a small magic/checksum/mode header regardless of
    /// whether any ops fall in range, so a caller checking
    /// `bytes.is_empty()` on a post-write export would never see `true` for
    /// a no-op restore. Comparing projections up front avoids emitting a
    /// write (and the header-only export) at all.
    ///
    /// Returns the delta bytes to be applied through the normal write path,
    /// or a genuinely empty `Vec` — with no row mutation performed — when
    /// restoring would not change the live row (e.g. restoring to the
    /// version the document is already at).
    ///
    /// Historical fields are inspected on the *live* forked container (via
    /// `LoroMap::get` → `ValueOrContainer`), not the flattened
    /// `read_at_version` projection: scalar entries are replaced the same
    /// way `upsert` replaces them, but container-shaped entries (e.g. a
    /// Notion-style block list) are rebuilt structurally via
    /// `insert_container` plus recursive repopulation — see
    /// `restore_containers` — so restoring a row never collapses its nested
    /// CRDT containers into plain flattened values.
    pub fn restore_to_version(
        &self,
        collection: &str,
        row_id: &str,
        version: &loro::VersionVector,
    ) -> Result<Vec<u8>> {
        let frontiers = self.doc.vv_to_frontiers(version);
        let forked = self
            .doc
            .fork_at(&frontiers)
            .map_err(|e| CrdtError::Loro(format!("fork at version: {e}")))?;
        let forked_coll = forked.get_map(collection);
        let historical_row = match forked_coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            Some(_) => return Err(CrdtError::Loro("historical state is not a map".into())),
            None => {
                return Err(CrdtError::Loro(
                    "document did not exist at target version".into(),
                ));
            }
        };
        let historical_value = historical_row.get_value();

        let live = self.read_row(collection, row_id);
        if live.as_ref() == Some(&historical_value) {
            return Ok(Vec::new());
        }

        let vv_before = self.doc.oplog_vv();

        let coll = self.doc.get_map(collection);
        let live_row = match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => coll
                .insert_container(row_id, LoroMap::new())
                .map_err(|e| CrdtError::Loro(e.to_string()))?,
        };

        // Full-projection replace against the historical row's own key set —
        // restoring is authoritative: any key live but absent historically
        // is dropped, matching the pre-fix destroy-and-recreate behavior for
        // every field the old flattening path could express.
        let historical_keys: HashSet<String> =
            historical_row.keys().map(|k| k.to_string()).collect();
        let keys_to_delete: Vec<String> = live_row
            .keys()
            .filter(|key| !historical_keys.contains(key.as_ref()))
            .map(|key| key.to_string())
            .collect();
        for key in &keys_to_delete {
            live_row
                .delete(key)
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }

        for key in historical_row.keys() {
            if let Some(value) = historical_row.get(&key) {
                restore_containers::rebuild_map_field(&live_row, &key, value)?;
            }
        }

        self.doc
            .export(loro::ExportMode::updates(&vv_before))
            .map_err(|e| CrdtError::Loro(format!("restore delta export: {e}")))
    }
}
