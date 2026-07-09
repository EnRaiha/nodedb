// SPDX-License-Identifier: Apache-2.0

//! Version-history operations: version vectors, time-travel reads, targeted compaction, restore.

use loro::{LoroDoc, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};

use super::core::CrdtState;

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
    pub fn restore_to_version(
        &self,
        collection: &str,
        row_id: &str,
        version: &loro::VersionVector,
    ) -> Result<Vec<u8>> {
        let historical = self
            .read_at_version(collection, row_id, version)?
            .ok_or_else(|| CrdtError::Loro("document did not exist at target version".into()))?;

        let live = self.read_row(collection, row_id);
        if live.as_ref() == Some(&historical) {
            return Ok(Vec::new());
        }

        let vv_before = self.doc.oplog_vv();

        let fields: Vec<(&str, LoroValue)> = match &historical {
            LoroValue::Map(map) => map.iter().map(|(k, v)| (k.as_ref(), v.clone())).collect(),
            _ => return Err(CrdtError::Loro("historical state is not a map".into())),
        };
        self.upsert(collection, row_id, &fields)?;

        self.doc
            .export(loro::ExportMode::updates(&vv_before))
            .map_err(|e| CrdtError::Loro(format!("restore delta export: {e}")))
    }
}
