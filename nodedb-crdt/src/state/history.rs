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

    /// Next op counter this document's own peer will author.
    ///
    /// Equivalently, how many ops this peer has authored so far. Looked up
    /// directly in the oplog version vector rather than tracked separately,
    /// so it can never drift from what `doc.oplog_vv()` actually reports.
    pub fn local_op_counter(&self) -> i32 {
        self.doc.oplog_vv().get(&self.peer_id).copied().unwrap_or(0)
    }

    /// Export exactly the ops this document's own peer authored in
    /// `[from_counter, to_counter)`.
    ///
    /// A deferred batch of local writes must be split into one
    /// self-contained delta per row so each can be committed independently
    /// downstream. Because each row's ops occupy a contiguous range in this
    /// peer's own op sequence (interleaving never happens within a single
    /// author), a bounded range export over that peer's `IdSpan` yields
    /// exactly that row's operations — nothing from before, nothing from
    /// after, and nothing from any other peer. Unlike `export_updates_since`,
    /// whose cost is proportional to everything after a version, this is
    /// bounded by the width of the requested span.
    ///
    /// An empty or inverted range (`to_counter <= from_counter`) is not an
    /// error — it simply exports nothing.
    pub fn export_local_range(&self, from_counter: i32, to_counter: i32) -> Result<Vec<u8>> {
        if to_counter <= from_counter {
            return Ok(Vec::new());
        }
        let span = loro::IdSpan::new(self.peer_id, from_counter, to_counter);
        self.doc
            .export(loro::ExportMode::updates_in_range(vec![span]))
            .map_err(|e| CrdtError::Loro(format!("bounded range export failed: {e}")))
    }

    /// Compact history at a specific version (not just current frontiers).
    ///
    /// Discards oplog entries before the target version. Current state and
    /// all versions after the target are preserved.
    pub fn compact_at_version(&mut self, version: &loro::VersionVector) -> Result<()> {
        self.compact_to_frontiers(&self.doc.vv_to_frontiers(version))
    }

    /// Generate a forward restore delta without changing authoritative state.
    ///
    /// The source must be quiescent: Loro's fork barrier can otherwise commit
    /// a pending source transaction. The working fork has a distinct peer ID,
    /// so its generated operations can safely be imported into the source.
    pub fn preview_restore_to_version(
        &self,
        collection: &str,
        row_id: &str,
        version: &loro::VersionVector,
    ) -> Result<Vec<u8>> {
        let pending_operations = self.doc.get_pending_txn_len();
        if pending_operations != 0 {
            return Err(CrdtError::PreviewSourceTransactionPending {
                operations: pending_operations,
            });
        }
        let historical = historical_row(&self.doc, collection, row_id, version)?;
        let historical_value = historical.get_value();
        if self.read_row(collection, row_id).as_ref() == Some(&historical_value) {
            return Ok(Vec::new());
        }

        let working = self.doc.fork();
        let base = working.oplog_vv();
        apply_restore_to_document(&working, collection, row_id, &historical)?;
        working
            .export(loro::ExportMode::updates(&base))
            .map_err(|e| CrdtError::Loro(format!("restore preview delta export: {e}")))
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
        let historical = historical_row(&self.doc, collection, row_id, version)?;
        if self.read_row(collection, row_id).as_ref() == Some(&historical.get_value()) {
            return Ok(Vec::new());
        }
        let vv_before = self.doc.oplog_vv();
        apply_restore_to_document(&self.doc, collection, row_id, &historical)?;
        self.doc
            .export(loro::ExportMode::updates(&vv_before))
            .map_err(|e| CrdtError::Loro(format!("restore delta export: {e}")))
    }
}

fn historical_row(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    version: &loro::VersionVector,
) -> Result<LoroMap> {
    let frontiers = doc.vv_to_frontiers(version);
    let forked = doc
        .fork_at(&frontiers)
        .map_err(|e| CrdtError::Loro(format!("fork at version: {e}")))?;
    match forked.get_map(collection).get(row_id) {
        Some(ValueOrContainer::Container(loro::Container::Map(row))) => Ok(row),
        Some(_) => Err(CrdtError::Loro("historical state is not a map".into())),
        None => Err(CrdtError::Loro(
            "document did not exist at target version".into(),
        )),
    }
}

fn apply_restore_to_document(
    doc: &LoroDoc,
    collection: &str,
    row_id: &str,
    historical_row: &LoroMap,
) -> Result<()> {
    let coll = doc.get_map(collection);
    let live_row = match coll.get(row_id) {
        Some(ValueOrContainer::Container(loro::Container::Map(row))) => row,
        _ => coll
            .insert_container(row_id, LoroMap::new())
            .map_err(|e| CrdtError::Loro(e.to_string()))?,
    };
    let historical_keys: HashSet<String> =
        historical_row.keys().map(|key| key.to_string()).collect();
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::CrdtState;

    fn prepared_state() -> (CrdtState, loro::VersionVector, Vec<u8>) {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert(
                "pages",
                "doc-1",
                &[("title", LoroValue::String("v1".into()))],
            )
            .expect("v1");
        state
            .list_insert_fields(
                "pages",
                "doc-1",
                "blocks",
                0,
                &[("id".into(), LoroValue::String("blk-0".into()))],
            )
            .expect("block");
        let historical_version = state.oplog_version_vector();
        state
            .upsert(
                "pages",
                "doc-1",
                &[("title", LoroValue::String("v2".into()))],
            )
            .expect("v2");
        let pre_restore = state.export_snapshot().expect("pre-restore snapshot");
        (state, historical_version, pre_restore)
    }

    #[test]
    fn preview_restore_is_non_mutating_and_replays_nested_containers() {
        let (state, historical_version, pre_restore) = prepared_state();
        let before_snapshot = state.export_snapshot().expect("before snapshot");
        let before_frontier = state.oplog_version_vector();
        let before_row = state.read_row("pages", "doc-1");

        let delta = state
            .preview_restore_to_version("pages", "doc-1", &historical_version)
            .expect("preview restore");
        assert!(!delta.is_empty());
        assert_eq!(
            state.export_snapshot().expect("after snapshot"),
            before_snapshot
        );
        assert_eq!(state.oplog_version_vector(), before_frontier);
        assert_eq!(state.read_row("pages", "doc-1"), before_row);

        state.import(&delta).expect("apply preview delta");
        assert_eq!(
            state.read_field("pages", "doc-1", "title"),
            Some(LoroValue::String("v1".into()))
        );
        assert_eq!(
            state
                .list_length("pages", "doc-1", "blocks")
                .expect("blocks"),
            1
        );

        let peer = CrdtState::new(2).expect("peer");
        peer.import(&pre_restore).expect("pre-state");
        peer.import(&delta).expect("restore delta");
        assert_eq!(
            peer.read_field("pages", "doc-1", "title"),
            Some(LoroValue::String("v1".into()))
        );
        assert_eq!(
            peer.list_length("pages", "doc-1", "blocks")
                .expect("blocks"),
            1
        );
    }

    #[test]
    fn preview_restore_to_current_version_is_empty() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("docs", "doc-1", &[("body", LoroValue::String("v1".into()))])
            .expect("write");
        state.doc.commit();
        let current = state.oplog_version_vector();
        assert!(
            state
                .preview_restore_to_version("docs", "doc-1", &current)
                .expect("preview")
                .is_empty()
        );
    }

    #[test]
    fn local_op_counter_starts_at_zero_and_advances() {
        let state = CrdtState::new(1).expect("state");
        assert_eq!(state.local_op_counter(), 0);

        state
            .upsert("docs", "a", &[("v", LoroValue::I64(1))])
            .expect("write a");
        state.doc.commit();
        let after_a = state.local_op_counter();
        assert!(after_a > 0);

        state
            .upsert("docs", "b", &[("v", LoroValue::I64(2))])
            .expect("write b");
        state.doc.commit();
        let after_b = state.local_op_counter();
        assert!(after_b > after_a);
    }

    #[test]
    fn export_local_range_round_trips_one_row() {
        let state = CrdtState::new(1).expect("state");

        let start_a = state.local_op_counter();
        state
            .upsert("docs", "a", &[("v", LoroValue::I64(1))])
            .expect("write a");
        state.doc.commit();
        let end_a = state.local_op_counter();

        state
            .upsert("docs", "b", &[("v", LoroValue::I64(2))])
            .expect("write b");
        state.doc.commit();

        let row_a_delta = state
            .export_local_range(start_a, end_a)
            .expect("export row a range");

        let target = CrdtState::new(2).expect("target state");
        target.import(&row_a_delta).expect("import row a delta");

        assert!(target.row_exists("docs", "a"));
        assert!(!target.row_exists("docs", "b"));
    }

    #[test]
    fn empty_range_exports_nothing() {
        let state = CrdtState::new(1).expect("state");
        state
            .upsert("docs", "a", &[("v", LoroValue::I64(1))])
            .expect("write a");
        state.doc.commit();

        let delta = state.export_local_range(5, 5).expect("empty range export");
        assert!(delta.is_empty());

        // An empty range yields no bytes at all — there is nothing to send, and
        // an empty blob is NOT importable (it carries no header). Callers must
        // skip an empty export rather than enqueue or import it.
        let target = CrdtState::new(2).expect("target state");
        assert!(
            target.import(&delta).is_err(),
            "an empty export is not a valid delta; callers must skip it"
        );
        assert!(!target.row_exists("docs", "a"));
    }
}
