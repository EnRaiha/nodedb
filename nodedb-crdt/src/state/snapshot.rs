// SPDX-License-Identifier: Apache-2.0

//! Snapshot export/import, history compaction, memory estimation.

use loro::LoroDoc;

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::import_admission::{CrdtImportLimits, admit_import};

impl CrdtState {
    /// Export the current state as bytes for sync.
    pub fn export_snapshot(&self) -> Result<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| CrdtError::Loro(format!("snapshot export failed: {e}")))
    }

    /// Import remote updates under the finite default byte and operation caps.
    pub fn import(&self, data: &[u8]) -> Result<()> {
        self.import_with_limits(data, CrdtImportLimits::default())
    }

    /// Import remote updates after bounded authenticated metadata admission.
    ///
    /// The limits are checked before Loro can import the blob, so rejected
    /// bytes never mutate this document or allocate an import graph.
    pub fn import_with_limits(&self, data: &[u8], limits: CrdtImportLimits) -> Result<()> {
        admit_import(data, &self.doc.oplog_vv(), limits)?;
        self.doc
            .import(data)
            .map_err(|e| CrdtError::DeltaApplyFailed(e.to_string()))?;
        Ok(())
    }

    /// Compact the CRDT history by replacing the internal LoroDoc with a
    /// shallow snapshot.
    ///
    /// A shallow snapshot contains the current state but discards the
    /// full operation history. This is the CRDT equivalent of WAL
    /// truncation after checkpoint.
    ///
    /// After compaction:
    /// - All current state is preserved (reads return same values).
    /// - New deltas can still be applied and merged.
    /// - Historical operations before the snapshot point are gone.
    /// - Peers that sync after compaction receive a full snapshot
    ///   instead of incremental deltas (acceptable for long-offline peers).
    ///
    /// Call this periodically (e.g., every 30 minutes or when memory
    /// pressure exceeds threshold) to prevent unbounded history growth.
    pub fn compact_history(&mut self) -> Result<()> {
        // Export a shallow snapshot at the current frontiers.
        let frontiers = self.doc.oplog_frontiers();
        let snapshot = self
            .doc
            .export(loro::ExportMode::shallow_snapshot(&frontiers))
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot export: {e}")))?;

        // Replace the doc with a fresh one loaded from the snapshot.
        let new_doc = LoroDoc::new();
        new_doc
            .set_peer_id(self.peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id on compacted doc: {e}")))?;
        // This snapshot is generated locally, but it remains finite and goes
        // through the same metadata/range admission before import.
        admit_import(&snapshot, &new_doc.oplog_vv(), CrdtImportLimits::default())?;
        new_doc
            .import(&snapshot)
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot import: {e}")))?;

        self.doc = new_doc;
        Ok(())
    }

    /// Estimated memory usage of the CRDT state (bytes).
    ///
    /// Includes operation history, current state, and internal caches.
    /// Use this to decide when to trigger `compact_history()`.
    pub fn estimated_memory_bytes(&self) -> usize {
        // Loro doesn't expose a direct memory metric.
        // Use snapshot size as a proxy — it's proportional to state size.
        // This is not precise but good enough for pressure monitoring.
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;

    fn source_delta() -> Vec<u8> {
        let source = CrdtState::new(7).expect("source state");
        source
            .upsert(
                "docs",
                "row",
                &[("title", LoroValue::String("value".into()))],
            )
            .expect("source write");
        let empty = loro::VersionVector::default();
        source.export_updates_since(&empty).expect("source delta")
    }

    #[test]
    fn import_rejects_oversize_before_state_mutation() {
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        let result = state.import_with_limits(
            &[0u8; 2],
            CrdtImportLimits {
                max_bytes: 1,
                max_encoded_operations: 1,
                max_new_operations: 1,
            },
        );
        assert!(matches!(result, Err(CrdtError::ImportTooLarge { .. })));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn import_rejects_malformed_metadata_before_state_mutation() {
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        assert!(matches!(
            state.import(&[0xff]),
            Err(CrdtError::ImportMalformed { .. })
        ));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn import_rejects_operation_limit_before_state_mutation() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        let result = state.import_with_limits(
            &delta,
            CrdtImportLimits {
                max_bytes: delta.len(),
                max_encoded_operations: 0,
                max_new_operations: 0,
            },
        );
        assert!(matches!(
            result,
            Err(CrdtError::ImportOperationLimitExceeded { .. })
        ));
        assert_eq!(state.frontier(), before);
        assert!(!state.row_exists("docs", "row"));
    }

    #[test]
    fn already_known_operations_still_count_toward_decode_budget() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        state.import(&delta).expect("initial import");
        let before = state.frontier();
        let result = state.import_with_limits(
            &delta,
            CrdtImportLimits {
                max_bytes: delta.len(),
                max_encoded_operations: 0,
                max_new_operations: 0,
            },
        );
        assert!(matches!(
            result,
            Err(CrdtError::ImportOperationLimitExceeded { .. })
        ));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn stale_replay_after_receiver_advances_is_idempotent() {
        let source = CrdtState::new(7).expect("source");
        let empty = loro::VersionVector::default();
        source
            .upsert("docs", "row", &[("value", LoroValue::I64(1))])
            .expect("first write");
        let stale = source.export_updates_since(&empty).expect("stale delta");
        source
            .set_fields("docs", "row", &[("value", LoroValue::I64(2))])
            .expect("second write");
        let current = source.export_updates_since(&empty).expect("current delta");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&current).expect("advance receiver");
        let before = receiver.frontier();
        receiver.import(&stale).expect("stale replay is idempotent");
        assert_eq!(receiver.frontier(), before);
    }

    #[test]
    fn bounded_import_accepts_valid_delta_and_snapshot() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        state.import(&delta).expect("bounded delta import");
        assert!(state.row_exists("docs", "row"));

        let snapshot = state.export_snapshot().expect("snapshot");
        let restored = CrdtState::new(2).expect("restored state");
        restored.import(&snapshot).expect("bounded snapshot import");
        assert!(restored.row_exists("docs", "row"));
    }
}
