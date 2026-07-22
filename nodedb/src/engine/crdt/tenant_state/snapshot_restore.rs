// SPDX-License-Identifier: BUSL-1.1

//! Exact collection-state replacement for CRDT transaction rollback.

use nodedb_crdt::state::CrdtState;

use super::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Replace one collection's state with an exact pre-image.
    ///
    /// Normal snapshot import is a monotonic Loro merge and therefore cannot
    /// undo a delta already imported into the same `LoroDoc`. Transaction
    /// rollback needs replacement semantics instead: construct and validate a
    /// fresh document first, then atomically replace this collection's entry.
    /// `None` restores the prior absence of the collection.
    pub(crate) fn restore_collection_snapshot(
        &mut self,
        collection: &str,
        snapshot: Option<&[u8]>,
    ) -> crate::Result<()> {
        let Some(snapshot) = snapshot else {
            self.collections.remove(collection);
            return Ok(());
        };

        // Do every fallible step before mutating `collections`, so an invalid
        // rollback token cannot discard the current state while reporting an
        // error to the transaction driver.
        let replacement = CrdtState::new(self.peer_id).map_err(crate::Error::Crdt)?;
        replacement.import(snapshot).map_err(crate::Error::Crdt)?;
        self.collections.insert(collection.to_owned(), replacement);
        Ok(())
    }
}
