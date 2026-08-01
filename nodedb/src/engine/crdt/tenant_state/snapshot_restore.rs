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
        // Same per-collection derivation as `state_mut`: a rollback must not
        // hand the collection back a document whose operation identities
        // collide with a sibling collection's.
        //
        // The pre-image is a snapshot this process exported when the
        // transaction opened, so it is admitted as local: under the peer
        // ceilings a collection that grew past them could be written but never
        // rolled back, and the transaction driver would be handed a failure it
        // has no way to act on.
        let replacement = CrdtState::from_local_snapshot(
            Self::collection_peer_id(self.peer_id, collection),
            snapshot,
        )
        .map_err(crate::Error::Crdt)?;
        self.collections.insert(collection.to_owned(), replacement);
        Ok(())
    }
}
