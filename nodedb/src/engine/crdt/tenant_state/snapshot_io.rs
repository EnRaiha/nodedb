// SPDX-License-Identifier: BUSL-1.1

//! Snapshot export and import for a tenant's per-collection CRDT state.

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Export one collection's CRDT state as binary snapshot bytes.
    ///
    /// Returns `None` when the collection has no local state.
    pub fn export_snapshot_bytes(&self, collection: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) => state
                .export_snapshot()
                .map(Some)
                .map_err(crate::Error::Crdt),
            None => Ok(None),
        }
    }

    /// Export every collection's snapshot as `(collection, bytes)` pairs.
    pub fn export_all_snapshots(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::with_capacity(self.collections.len());
        for (collection, state) in &self.collections {
            let bytes = state.export_snapshot().map_err(crate::Error::Crdt)?;
            out.push((collection.clone(), bytes));
        }
        Ok(out)
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes for the
    /// document's collection. `None` when the collection or row is absent.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) if state.row_exists(collection, row_id) => {
                Ok(Some(state.export_snapshot().map_err(crate::Error::Crdt)?))
            }
            _ => Ok(None),
        }
    }

    /// Import a full CRDT snapshot for a single collection (snapshot restore).
    ///
    /// Fails when the blob's operations cannot be fully applied — a restore
    /// that left operations causally pending has NOT restored the collection,
    /// and reporting success would leave the caller unable to tell a complete
    /// restore from a partial one.
    pub fn import_snapshot_bytes(&mut self, collection: &str, bytes: &[u8]) -> crate::Result<()> {
        self.state_mut(collection)?
            .import(bytes)
            .map_err(crate::Error::Crdt)
    }
}
