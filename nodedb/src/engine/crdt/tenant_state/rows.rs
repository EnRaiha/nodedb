// SPDX-License-Identifier: BUSL-1.1

//! Row reads, bitemporal history access, and collection purge.

use loro::LoroValue;

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Read a single row's fields as a `LoroValue`, or `None` if absent.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row(collection, row_id))
    }

    /// Check if a row exists in a collection's document store.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|state| state.row_exists(collection, row_id))
    }

    /// Read the row as it was at `asof_ms` (system-time). Returns `None` when
    /// the collection is absent or no version existed at or before that time.
    pub fn read_row_as_of(
        &self,
        collection: &str,
        row_id: &str,
        asof_ms: i64,
    ) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row_as_of(collection, row_id, asof_ms))
    }

    /// Count archived (superseded) bitemporal versions for a row.
    /// Returns `0` when the collection has no local state.
    pub fn archive_version_count(&self, collection: &str, row_id: &str) -> usize {
        self.collections
            .get(collection)
            .map(|state| state.archive_version_count(collection, row_id))
            .unwrap_or(0)
    }

    /// Drop archived bitemporal versions older than `cutoff_system_ms`
    /// for the given collection. The live row is never touched. Called
    /// from the Data Plane purge handler. A collection with no local state
    /// purges nothing.
    pub fn purge_history_before(
        &self,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        match self.collections.get(collection) {
            Some(state) => state
                .purge_history_before(collection, cutoff_system_ms)
                .map_err(crate::Error::Crdt),
            None => Ok(0),
        }
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Purge all CRDT state for a single collection.
    ///
    /// Four things happen:
    /// 1. Every row in the collection's Loro doc is cleared.
    /// 2. The collection's conflict-resolution policy is removed from
    ///    the policy registry.
    /// 3. The collection's installed constraints and their version fence
    ///    are cleared — otherwise a re-created collection of the same name
    ///    would be validated against the dropped collection's constraints,
    ///    and (because its constraint version restarts at 1) its fresh
    ///    constraint install would be rejected as stale by the fence.
    /// 4. Any dead-letter entries (rejected deltas) scoped to this
    ///    collection are dropped — otherwise a re-created collection
    ///    of the same name would inherit unrelated rejected deltas.
    ///
    /// Returns the number of CRDT rows removed. Idempotent.
    pub fn purge_collection(&mut self, collection: &str) -> crate::Result<usize> {
        let removed = match self.collections.get(collection) {
            Some(state) => state
                .clear_collection(collection)
                .map_err(crate::Error::Crdt)?,
            None => 0,
        };
        self.validator.policies_mut().remove(collection);
        self.validator.clear_collection_constraints(collection);
        self.constraint_versions.remove(collection);
        let dlq_dropped = self
            .validator
            .dlq_mut()
            .purge_collection(self.tenant_id.as_u64(), collection);
        if dlq_dropped > 0 {
            tracing::debug!(
                tenant = self.tenant_id.as_u64(),
                collection,
                dlq_dropped,
                "crdt: dropped DLQ entries scoped to purged collection"
            );
        }
        Ok(removed)
    }
}
