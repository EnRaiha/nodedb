// SPDX-License-Identifier: BUSL-1.1

use nodedb_graph::CsrIndex;
use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::CoreLoop;

impl CoreLoop {
    /// Shared-access view of a `(database, tenant)` CSR partition.
    ///
    /// Returns `None` if the tenant has no graph state for this database on
    /// this core — read paths treat that as "empty" rather than an error.
    #[inline]
    pub(in crate::data::executor) fn csr_partition(
        &self,
        database_id: u64,
        tid: u64,
    ) -> Option<&CsrIndex> {
        self.csr
            .partition(DatabaseId::new(database_id), TenantId::new(tid))
    }

    /// Mutable view of a `(database, tenant)` CSR partition, creating an empty
    /// one on first use. Canonical write-path entry point — resolves the
    /// database + tenant once, then all subsequent operations address
    /// unprefixed node names inside that partition.
    #[inline]
    pub(in crate::data::executor) fn csr_partition_mut(
        &mut self,
        database_id: u64,
        tid: u64,
    ) -> &mut CsrIndex {
        self.csr
            .get_or_create(DatabaseId::new(database_id), TenantId::new(tid))
    }

    /// Mark `node_id` as deleted within the caller's `(database, tenant)`.
    /// Used by PointDelete cascade so subsequent `EdgePut` to the same node
    /// is rejected as dangling.
    ///
    /// Returns `true` if this call newly inserted the node (it was not already
    /// marked deleted), `false` if the node was already present. A
    /// transactional caller uses this to decide whether a rollback should
    /// un-mark the node: only a newly-inserted tombstone may be reversed —
    /// un-marking a node that a prior committed op already deleted would
    /// wrongly resurrect it as a valid edge target.
    #[inline]
    pub(in crate::data::executor) fn mark_node_deleted(
        &mut self,
        database_id: u64,
        tid: u64,
        node_id: &str,
    ) -> bool {
        self.deleted_nodes
            .entry((DatabaseId::new(database_id), TenantId::new(tid)))
            .or_default()
            .insert(node_id.to_string())
    }

    /// Reverse a [`CoreLoop::mark_node_deleted`] by removing `node_id` from the
    /// caller's `(database, tenant)` deleted-nodes set. Called only on
    /// transaction rollback, and only for a node THIS transaction newly marked
    /// (see the `was_newly_marked` capture in `apply_point_delete`), so it
    /// never removes a pre-existing tombstone.
    #[inline]
    pub(in crate::data::executor) fn unmark_node_deleted(
        &mut self,
        database_id: u64,
        tid: u64,
        node_id: &str,
    ) {
        if let Some(set) = self
            .deleted_nodes
            .get_mut(&(DatabaseId::new(database_id), TenantId::new(tid)))
        {
            set.remove(node_id);
        }
    }

    /// Test whether `node_id` has been marked deleted within the
    /// caller's `(database, tenant)`.
    #[inline]
    pub(in crate::data::executor) fn is_node_deleted(
        &self,
        database_id: u64,
        tid: u64,
        node_id: &str,
    ) -> bool {
        self.deleted_nodes
            .get(&(DatabaseId::new(database_id), TenantId::new(tid)))
            .is_some_and(|s| s.contains(node_id))
    }
}
