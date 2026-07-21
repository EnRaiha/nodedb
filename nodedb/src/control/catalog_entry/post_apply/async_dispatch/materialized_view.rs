// SPDX-License-Identifier: BUSL-1.1

//! Synchronous post-apply reclaim for `CatalogEntry::DeleteMaterializedView`.
//!
//! The replicated catalog entry removes both the materialized-view definition
//! and its implementation-owned target collection. Every node must then
//! reclaim that target through the same collection-wide Data Plane path before
//! advancing its metadata applied index. This prevents a same-name re-CREATE
//! from observing predecessor rows, indexes, or derived cache entries.

use std::sync::Arc;

use crate::control::state::SharedState;

/// Reclaim the dropped view's target collection on this node.
///
/// `reclaim_collection_storage` provides the durable pending-reclaim fallback
/// and dispatches `UnregisterCollection` to every local Data Plane core. The
/// caller treats an error as fatal because the catalog deletion is already
/// committed; serving through an incomplete reclaim would violate object
/// incarnation isolation.
pub async fn delete_async(
    tenant_id: u64,
    name: String,
    purge_lsn: u64,
    shared: Arc<SharedState>,
) -> Result<(), super::collection::ReclaimFailure> {
    super::collection::reclaim_collection_storage(&shared, 0, tenant_id, &name, purge_lsn, false)
        .await
}
