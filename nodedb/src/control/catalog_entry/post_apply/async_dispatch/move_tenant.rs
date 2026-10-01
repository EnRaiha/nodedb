// SPDX-License-Identifier: BUSL-1.1

//! Source-storage reclaim for `CatalogEntry::MoveTenantCutover`.
//!
//! A collection's storage key and home vShard both hash its database. The
//! cutover re-issues every moved row into the target database before it
//! proposes the entry, so the rows under the source key are dead on every
//! node. Each node reclaims them through the collection purge path: WAL and
//! redb tombstones, then `UnregisterCollection` on every local core.
//!
//! A replayed entry finds nothing under the source key, and the purge of an
//! absent collection is a no-op.

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

use super::collection::{ReclaimFailure, reclaim_collection_storage};

/// Reclaim the source-keyed storage of every moved collection on this node.
///
/// A collection whose reclaim queued a durable retry does not stop the loop:
/// the pending-reclaim worker owns it, and the remaining collections still
/// need their reclaim. The first such failure is returned once every
/// collection was tried. A failure with no retry queued returns at once.
pub(crate) async fn reclaim_moved_sources(
    shared: &SharedState,
    source_db_id: u64,
    collections: &[StoredCollection],
) -> Result<(), ReclaimFailure> {
    let mut queued: Option<ReclaimFailure> = None;
    for coll in collections {
        // The purge boundary is a WAL LSN of this node: every source write
        // this node holds sits below it.
        let purge_lsn = shared.wal.next_lsn().as_u64();
        match reclaim_collection_storage(
            shared,
            source_db_id,
            coll.tenant_id,
            &coll.name,
            purge_lsn,
            false,
        )
        .await
        {
            Ok(()) => {}
            Err(failure) if failure.retry_queued => {
                queued.get_or_insert(failure);
            }
            Err(failure) => return Err(failure),
        }
    }
    match queued {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}
