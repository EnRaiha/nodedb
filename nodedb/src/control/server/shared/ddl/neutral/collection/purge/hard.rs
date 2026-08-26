// SPDX-License-Identifier: BUSL-1.1

//! Synchronous hard-purge used by the re-CREATE path.
//!
//! `CREATE COLLECTION` over a soft-deleted name must not resurrect stale rows
//! under the reused storage prefix. Composes `DROP COLLECTION ... PURGE`'s
//! catalog removal and storage reclaim into one awaitable driven inline first.

use crate::control::catalog_entry::post_apply::ReclaimFailure;
use crate::control::state::SharedState;

/// Remove the catalog row and reclaim every engine's storage, awaiting both.
/// `purge_lsn` is the WAL tombstone boundary (`next_lsn`). An absent catalog
/// row is an error — reclaiming under a live row deletes live data.
pub(crate) async fn hard_purge_collection(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
    drain_already_held: bool,
) -> Result<(), ReclaimFailure> {
    // If the old row survives, the new collection must not register over it.
    // Runs before any retry record queues, so failure here is `no_retry`.
    {
        let catalog = state.credentials.catalog();
        crate::control::catalog_entry::apply::collection::prepare_purge_checked(
            database_id,
            tenant_id,
            name,
            catalog,
        )
        .map_err(ReclaimFailure::no_retry)?;
    }

    // Reclaim engine-local storage — the async half of `PurgeCollection` post-apply.
    crate::control::catalog_entry::post_apply::reclaim_collection_storage(
        state,
        database_id,
        tenant_id,
        name,
        purge_lsn,
        drain_already_held,
    )
    .await?;

    Ok(())
}
