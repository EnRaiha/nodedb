// SPDX-License-Identifier: BUSL-1.1

//! Synchronous hard-purge used by the re-CREATE path.
//!
//! `CREATE COLLECTION` of a name that already exists but is
//! soft-deleted (`is_active == false`) is an explicit request for a
//! fresh collection — distinct from `UNDROP` recovery. Soft-delete
//! keeps the old rows under the same `{db}:{tenant}:{name}:` storage
//! prefix for the retention window; re-creating the name before GC
//! runs would resurrect those stale rows. Before the new collection
//! can register over the reused prefix, the old catalog row AND the
//! old Data Plane storage keys must be gone.
//!
//! This composes the two halves the `DROP COLLECTION ... PURGE`
//! applier runs on every node — the catalog-row removal
//! (`apply::collection::purge`) and the storage reclaim
//! (`post_apply::async_dispatch::collection::reclaim_collection_storage`) —
//! into one awaitable the
//! Control Plane can drive inline before it proceeds to persist the
//! new collection. No engine is special-cased: the reclaim dispatch
//! covers every engine exactly as `DROP ... PURGE` does.

use crate::control::catalog_entry::post_apply::ReclaimFailure;
use crate::control::state::SharedState;

/// Hard-purge `(tenant_id, name)`: remove the catalog metadata row
/// (primary `StoredCollection` + owner + surrogate map) and reclaim
/// every engine's Data Plane storage for the collection, awaiting
/// completion so both are done before the caller proceeds.
///
/// `purge_lsn` is the WAL tombstone boundary: writes with
/// `lsn < purge_lsn` for this collection are shadowed on replay, so
/// callers pass the current WAL `next_lsn` — every pre-drop row sits
/// below it while every post-CREATE row sits at or above it.
///
/// Returns `Err` if the catalog-row removal OR the engine storage
/// reclaim fails, so the re-CREATE caller can ABORT rather than register
/// a new collection over un-purged data (the failure-path resurrection
/// hole). This interactive path is fail-closed on both halves.
///
/// The storage-reclaim half (`reclaim_collection_storage`) is
/// result-checked: its correctness-critical redb + versioned engine
/// purge propagates a failure here (and also records a durable
/// `_system.pending_reclaim` entry so the purge is retried
/// at-least-once even though this caller aborts). Its best-effort
/// substeps (WAL tombstone, redb tombstone, L2 enqueue, quiesce drain,
/// Lite broadcast) still log-and-continue.
pub(crate) async fn hard_purge_collection(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
    drain_already_held: bool,
) -> Result<(), ReclaimFailure> {
    // 1. Remove the catalog metadata row (primary StoredCollection,
    //    owner row, surrogate map) — the synchronous half of the
    //    `PurgeCollection` applier. Propagate failure: if the old row
    //    survives, the new collection must NOT register over it. This runs
    //    before any durable retry record is queued, so it is a `no_retry`
    //    failure — the caller releases its lifecycle guard.
    {
        let catalog = state.credentials.catalog();
        crate::control::catalog_entry::apply::collection::prepare_purge(
            database_id,
            tenant_id,
            name,
            catalog,
        )
        .map_err(ReclaimFailure::no_retry)?;
    }

    // 2. Reclaim engine-local storage on the Data Plane (WAL tombstone,
    //    redb tombstone, quiesce drain, `MetaOp::UnregisterCollection`
    //    dispatch, Lite `CollectionPurged` broadcast) — the async half
    //    of the `PurgeCollection` post-apply, shared verbatim.
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
