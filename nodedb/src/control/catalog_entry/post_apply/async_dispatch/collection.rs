// SPDX-License-Identifier: BUSL-1.1

//! Collection-specific async post-apply dispatchers.
//!
//! Runs on **every node** (via `spawn_post_apply_async_side_effects`
//! in `apply_replicated`). Each node's local Data Plane observes
//! catalog mutations symmetrically.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::catalog_entry::post_apply::collection;
use crate::control::security::catalog::{StoredCollection, StoredL2CleanupEntry};
use crate::control::state::SharedState;
use crate::types::TenantId;

pub async fn put_async(stored: StoredCollection, shared: Arc<SharedState>) {
    collection::put_async(stored, shared).await;
}

/// Dispatch `MetaOp::UnregisterCollection` to this node's local Data
/// Plane so every engine reclaims storage for the purged collection.
/// Called on every node (not just the leader), so each node's Data
/// Plane reclaims its own L1 segment files, memtables, compaction
/// debt, and WAL tombstone entries locally.
///
/// Order of operations:
///
/// 1. Persist the tombstone into the local `_system.wal_tombstones`
///    redb table. Makes startup replay O(1) and survives process
///    crashes between steps 2 and 3.
/// 2. Append a `CollectionTombstoned` record to the local WAL. Replay
///    constructs the same in-memory set whether or not step 1 committed
///    (belt + suspenders — if the redb write failed but WAL succeeded,
///    replay still sees the tombstone).
/// 3. Dispatch `MetaOp::UnregisterCollection` into the Data Plane to
///    reclaim engine-local storage.
pub async fn purge_async(tenant_id: u64, name: String, purge_lsn: u64, shared: Arc<SharedState>) {
    reclaim_collection_storage(&shared, tenant_id, &name, purge_lsn).await;
}

/// Borrowed core of [`purge_async`]: reclaim every engine's storage for
/// `(tenant_id, name)` on this node — WAL tombstone, redb tombstone,
/// optional L2 cleanup enqueue, quiesce drain, `MetaOp::UnregisterCollection`
/// dispatch to the local Data Plane, and Lite `CollectionPurged` broadcast.
///
/// Split out from `purge_async` so the synchronous re-CREATE hard-purge
/// (`shared::ddl::neutral::collection::purge::hard_purge_collection`) can reuse the exact
/// same reclaim body against a `&SharedState` without an owned `Arc`, and
/// so the raft post-apply path and the re-CREATE path share one
/// implementation rather than a copy.
pub(crate) async fn reclaim_collection_storage(
    shared: &SharedState,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
) {
    // 1. Persist to redb (every node has its own catalog).
    if let Some(catalog) = shared.credentials.catalog()
        && let Err(e) = catalog.record_wal_tombstone(tenant_id, name, purge_lsn)
    {
        warn!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            error = %e,
            "failed to persist WAL tombstone to _system.wal_tombstones — \
             replay will fall back to WAL extraction"
        );
    }

    // 2. Append to local WAL.
    if let Err(e) =
        shared
            .wal
            .append_collection_tombstone(TenantId::new(tenant_id), name, purge_lsn)
    {
        warn!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            error = %e,
            "failed to append CollectionTombstoned WAL record"
        );
    }

    // 2b. Enqueue an L2 cleanup entry if cold storage is configured.
    // Recorded even when `bytes_pending` is unknown (0) — the worker
    // discovers actual byte count at delete time. Doing this BEFORE
    // the Data Plane dispatch means we ack even when the worker is
    // backed up or transiently offline, and `_system.l2_cleanup_queue`
    // surfaces the backlog for operators. Idempotent: re-enqueuing
    // the same `(tenant, name)` replaces the prior entry.
    if shared.cold_storage.is_some()
        && let Some(catalog) = shared.credentials.catalog()
    {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let entry = StoredL2CleanupEntry {
            tenant_id,
            name: name.to_string(),
            purge_lsn,
            enqueued_at_ns: now_ns,
            bytes_pending: 0,
            last_error: String::new(),
            attempts: 0,
        };
        if let Err(e) = catalog.enqueue_l2_cleanup(&entry) {
            warn!(
                collection = %name,
                tenant = tenant_id,
                purge_lsn,
                error = %e,
                "failed to enqueue _system.l2_cleanup_queue entry — \
                 L2 bytes will not be reaped until next purge attempt"
            );
        }
    }

    // 3. Quiesce drain: stop accepting new scans for this collection
    //    and wait for in-flight scans to release. Unlinking segment
    //    files while a scan is touching an mmap page faults the
    //    whole TPC reactor — drain ordering is a correctness, not
    //    performance, requirement.
    shared.quiesce.begin_drain(tenant_id, name);
    shared.quiesce.wait_until_drained(tenant_id, name).await;

    // 4. Reclaim on local Data Plane.
    crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
        shared, tenant_id, name, purge_lsn,
    )
    .await;

    // 4b. Broadcast `CollectionPurged` to every connected Lite
    //     session subscribed to this collection. Fire-and-forget;
    //     each session's control-frame channel is bounded to 32 and
    //     any saturated channel drops the notification (the client
    //     picks it up on reconnect via the offline-replay path).
    shared
        .crdt_sync_delivery
        .broadcast_collection_purged(tenant_id, name, purge_lsn);

    // 5. Drop the quiesce entry. From here on, the catalog has no
    //    record of the collection; queries return `collection_not_found`.
    shared.quiesce.forget(tenant_id, name);
    debug!(
        collection = %name,
        tenant = tenant_id,
        purge_lsn,
        "catalog_entry: UnregisterCollection dispatched to local Data Plane"
    );
}
