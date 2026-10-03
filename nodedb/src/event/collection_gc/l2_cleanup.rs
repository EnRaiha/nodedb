// SPDX-License-Identifier: BUSL-1.1

//! L2 (object-store) cleanup worker.
//!
//! Drains `_system.l2_cleanup_queue` — one entry per collection whose
//! hard-delete committed but whose object-store bytes are still owed.
//! Runs once per tick on the Tokio runtime. For each entry the worker
//! lists every object under `{prefix}{tenant_id}/{collection}/` (the
//! scheme used by `ColdStorage::encode_and_upload` for columnar
//! Parquet today — per-engine prefix expansion is a separate slice)
//! and issues deletes. On success the entry is removed and the
//! reclaimed-byte counter advances; on failure
//! `record_l2_cleanup_attempt` bumps `attempts` and stores
//! `last_error` so operators can see via `_system.l2_cleanup_queue`
//! why an entry is stuck.
//!
//! An object a kept base snapshot references is skipped, and its entry
//! stays queued until retention retires that base.
//!
//! Tick cadence defaults to 30s. No configurable backoff: the queue
//! is small, retries are bounded per tick by the attempt count on
//! each row, and persistent failures surface via the metric
//! `nodedb_l2_cleanup_queue_depth{tenant}` (updated each pass).

use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::pitr::ColdPins;
use crate::control::state::SharedState;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Handle for the spawned worker task.
#[derive(Debug)]
pub struct L2CleanupWorker {
    pub handle: JoinHandle<()>,
}

/// Spawn the L2 cleanup worker. Exits cleanly if `shared.cold_storage`
/// is `None` — no object store configured, nothing to drain.
pub fn spawn_l2_cleanup(shared: Arc<SharedState>) -> L2CleanupWorker {
    let handle = tokio::spawn(async move { run_loop(shared).await });
    L2CleanupWorker { handle }
}

async fn run_loop(shared: Arc<SharedState>) {
    if shared.cold_storage.is_none() {
        info!("l2 cleanup worker: cold storage not configured — exiting");
        return;
    }
    info!(
        tick_secs = TICK_INTERVAL.as_secs(),
        "l2 cleanup worker started"
    );

    // Delay one tick so the catalog + cold storage are fully wired
    // before the first pass.
    tokio::time::sleep(TICK_INTERVAL).await;

    loop {
        drain_once(&shared).await;
        tokio::time::sleep(TICK_INTERVAL).await;
    }
}

/// One worker pass. Public for testability.
pub async fn drain_once(shared: &SharedState) {
    let Some(cold) = shared.cold_storage.as_ref() else {
        return;
    };
    let catalog = shared.credentials.catalog();

    let queue = match catalog.load_l2_cleanup_queue() {
        Ok(q) => q,
        Err(e) => {
            warn!(error = %e, "l2 cleanup: failed to load queue");
            return;
        }
    };

    // Refresh the depth gauge every pass as a full snapshot so
    // tenants that just drained to zero stop showing as backed up.
    if let Some(metrics) = shared.system_metrics.as_ref() {
        let mut depths: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        for e in &queue {
            *depths.entry(e.tenant_id).or_insert(0) += 1;
        }
        metrics.purge.set_l2_cleanup_queue_depth(depths);
    }

    if queue.is_empty() {
        return;
    }

    let store = cold.object_store();
    for entry in queue {
        let prefix = format!("{}/{}/{}/", entry.database_id, entry.tenant_id, entry.name);
        match delete_prefix(store.clone(), &prefix, shared.pitr.cold_pins()).await {
            Ok(Reclaimed { bytes, pinned }) if pinned > 0 => {
                if let Some(metrics) = shared.system_metrics.as_ref()
                    && bytes > 0
                {
                    metrics
                        .purge
                        .add_bytes_reclaimed(entry.tenant_id, "unknown", "l2", bytes);
                }
                let msg = format!("{pinned} objects are referenced by a kept base snapshot");
                if let Err(e) = catalog.record_l2_cleanup_attempt(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.name,
                    &msg,
                ) {
                    warn!(error = %e, "l2 cleanup: failed to record attempt");
                }
                debug!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    pinned,
                    "l2 cleanup: pinned objects kept; will retry next tick"
                );
            }
            Ok(Reclaimed {
                bytes: bytes_deleted,
                ..
            }) => {
                if let Err(e) =
                    catalog.remove_l2_cleanup(entry.database_id, entry.tenant_id, &entry.name)
                {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %e,
                        "l2 cleanup: removed L2 bytes but failed to reap queue entry"
                    );
                    continue;
                }
                if let Some(metrics) = shared.system_metrics.as_ref()
                    && bytes_deleted > 0
                {
                    // Engine label is "unknown" here — the per-engine
                    // reclaim handlers will record their own
                    // fine-grained bytes once those land.
                    metrics.purge.add_bytes_reclaimed(
                        entry.tenant_id,
                        "unknown",
                        "l2",
                        bytes_deleted,
                    );
                }
                debug!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    purge_lsn = entry.purge_lsn,
                    bytes_deleted,
                    "l2 cleanup: drained queue entry"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if let Err(update_err) = catalog.record_l2_cleanup_attempt(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.name,
                    &msg,
                ) {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %update_err,
                        "l2 cleanup: failed to record attempt"
                    );
                }
                warn!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    attempts = entry.attempts + 1,
                    error = %msg,
                    "l2 cleanup: delete failed; will retry next tick"
                );
            }
        }
    }
}

/// What one [`delete_prefix`] pass did.
#[derive(Debug, Default, PartialEq, Eq)]
struct Reclaimed {
    bytes: u64,
    /// Objects kept because a kept base snapshot references them.
    pinned: u64,
}

/// Delete every object under `prefix` in the given store, except the
/// objects `pins` holds. Errors on any per-object failure after
/// attempting the rest — i.e. a best-effort pass that surfaces the
/// first failure for the queue-entry's `last_error`.
///
/// The pin read lock is held for the whole pass, so no base pins a key
/// between its check and its delete.
async fn delete_prefix(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    pins: &tokio::sync::RwLock<ColdPins>,
) -> Result<Reclaimed, object_store::Error> {
    let pins = pins.read().await;
    let path = object_store::path::Path::from(prefix);
    let mut list = store.list(Some(&path));
    let mut reclaimed = Reclaimed::default();
    let mut first_err: Option<object_store::Error> = None;
    while let Some(meta) = list.next().await {
        match meta {
            Ok(m) if pins.is_pinned(m.location.as_ref()) => reclaimed.pinned += 1,
            Ok(m) => {
                reclaimed.bytes += m.size;
                if let Err(e) = store.delete(&m.location).await
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(reclaimed),
    }
}

#[cfg(test)]
mod tests {
    //! The queue drain runs against a full `SharedState`. The catalog-level
    //! queue CRUD is tested in `control/security/catalog/l2_cleanup_queue.rs`.

    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    #[tokio::test]
    async fn a_pinned_object_survives_until_its_base_is_retired() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in ["1/2/c/pinned.seg", "1/2/c/free.seg"] {
            store
                .put(&Path::from(key), PutPayload::from_static(b"seg"))
                .await
                .unwrap();
        }
        let pins = tokio::sync::RwLock::new(ColdPins::default());
        pins.write()
            .await
            .pin("snap-1", vec!["1/2/c/pinned.seg".to_string()]);

        let first = delete_prefix(Arc::clone(&store), "1/2/c/", &pins)
            .await
            .unwrap();
        assert_eq!(
            first,
            Reclaimed {
                bytes: 3,
                pinned: 1
            }
        );
        assert!(store.head(&Path::from("1/2/c/pinned.seg")).await.is_ok());
        assert!(store.head(&Path::from("1/2/c/free.seg")).await.is_err());

        pins.write().await.unpin("snap-1");
        let second = delete_prefix(Arc::clone(&store), "1/2/c/", &pins)
            .await
            .unwrap();
        assert_eq!(
            second,
            Reclaimed {
                bytes: 3,
                pinned: 0
            }
        );
        assert!(store.head(&Path::from("1/2/c/pinned.seg")).await.is_err());
    }

    #[tokio::test]
    async fn incomplete_pins_keep_every_object() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from("1/2/c/x.seg"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let pins = tokio::sync::RwLock::new(ColdPins::from_bases([], false));
        let pass = delete_prefix(Arc::clone(&store), "1/2/c/", &pins)
            .await
            .unwrap();
        assert_eq!(pass.pinned, 1);
        assert!(store.head(&Path::from("1/2/c/x.seg")).await.is_ok());
    }
}
