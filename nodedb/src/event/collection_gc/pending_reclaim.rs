// SPDX-License-Identifier: BUSL-1.1

//! Pending engine-reclaim worker — post-drop storage-purge backlog.
//!
//! Drains `_system.pending_reclaim` — one entry per collection whose
//! catalog row was removed at DROP apply but whose redb + versioned
//! engine purge (`clear_collection_all_engines`, via
//! `MetaOp::UnregisterCollection`) did not succeed on this node. Left
//! outstanding, that failure leaves engine storage rows behind a gone
//! catalog row — permanent divergence that resurrects the dropped
//! collection's history when the name is re-CREATEd. Each pass re-runs
//! the engine purge for every queued entry: on success the entry is
//! removed; on failure `record_pending_reclaim_attempt` bumps `attempts`
//! and stores `last_error` so operators can see via
//! `_system.pending_reclaim` why an entry is stuck.
//!
//! Runs on every node (leader and followers) — each node owns and
//! retries its own local reclaim. Structure mirrors `l2_cleanup.rs`.
//!
//! Tick cadence defaults to 30s. The engine purge is idempotent, so a
//! retry that races a concurrent success is harmless.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::state::SharedState;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Handle for the spawned worker task.
#[derive(Debug)]
pub struct PendingReclaimWorker {
    pub handle: JoinHandle<()>,
}

/// Spawn the pending-reclaim worker.
pub fn spawn_pending_reclaim(shared: Arc<SharedState>) -> PendingReclaimWorker {
    let handle = tokio::spawn(async move { run_loop(shared).await });
    PendingReclaimWorker { handle }
}

async fn run_loop(shared: Arc<SharedState>) {
    info!(
        tick_secs = TICK_INTERVAL.as_secs(),
        "pending-reclaim worker started"
    );
    loop {
        tokio::time::sleep(TICK_INTERVAL).await;
        if let Err(error) = drain_once(&shared).await {
            warn!(error = %error, "pending-reclaim worker pass incomplete");
        }
    }
}

/// One worker pass: re-run the engine purge for every queued entry.
/// Public for the boot-time drain and for testing.
pub async fn drain_once(shared: &SharedState) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let queue = catalog.load_pending_reclaim_queue()?;
    let mut last_error = None;

    for entry in queue {
        if !shared
            .quiesce
            .is_draining(entry.database_id, entry.tenant_id, &entry.name)
        {
            shared
                .quiesce
                .begin_drain(entry.database_id, entry.tenant_id, &entry.name);
        }
        match crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
            shared,
            entry.database_id,
            entry.tenant_id,
            &entry.name,
            entry.purge_lsn,
        )
        .await
        {
            Ok(()) => {
                if let Err(error) =
                    crate::control::catalog_entry::apply::collection::finalize_purge(
                        entry.database_id,
                        entry.tenant_id,
                        &entry.name,
                        catalog,
                    )
                {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %error,
                        "pending-reclaim: engine rows purged but catalog finalization failed"
                    );
                    last_error = Some(error.to_string());
                    continue;
                }
                if let Err(error) = catalog.remove_pending_reclaim(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.name,
                ) {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %error,
                        "pending-reclaim: purged engine rows but failed to reap queue entry"
                    );
                    last_error = Some(error.to_string());
                    continue;
                }
                shared
                    .quiesce
                    .forget(entry.database_id, entry.tenant_id, &entry.name);
                debug!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    purge_lsn = entry.purge_lsn,
                    "pending-reclaim: drained queue entry — engine storage purged"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if let Err(update_err) =
                    catalog.record_pending_reclaim_attempt(
                        entry.database_id,
                        entry.tenant_id,
                        &entry.name,
                        &msg,
                    )
                {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %update_err,
                        "pending-reclaim: failed to record attempt"
                    );
                }
                warn!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    attempts = entry.attempts + 1,
                    error = %msg,
                    "pending-reclaim: engine purge failed; will retry next tick"
                );
                last_error = Some(msg);
            }
        }
    }

    if let Some(detail) = last_error {
        return Err(crate::Error::Storage {
            engine: "pending-reclaim".into(),
            detail,
        });
    }
    Ok(())
}
