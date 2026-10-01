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
use tracing::{debug, error, info, warn};

use crate::control::state::SharedState;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Longest one retry waits for open scans of the reclaimed collection. A
/// scan that outlives it leaves the row for the next pass, so it never
/// stalls the other rows.
const SCAN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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

/// What one retry of a queued entry did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The purge ran, and the row and its hold are gone.
    Reclaimed,
    /// A later incarnation holds the name, so the row was dropped unrun.
    Superseded,
    /// Scans of the collection stayed open past the drain wait. The row
    /// stays, with the attempt recorded, for the next pass.
    ScansOpen { open_scans: usize },
}

/// One worker pass: re-run the engine purge for every queued entry.
///
/// Rows whose scans stay open wait for the next pass. `Err` reports the last
/// failed purge after every row was tried.
pub async fn drain_once(shared: &SharedState) -> crate::Result<()> {
    let queue = shared.credentials.catalog().load_pending_reclaim_queue()?;
    drain_pass(shared, queue, false).await
}

/// The boot-time pass, before the gateway opens. Only an unreadable queue
/// fails the boot.
///
/// A row that does not reclaim keeps its row, its recorded attempt, and the
/// drain hold its retry took. The hold keeps a same-name CREATE waiting until
/// the worker's retry succeeds, so no replacement opens over storage that
/// retry erases. No client can scan yet, so a scan still open here is a
/// leak and is logged at error with the collection's name.
pub async fn drain_at_boot(shared: &SharedState) -> crate::Result<()> {
    let queue = shared.credentials.catalog().load_pending_reclaim_queue()?;
    if let Err(error) = drain_pass(shared, queue, true).await {
        error!(
            error = %error,
            "pending-reclaim at boot: a purge failed; its row and drain hold stay for the worker"
        );
    }
    Ok(())
}

/// Retry every row of `queue`. `Err` reports the last failed purge after
/// every row was tried.
async fn drain_pass(
    shared: &SharedState,
    queue: Vec<crate::control::security::catalog::StoredPendingReclaim>,
    at_boot: bool,
) -> crate::Result<()> {
    let mut last_error = None;
    for entry in queue {
        match retry_one(shared, &entry).await {
            Ok(RetryOutcome::ScansOpen { open_scans }) if at_boot => error!(
                tenant = entry.tenant_id,
                collection = %entry.name,
                open_scans,
                "pending-reclaim at boot: scans are open before the gateway opened, so a scan \
                 guard leaked; the row stays for the worker"
            ),
            Ok(RetryOutcome::ScansOpen { open_scans }) => warn!(
                tenant = entry.tenant_id,
                collection = %entry.name,
                open_scans,
                "pending-reclaim: open scans outlived the drain wait; retrying next tick"
            ),
            Ok(RetryOutcome::Reclaimed | RetryOutcome::Superseded) => {}
            Err(error) => last_error = Some(error.to_string()),
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

/// Re-run the engine purge for one queued entry.
///
/// Keeps the pending-reclaim path's drain hold on the name, so a same-name
/// CREATE waits for the purge. On success the entry is removed and that hold
/// released. On failure both stay, and the attempt is recorded.
///
/// An entry whose name now holds a later incarnation is dropped without any
/// reclaim. Data Plane storage is keyed by `(database, tenant, name)`, with
/// no incarnation component, so the later incarnation owns it now.
pub async fn retry_one(
    shared: &SharedState,
    entry: &crate::control::security::catalog::StoredPendingReclaim,
) -> crate::Result<RetryOutcome> {
    let catalog = shared.credentials.catalog();
    // The pending-reclaim path owns one hold per name for as long as the row
    // lives. After a restart it has none, so it takes one here.
    shared.quiesce.ensure_reclaim_hold(&entry.owner());
    let live = catalog.get_committed_collection(
        crate::types::DatabaseId::new(entry.database_id),
        entry.tenant_id,
        &entry.name,
    )?;
    if !entry.owns(live.as_ref()) {
        catalog.remove_pending_reclaim(entry.database_id, entry.tenant_id, &entry.name)?;
        shared.quiesce.release_reclaim_hold(&entry.owner());
        info!(
            tenant = entry.tenant_id,
            collection = %entry.name,
            "pending-reclaim: a later incarnation holds the name; retry dropped"
        );
        return Ok(RetryOutcome::Superseded);
    }
    // Scans of the reclaimed incarnation must release before its segments go.
    let drained = tokio::time::timeout(
        SCAN_DRAIN_TIMEOUT,
        shared
            .quiesce
            .wait_until_drained(entry.database_id, entry.tenant_id, &entry.name),
    )
    .await;
    if drained.is_err() {
        let open = shared
            .quiesce
            .open_scans(entry.database_id, entry.tenant_id, &entry.name);
        let detail = format!(
            "{open} scan(s) of '{}' stayed open for {SCAN_DRAIN_TIMEOUT:?}; the purge waits \
             for the next pass",
            entry.name
        );
        if let Err(update_err) = catalog.record_pending_reclaim_attempt(
            entry.database_id,
            entry.tenant_id,
            &entry.name,
            &detail,
        ) {
            warn!(
                tenant = entry.tenant_id,
                collection = %entry.name,
                error = %update_err,
                "pending-reclaim: failed to record attempt"
            );
        }
        return Ok(RetryOutcome::ScansOpen { open_scans: open });
    }
    if let Err(e) =
        crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
            shared,
            entry.database_id,
            entry.tenant_id,
            &entry.name,
            entry.purge_lsn,
        )
        .await
    {
        let msg = e.to_string();
        if let Err(update_err) = catalog.record_pending_reclaim_attempt(
            entry.database_id,
            entry.tenant_id,
            &entry.name,
            &msg,
        ) {
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
        return Err(e);
    }
    if let Err(error) = crate::control::catalog_entry::apply::collection::finalize_purge(
        entry.database_id,
        entry.tenant_id,
        &entry.name,
        catalog,
    ) {
        warn!(
            tenant = entry.tenant_id,
            collection = %entry.name,
            error = %error,
            "pending-reclaim: engine rows purged but catalog finalization failed"
        );
        return Err(error);
    }
    if let Err(error) =
        catalog.remove_pending_reclaim(entry.database_id, entry.tenant_id, &entry.name)
    {
        warn!(
            tenant = entry.tenant_id,
            collection = %entry.name,
            error = %error,
            "pending-reclaim: purged engine rows but failed to reap queue entry"
        );
        return Err(error);
    }
    shared.quiesce.release_reclaim_hold(&entry.owner());
    debug!(
        tenant = entry.tenant_id,
        collection = %entry.name,
        purge_lsn = entry.purge_lsn,
        "pending-reclaim: drained queue entry — engine storage purged"
    );
    Ok(RetryOutcome::Reclaimed)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Hlc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::catalog::{StoredCollection, StoredPendingReclaim};
    use crate::types::DatabaseId;
    use crate::wal::WalManager;

    /// A reclaim queued against incarnation 1 finds incarnation 2 under the
    /// name. It drops the retry and sends nothing to the Data Plane, whose
    /// storage for the name now belongs to incarnation 2.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reclaim_for_incarnation_one_never_touches_incarnation_two() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("reclaim.wal")).expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let mut side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let catalog = state.credentials.catalog();

        let mut second = StoredCollection::stamped_for_test(1, "events", "tester");
        second.modification_hlc = Hlc::new(20, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &second)
            .expect("seed incarnation 2");
        let entry = StoredPendingReclaim {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "events".to_string(),
            purge_lsn: 500,
            enqueued_at_ns: 0,
            last_error: String::new(),
            attempts: 0,
            target_hlc: Some(Hlc::new(10, 0)),
            cancelled_create: false,
        };
        catalog.enqueue_pending_reclaim(&entry).expect("queue");

        drain_once(&state)
            .await
            .expect("drain drops the stale retry");

        assert!(
            catalog.load_pending_reclaim_queue().unwrap().is_empty(),
            "the stale retry is dropped"
        );
        let live = catalog
            .get_committed_collection(DatabaseId::DEFAULT, 1, "events")
            .unwrap()
            .expect("incarnation 2 keeps its row");
        assert_eq!(live.modification_hlc, Hlc::new(20, 0));
        assert!(
            side.request_rx.try_pop().is_err(),
            "no reclaim reaches the Data Plane"
        );
        assert!(
            !state
                .quiesce
                .is_draining(DatabaseId::DEFAULT.as_u64(), 1, "events"),
            "the retry releases its drain hold"
        );
    }

    fn queued(name: &str, target_hlc: Option<Hlc>) -> StoredPendingReclaim {
        StoredPendingReclaim {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: name.to_string(),
            purge_lsn: 500,
            enqueued_at_ns: 0,
            last_error: String::new(),
            attempts: 0,
            target_hlc,
            cancelled_create: false,
        }
    }

    /// A scan that never closes bounds its row's retry, and the pass still
    /// reaches the next row.
    #[tokio::test(start_paused = true)]
    async fn a_hung_scan_never_stalls_other_rows() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("hung.wal")).expect("test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let mut side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let catalog = state.credentials.catalog();

        // "hung" has an open scan that never closes.
        let _scan = state
            .quiesce
            .try_start_scan(DatabaseId::DEFAULT.as_u64(), 1, "hung")
            .expect("scan opens");
        catalog
            .enqueue_pending_reclaim(&queued("hung", None))
            .expect("queue hung");
        // "stale" is owned by a later incarnation, so its retry is dropped.
        let mut later = StoredCollection::stamped_for_test(1, "stale", "tester");
        later.modification_hlc = Hlc::new(20, 0);
        catalog
            .put_collection(DatabaseId::DEFAULT, &later)
            .expect("seed the later incarnation");
        catalog
            .enqueue_pending_reclaim(&queued("stale", Some(Hlc::new(10, 0))))
            .expect("queue stale");

        drain_once(&state)
            .await
            .expect("a row with open scans waits for the next pass without failing it");

        let rows = catalog.load_pending_reclaim_queue().unwrap();
        assert_eq!(rows.len(), 1, "only the hung row stays: {rows:?}");
        assert_eq!(rows[0].name, "hung");
        assert_eq!(rows[0].attempts, 1);
        assert!(
            side.request_rx.try_pop().is_err(),
            "no purge reached the Data Plane while the scan stayed open"
        );
    }

    /// At boot, a row whose scans stay open keeps its row with the attempt
    /// recorded, and the boot pass still succeeds.
    #[tokio::test(start_paused = true)]
    async fn an_open_scan_at_boot_does_not_fail_the_boot() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("boot.wal")).expect("test WAL"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let catalog = state.credentials.catalog();
        let _leaked = state
            .quiesce
            .try_start_scan(DatabaseId::DEFAULT.as_u64(), 1, "leaked")
            .expect("scan opens");
        catalog
            .enqueue_pending_reclaim(&queued("leaked", None))
            .expect("queue leaked");

        drain_at_boot(&state)
            .await
            .expect("an open scan at boot does not fail the boot");

        let rows = catalog.load_pending_reclaim_queue().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attempts, 1);
    }
}
