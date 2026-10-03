// SPDX-License-Identifier: BUSL-1.1

//! Durable saves of the routing table, off the async threads.
//!
//! A routing save is a catalog write with an fsync. Run on the tick, one
//! stalled fsync would hold up the heartbeats and elections of every group on
//! this node. So every routing save goes through one persister:
//! - A caller changes the in-memory table, then calls
//!   [`RoutingPersister::request`], which returns a sequence number.
//! - The persister runs beside the tick loop. It copies the table as it
//!   stands, saves the copy on a blocking thread, and reports the highest
//!   sequence number the save covers.
//! - A caller that must not go on before the save is durable either polls
//!   [`RoutingPersister::is_durable`] each tick or awaits
//!   [`RoutingPersister::wait`].
//!
//! One writer saves, and it copies the table after it reads the requested
//! sequence number. So a save covers every change made before the request
//! it reports, and an older copy never lands after a newer one.
//!
//! A failed save is reported, and the persister retries it after
//! [`RETRY_DELAY`].

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tracing::warn;

use crate::catalog::ClusterCatalog;
use crate::routing::RoutingTable;

/// The wait before a failed save is tried again.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// The highest sequence numbers a save covered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SaveOutcome {
    /// Every request at or below it is durable.
    durable: u64,
    /// The last failed save covered the requests at or below it.
    failed: u64,
    /// The persister ended. A request not yet durable is never saved.
    stopped: bool,
}

/// The single writer of the routing table to the catalog.
pub(in crate::raft_loop) struct RoutingPersister {
    catalog: Arc<ClusterCatalog>,
    routing: Arc<RwLock<RoutingTable>>,
    requested: AtomicU64,
    wake: Notify,
    outcome: watch::Sender<SaveOutcome>,
}

impl RoutingPersister {
    pub(in crate::raft_loop) fn new(
        catalog: Arc<ClusterCatalog>,
        routing: Arc<RwLock<RoutingTable>>,
    ) -> Self {
        Self {
            catalog,
            routing,
            requested: AtomicU64::new(0),
            wake: Notify::new(),
            outcome: watch::Sender::new(SaveOutcome::default()),
        }
    }

    /// Ask for a save of the table as it stands now. Returns the sequence
    /// number the save reports. Never waits.
    pub(in crate::raft_loop) fn request(&self) -> u64 {
        let seq = self.requested.fetch_add(1, Ordering::AcqRel) + 1;
        self.wake.notify_one();
        seq
    }

    /// Whether the save of request `seq` is durable.
    pub(in crate::raft_loop) fn is_durable(&self, seq: u64) -> bool {
        self.outcome.borrow().durable >= seq
    }

    /// Wait until the save of request `seq` is durable, a save that covers it
    /// failed, or the persister ended. Returns whether it is durable.
    pub(in crate::raft_loop) async fn wait(&self, seq: u64) -> bool {
        let mut rx = self.outcome.subscribe();
        loop {
            let outcome = *rx.borrow_and_update();
            if outcome.durable >= seq {
                return true;
            }
            if outcome.failed >= seq || outcome.stopped {
                return false;
            }
            if rx.changed().await.is_err() {
                return false;
            }
        }
    }

    /// Save the table whenever a request is not yet durable, until shutdown
    /// begins. A request made before shutdown gets one last save attempt.
    pub(in crate::raft_loop) async fn run(&self, shutdown: watch::Receiver<bool>) {
        self.save_until_shutdown(shutdown).await;
        self.save_pending().await;
        self.outcome.send_modify(|o| o.stopped = true);
    }

    async fn save_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if self.save_pending().await == Some(false)
                && wait_or_shutdown(&mut shutdown, RETRY_DELAY).await
            {
                return;
            }
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                _ = self.wake.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// Save the table when a request is not yet durable. `None` when nothing
    /// waited, else whether the save succeeded.
    async fn save_pending(&self) -> Option<bool> {
        let target = self.requested.load(Ordering::Acquire);
        if target <= self.outcome.borrow().durable {
            return None;
        }
        // Copied after `target` was read: the copy holds every change made
        // before each request up to `target`.
        let table = self
            .routing
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let catalog = Arc::clone(&self.catalog);
        let saved = tokio::task::spawn_blocking(move || catalog.save_routing(&table)).await;
        let error = match saved {
            Ok(Ok(())) => {
                self.outcome
                    .send_modify(|o| o.durable = o.durable.max(target));
                return Some(true);
            }
            Ok(Err(e)) => e.to_string(),
            Err(e) => format!("save task: {e}"),
        };
        warn!(
            requested = target,
            error = %error,
            "could not save the routing table; the save is retried"
        );
        self.outcome
            .send_modify(|o| o.failed = o.failed.max(target));
        Some(false)
    }
}

/// Wait `wait`, or less when shutdown begins. Returns whether it began.
async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, wait: Duration) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(wait) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn persister(dir: &tempfile::TempDir) -> Arc<RoutingPersister> {
        let catalog = Arc::new(
            ClusterCatalog::open(&dir.path().join("cluster.redb")).expect("open the catalog"),
        );
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(2, &[1, 2, 3], 3)));
        Arc::new(RoutingPersister::new(catalog, routing))
    }

    /// A requested save becomes durable, and the saved table holds the
    /// change made before the request.
    #[tokio::test]
    async fn a_request_is_saved_off_the_caller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let persister = persister(&dir);
        let (stop_tx, stop_rx) = watch::channel(false);
        let runner = {
            let persister = Arc::clone(&persister);
            tokio::spawn(async move { persister.run(stop_rx).await })
        };

        persister
            .routing
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .set_group_members(1, vec![2]);
        let seq = persister.request();
        assert!(persister.wait(seq).await);
        assert!(persister.is_durable(seq));
        let saved = persister
            .catalog
            .load_routing()
            .expect("load the routing table")
            .expect("a saved routing table");
        assert_eq!(saved.group_info(1).expect("group 1").members, vec![2]);

        stop_tx.send(true).expect("signal shutdown");
        runner.await.expect("the persister ends");
    }
}
