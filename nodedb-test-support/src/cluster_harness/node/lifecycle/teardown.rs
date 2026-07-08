// SPDX-License-Identifier: BUSL-1.1

//! Query execution, cooperative shutdown, and panic-safe `Drop` teardown
//! for [`TestClusterNode`].

use super::types::TestClusterNode;

impl TestClusterNode {
    /// Execute a simple query; returns an error message on SQL error.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self.client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Cooperatively shut down every background task this node owns.
    pub async fn shutdown(self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        for tx in &self.core_stop_txs {
            let _ = tx.send(());
        }
        // Give tokio a chance to drop the task futures before TempDir
        // is dropped — otherwise redb file locks can linger.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

/// Panic-safe teardown. Without this, a test that panics (e.g. a
/// `wait_for` tripping its budget) would drop `TestClusterNode`
/// without ever calling the async `shutdown()`, leaving every
/// background task still running:
///
/// - `watch::Sender`s close on drop but DO NOT transmit their last
///   value, so the raft / pgwire / poller loops block on
///   `select { shutdown.changed() }` forever.
/// - `JoinHandle`s on drop DETACH the task instead of cancelling it.
/// - Those detached tasks keep the tempdir's redb files open, so
///   `TempDir::drop` either hangs or the whole test process sticks
///   around until nextest kills it at `slow-timeout` (previously
///   ~2 minutes of wasted CI time per flaky cluster test).
///
/// The Drop here fires the watch senders synchronously and aborts
/// every JoinHandle we own. `abort()` is non-blocking: the next time
/// the task hits an `.await` it gets cancelled and releases its
/// resources, including the redb handles. Combined with the
/// already-present `core_stop_tx` drop (which disconnects the
/// blocking Data Plane loop), this guarantees the node tears down
/// in milliseconds instead of minutes.
impl Drop for TestClusterNode {
    fn drop(&mut self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        // `core_stop_tx` is a std mpsc Sender; dropping it disconnects
        // the receiver the spawn_blocking data-plane loop polls, so
        // no explicit signal needed here.
        self._conn_handle.abort();
        self._pg_handle.abort();
        self._native_handle.abort();
        self._poller_handle.abort();
        for h in &self._core_handles {
            h.abort();
        }
    }
}

pub(in crate::cluster_harness::node) fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}
