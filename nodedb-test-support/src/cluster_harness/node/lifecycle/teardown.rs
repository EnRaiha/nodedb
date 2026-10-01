// SPDX-License-Identifier: BUSL-1.1

//! Query execution, cooperative shutdown, and panic-safe `Drop` teardown
//! for [`TestClusterNode`].

use std::time::Duration;

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

    /// Shut down and AWAIT every background task before returning, flushing
    /// the WAL first — unlike [`Self::shutdown`], which fires shutdown
    /// signals and yields a bounded number of times as a best-effort drain.
    ///
    /// Required before reopening this node's data directory on the same path
    /// (a WAL-only restart, e.g. via `spawn_single_node_calvin_on_path`): the
    /// next `TestClusterNode` needs every redb file handle from this instance
    /// actually released, and any buffered WAL writes durable, or it will
    /// either fail to open the store (still locked) or find nothing to
    /// replay. Triggers no checkpoint/snapshot — callers that need a
    /// WAL-only restart (proving pure WAL replay rebuilds in-memory-only
    /// structures such as the vector HNSW index) rely on that.
    pub async fn graceful_shutdown_wal_only(mut self) {
        // Drop the client FIRST so the underlying socket closes and the
        // server-side pgwire session task can drain and drop its
        // `Arc<SharedState>` clone before we touch redb at all — mirrors
        // `pgwire_harness::TestServer::graceful_shutdown`.
        let _ = self.client.take();

        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        for tx in &self.core_stop_txs {
            let _ = tx.send(());
        }

        // Persist any buffered WAL writes before the next node reopens this
        // directory.
        let _ = self.shared.wal.sync();

        // Abort and await the lease-renewal loop, so its `Arc<SharedState>`
        // clone drops before this function returns. A dropped `JoinHandle`
        // detaches its task and never cancels it, so the node keeps this one.
        if let Some(h) = self._lease_renewal_handle.take() {
            h.abort();
            let _ = h.await;
        }

        // Await the Data-Plane core OS threads so every engine's redb handle
        // (KV, document, vector HNSW snapshot files, etc.) is actually
        // released before returning.
        for h in std::mem::take(&mut self._core_handles) {
            h.abort();
            let _ = h.await;
        }

        if let Some(h) = self._poller_handle.take() {
            h.abort();
            let _ = h.await;
        }

        // Let the listeners drain via the shutdown bus within a bounded
        // window, falling back to abort so this cannot hang indefinitely.
        if let Some(mut h) = self._pg_handle.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut h).await {
                Ok(_) => {}
                Err(_) => {
                    h.abort();
                    let _ = h.await;
                }
            }
        }
        if let Some(mut h) = self._native_handle.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut h).await {
                Ok(_) => {}
                Err(_) => {
                    h.abort();
                    let _ = h.await;
                }
            }
        }
        if let Some(h) = self._conn_handle.take() {
            h.abort();
            let _ = h.await;
        }

        // Event consumers hold Arc<SharedState> + WatermarkStore (redb);
        // join them so those handles drop before we return.
        if let Some(ep) = self._event_plane.take() {
            ep.shutdown_and_join().await;
        }

        // Await every loop registered with LoopRegistry so their
        // Arc<SharedState> clones release before we return.
        self.shared
            .loop_registry
            .shutdown_all(Duration::from_secs(5))
            .await;

        // Stop and join the cluster subsystem tasks (SWIM, reachability,
        // decommission, rebalancer) started by `start_raft`. They share
        // the raft loop's `Arc<Mutex<MultiRaft>>`, which transitively
        // pins `Arc<SharedState>` — without this, `shutdown_all` never
        // runs and the subsystem tasks (and their clone) leak forever,
        // exactly the leak `RunningCluster`'s own doc warns about.
        if let Some(running) = self._running_cluster.take() {
            let errors = running.shutdown_all(Duration::from_secs(5)).await;
            if !errors.is_empty() {
                eprintln!(
                    "graceful_shutdown_wal_only: cluster subsystem shutdown errors: {errors:?}"
                );
            }
        }

        // Close the QUIC endpoint. Its driver owns the UDP socket and runs
        // until every connection is gone, and a live peer keeps its
        // connection open until the idle timeout. Without the close, a
        // restart on the same address finds the port still bound.
        if let Some(transport) = self.shared.cluster_transport.clone()
            && !transport.close(Duration::from_secs(2)).await
        {
            eprintln!(
                "graceful_shutdown_wal_only: node {} peers did not acknowledge the transport \
                 close within 2s",
                self.node_id
            );
        }

        // `start_raft` spawns tasks the harness holds no `JoinHandle` for:
        // the raft apply and tick loops, the sequencer service, the RPC
        // server, the health monitor, and the per-vShard Calvin schedulers.
        // Each holds an `Arc<SharedState>` clone and exits after the shutdown
        // signal sent above. `SharedState` owns the catalog redb and the QUIC
        // endpoint, so both stay open until its last clone drops. Wait for
        // `self.shared` to be the only clone, then drop it with `self`.
        //
        // A clone that survives the wait is a leak: a task that ignores
        // shutdown, or a reference cycle through `SharedState`. The node
        // fails here, at the leak, never later as a locked catalog or a
        // bound port on the next open.
        let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while std::sync::Arc::strong_count(&self.shared) > 1 {
            if tokio::time::Instant::now() >= poll_deadline {
                panic!(
                    "graceful_shutdown_wal_only: node {} SharedState still has {} strong refs \
                     5s after shutdown, where 1 is expected: a task or a reference cycle still \
                     holds SharedState, which keeps its redb files and QUIC endpoint open",
                    self.node_id,
                    std::sync::Arc::strong_count(&self.shared)
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // `self` (and `_data_dir`) drops here. `DataDir::Owned` deletes the
        // tempdir; `DataDir::Borrowed` is a no-op, leaving the caller's
        // directory intact for the next `..._on_path` spawn.
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
        // `Option::as_ref` — already-taken handles (e.g. after
        // `graceful_shutdown_wal_only`) are `None` and skipped.
        if let Some(h) = self._conn_handle.as_ref() {
            h.abort();
        }
        if let Some(h) = self._pg_handle.as_ref() {
            h.abort();
        }
        if let Some(h) = self._native_handle.as_ref() {
            h.abort();
        }
        if let Some(h) = self._poller_handle.as_ref() {
            h.abort();
        }
        if let Some(h) = self._lease_renewal_handle.as_ref() {
            h.abort();
        }
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
