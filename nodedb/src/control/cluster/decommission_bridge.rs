// SPDX-License-Identifier: BUSL-1.1

//! Bridges `nodedb-cluster`'s per-node decommission signal into the
//! process-wide graceful shutdown path.
//!
//! `nodedb-cluster` has no process-shutdown primitive of its own —
//! `ShutdownWatch` lives in `nodedb`, and `nodedb-cluster` cannot depend on
//! this crate. When this node finishes decommissioning,
//! `RunningCluster::decommission_signal()` fires exactly once for this
//! node's own decommission (never for a peer's); this task drives that into
//! `ShutdownWatch::signal()` so the node exits through the same graceful
//! shutdown path SIGINT/SIGTERM already use.

use std::sync::Arc;

use tokio::sync::watch;

use crate::control::shutdown::{LoopRegistry, ShutdownWatch, spawn_loop};

/// Spawn the task that drives `decommission_rx` into `shutdown`.
///
/// Registered in `registry` like any other background loop, so a normal
/// SIGINT/SIGTERM cancels this task via the same `ShutdownWatch` it would
/// otherwise signal. `ShutdownWatch::signal` is idempotent, so a race
/// between the decommission path and an operator-initiated shutdown is
/// harmless — whichever reaches `signal()` first wins, and the other is a
/// no-op.
pub fn spawn_decommission_shutdown_bridge(
    registry: &LoopRegistry,
    shutdown: &Arc<ShutdownWatch>,
    mut decommission_rx: watch::Receiver<bool>,
) {
    let signal_shutdown = Arc::clone(shutdown);
    spawn_loop(
        registry,
        shutdown,
        "decommission_shutdown_bridge",
        move |mut shutdown_rx| async move {
            tokio::select! {
                _ = shutdown_rx.wait_cancelled() => {}
                result = decommission_rx.changed() => {
                    if result.is_ok() && *decommission_rx.borrow() {
                        signal_shutdown.signal();
                    }
                }
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Observing the decommission watch must result in the process
    /// `ShutdownWatch` being signalled — the bridge this module exists for.
    #[tokio::test]
    async fn decommission_signal_drives_process_shutdown_watch() {
        let registry = LoopRegistry::new();
        let shutdown = Arc::new(ShutdownWatch::new());
        let (decommission_tx, decommission_rx) = watch::channel(false);

        spawn_decommission_shutdown_bridge(&registry, &shutdown, decommission_rx);
        assert!(!shutdown.is_shutdown());

        decommission_tx
            .send(true)
            .expect("receiver still held by the bridge task");

        let mut shutdown_signal = shutdown.subscribe();
        tokio::time::timeout(Duration::from_secs(1), shutdown_signal.wait_cancelled())
            .await
            .expect("process shutdown watch must observe the decommission signal");
        assert!(shutdown.is_shutdown());
    }

    /// A normal shutdown (no decommission) must not hang the bridge task —
    /// it observes `ShutdownWatch` cancellation and exits on its own.
    #[tokio::test]
    async fn ordinary_shutdown_cancels_bridge_without_decommission() {
        let registry = LoopRegistry::new();
        let shutdown = Arc::new(ShutdownWatch::new());
        let (_decommission_tx, decommission_rx) = watch::channel(false);

        spawn_decommission_shutdown_bridge(&registry, &shutdown, decommission_rx);

        shutdown.signal();
        let report = registry.shutdown_all(Duration::from_millis(200)).await;
        assert!(report.is_clean(), "{report}");
    }
}
