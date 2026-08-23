// SPDX-License-Identifier: BUSL-1.1

//! [`DecommissionSubsystem`] — wraps the [`DecommissionObserver`] lifecycle.
//!
//! Depends on `swim` because the observer polls topology state that is
//! populated by the metadata Raft group after SWIM has established cluster
//! membership.
//!
//! # Shutdown integration
//!
//! The observer emits its own `watch<bool>` when the local node reaches
//! `Decommissioned` state (scoped to `local_node_id` — it never fires for a
//! peer's decommission). `start()` bridges that single signal two ways:
//! into this subsystem's own `shutdown_tx`, which cancels the observer's
//! poll loop, and into `BootstrapCtx::decommission_signal`, the
//! cluster-boundary watch exposed via `RunningCluster::decommission_signal`.
//! `nodedb-cluster` has no process-shutdown primitive of its own — the host
//! process (`nodedb`) subscribes to that watch and drives its own
//! `ShutdownWatch::signal`, so the node exits through the ordinary graceful
//! shutdown path rather than continuing to run after losing its Raft
//! membership.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::watch;
use tracing::warn;

use crate::decommission::observer::DecommissionObserver;

use super::super::context::BootstrapCtx;
use super::super::errors::{BootstrapError, ShutdownError};
use super::super::health::SubsystemHealth;
use super::super::r#trait::{ClusterSubsystem, SubsystemHandle};

/// Owns the decommission observer lifecycle.
pub struct DecommissionSubsystem {
    /// The numeric local node id.
    local_node_id: u64,
    /// How often the observer polls the topology for its own state.
    poll_interval: Duration,
}

impl DecommissionSubsystem {
    pub fn new(local_node_id: u64, poll_interval: Duration) -> Self {
        Self {
            local_node_id,
            poll_interval,
        }
    }
}

#[async_trait]
impl ClusterSubsystem for DecommissionSubsystem {
    fn name(&self) -> &'static str {
        "decommission"
    }

    fn dependencies(&self) -> &'static [&'static str] {
        &["swim"]
    }

    async fn start(&self, ctx: &BootstrapCtx) -> Result<SubsystemHandle, BootstrapError> {
        let (observer, decommission_rx) = DecommissionObserver::new(
            Arc::clone(&ctx.topology),
            self.local_node_id,
            self.poll_interval,
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        bridge_decommission_signal(
            decommission_rx.clone(),
            shutdown_tx.clone(),
            ctx.decommission_signal.clone(),
        );

        let task = tokio::spawn(async move { observer.run(shutdown_rx).await });

        Ok(SubsystemHandle::new("decommission", task, shutdown_tx))
    }

    async fn shutdown(&self, _deadline: Instant) -> Result<(), ShutdownError> {
        // Driven by SubsystemHandle::shutdown_tx — no extra state needed.
        Ok(())
    }

    fn health(&self) -> SubsystemHealth {
        SubsystemHealth::Running
    }
}

/// Bridges the observer's fired signal into `shutdown_tx` (cancels this
/// subsystem's own poll loop) and `cluster_signal` (the cluster-boundary
/// watch the host process bridges into its own graceful shutdown).
///
/// Fires at most once: after the first `true` observation both sends
/// happen and the task exits, since decommission is a one-way transition.
fn bridge_decommission_signal(
    mut decommission_rx: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
    cluster_signal: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        while decommission_rx.changed().await.is_ok() {
            if *decommission_rx.borrow() {
                warn!("decommission observer fired: local node is leaving cluster");
                let _ = shutdown_tx.send(true);
                let _ = cluster_signal.send(true);
                return;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decommission_name_and_deps() {
        let s = DecommissionSubsystem::new(1, Duration::from_secs(5));
        assert_eq!(s.name(), "decommission");
        assert_eq!(s.dependencies(), &["swim"]);
    }

    /// The observer's fired signal must reach the cluster-boundary watch,
    /// not merely this subsystem's own internal poll-loop watch.
    #[tokio::test]
    async fn observer_fired_signal_reaches_cluster_boundary_watch() {
        let (decommission_tx, decommission_rx) = watch::channel(false);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (cluster_tx, mut cluster_rx) = watch::channel(false);

        bridge_decommission_signal(decommission_rx, shutdown_tx, cluster_tx);

        decommission_tx
            .send(true)
            .expect("receiver still held by bridge task");

        shutdown_rx
            .changed()
            .await
            .expect("local shutdown watch must observe the fired signal");
        assert!(*shutdown_rx.borrow());

        cluster_rx
            .changed()
            .await
            .expect("cluster-boundary watch must observe the fired signal");
        assert!(*cluster_rx.borrow());
    }
}
