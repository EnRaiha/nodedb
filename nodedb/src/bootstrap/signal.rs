// SPDX-License-Identifier: BUSL-1.1

//! Signal handling: graceful shutdown on Ctrl+C / SIGTERM, force-stop on second signal.

use std::sync::Arc;
use std::time::Duration;

use crate::control::cluster::ClusterHandle;
use crate::control::shutdown::ShutdownBus;
use crate::control::state::SharedState;

/// Deadline given to each cluster subsystem (SWIM, reachability,
/// decommission, rebalancer) to stop cleanly during graceful shutdown.
const CLUSTER_SUBSYSTEM_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

/// Spawn the graceful shutdown handler and the force-stop handler.
///
/// The graceful handler waits for the first Ctrl+C or SIGTERM, drains connections,
/// initiates the phased shutdown bus, and awaits the sequencer.
///
/// The force-stop handler waits for the graceful handler to be armed, then listens
/// for a second signal and calls `process::exit(1)`.
pub fn spawn_signal_handlers(
    shared: Arc<SharedState>,
    conn_semaphore: Arc<tokio::sync::Semaphore>,
    max_connections: usize,
    shutdown_bus: ShutdownBus,
    cluster_handle: Option<Arc<ClusterHandle>>,
) {
    let (force_stop_tx, force_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let sem_clone = Arc::clone(&conn_semaphore);
    let shared_signal = Arc::clone(&shared);
    let bus_for_signal = shutdown_bus.clone();

    tokio::spawn(async move {
        // Wait for first Ctrl+C or SIGTERM.
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        let active = max_connections - sem_clone.available_permits();
        if active > 0 {
            eprintln!();
            eprintln!(
                "  {} active connection(s). Draining (30s timeout)...",
                active
            );
            eprintln!("  Press Ctrl+C again to force stop.");
        } else {
            eprintln!("\n  Shutting down...");
        }

        let shapes = shared_signal.shape_registry.export_all();
        if !shapes.is_empty() {
            tracing::info!(shapes = shapes.len(), "persisting shape subscriptions");
        }

        crate::control::lease::shutdown_release::release_all_local_leases(
            Arc::clone(&shared_signal),
            crate::control::lease::shutdown_release::DEFAULT_SHUTDOWN_RELEASE_TIMEOUT,
        )
        .await;

        let sequencer_handle = bus_for_signal.initiate();

        // Arm the force-stop handler.
        let _ = force_stop_tx.send(());

        let report = shared_signal
            .loop_registry
            .shutdown_all(shared_signal.tuning.shutdown.deadline())
            .await;
        if report.is_clean() {
            tracing::info!(
                clean = report.exited_clean.len(),
                total = ?report.total,
                "all background loops exited cleanly"
            );
        } else {
            tracing::error!(
                clean = report.exited_clean.len(),
                laggards = ?report.laggards,
                total = ?report.total,
                "background loops exceeded shutdown deadline"
            );
        }

        // Stop cluster subsystem tasks (SWIM, reachability, decommission,
        // rebalancer) so they release their clone of `Arc<SharedState>`
        // (transitively, via the shared `MultiRaft` handle) before the
        // process exits. `RunningCluster::shutdown_all` consumes the
        // value, so it must be taken out of the handle's slot exactly
        // once; a `None` here means either single-node mode or that
        // shutdown already ran.
        if let Some(handle) = &cluster_handle {
            let running = handle
                .running_cluster
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            if let Some(running) = running {
                let errors = running
                    .shutdown_all(CLUSTER_SUBSYSTEM_SHUTDOWN_DEADLINE)
                    .await;
                if errors.is_empty() {
                    tracing::info!("cluster subsystems stopped cleanly");
                } else {
                    tracing::error!(?errors, "cluster subsystem shutdown errors");
                }
            }
        }

        match tokio::time::timeout(std::time::Duration::from_secs(2), sequencer_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) => {
                tracing::error!(error = %join_err, "shutdown sequencer task panicked");
            }
            Err(_) => {
                tracing::error!("shutdown sequencer exceeded 2s cap — forcing exit");
            }
        }

        std::process::exit(0);
    });

    tokio::spawn(async move {
        let _ = force_stop_rx.await;

        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install second SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        eprintln!("  Force stop.");
        std::process::exit(1);
    });
}
