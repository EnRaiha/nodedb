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

/// Grace period before a second signal is treated as a force-stop request.
///
/// Force-stop exists for an operator who presses Ctrl+C again because shutdown
/// is taking too long. A process supervisor that delivers SIGTERM twice in
/// quick succession, or a duplicate from a process group, is not that — and
/// must not turn a clean shutdown into `exit(1)`. Graceful shutdown normally
/// completes in milliseconds, so anything inside this window is a duplicate;
/// a human's second keypress necessarily falls outside it.
const FORCE_STOP_ARMING_DELAY: Duration = Duration::from_secs(2);

/// Wait for the canonical shutdown bus to reach its terminal phase.
///
/// The first signal must never impose a second, independent deadline on
/// shutdown: critical listeners are allowed to hold their phase until their
/// cleanup is complete. A second signal is handled separately as the sole
/// force-stop path.
async fn await_canonical_shutdown(
    shutdown_handle: &mut crate::control::shutdown::ShutdownHandle,
    sequencer_handle: tokio::task::JoinHandle<()>,
) {
    shutdown_handle
        .await_phase(crate::control::shutdown::ShutdownPhase::Closed)
        .await;
    if let Err(join_err) = sequencer_handle.await {
        tracing::error!(error = %join_err, "shutdown sequencer task panicked");
    }
}

/// Spawn the graceful shutdown handler and the force-stop handler.
///
/// The graceful handler waits for the first Ctrl+C or SIGTERM, immediately
/// initiates the phased shutdown bus, and awaits its terminal phase.
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

        // Begin the canonical phased shutdown immediately. In particular this
        // stops listeners before auxiliary release work starts.
        let mut shutdown_handle = bus_for_signal.handle();
        let sequencer_handle = bus_for_signal.initiate();

        // A second signal is now the only force-stop path.
        let _ = force_stop_tx.send(());

        let shapes = shared_signal.shape_registry.export_all();
        if !shapes.is_empty() {
            tracing::info!(shapes = shapes.len(), "persisting shape subscriptions");
        }

        crate::control::lease::shutdown_release::release_all_local_leases(
            Arc::clone(&shared_signal),
            crate::control::lease::shutdown_release::DEFAULT_SHUTDOWN_RELEASE_TIMEOUT,
        )
        .await;

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

        await_canonical_shutdown(&mut shutdown_handle, sequencer_handle).await;
    });

    tokio::spawn(async move {
        let _ = force_stop_rx.await;

        // Signals delivered inside the arming window are duplicates of the one
        // that started this shutdown. The listeners are installed after the
        // window so those never register as a force-stop request; if graceful
        // shutdown finishes first the process exits normally and this task
        // simply dies with it.
        tokio::time::sleep(FORCE_STOP_ARMING_DELAY).await;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::control::shutdown::{ShutdownPhase, ShutdownWatch};

    #[tokio::test]
    async fn canonical_shutdown_waits_for_critical_listener_drain() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, _) = ShutdownBus::new(watch);
        let guard = bus.register_critical_task(ShutdownPhase::DrainingListeners, "signal-test");
        let mut handle = bus.handle();
        let sequencer = bus.initiate();

        let mut completion = tokio::spawn(async move {
            await_canonical_shutdown(&mut handle, sequencer).await;
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut completion)
                .await
                .is_err()
        );
        guard.report_drained();
        tokio::time::timeout(Duration::from_secs(1), completion)
            .await
            .expect("first-signal shutdown must finish after the critical drain")
            .expect("shutdown waiter must not panic");
    }
}
