// SPDX-License-Identifier: BUSL-1.1

//! Unified shutdown-bus wiring, plus the test-only slow-drain-task
//! injection used by integration tests to exercise the offender-abort
//! path.

use std::sync::Arc;

use nodedb::control::shutdown::{LoopRegistry, ShutdownBus, ShutdownWatch};
use nodedb::control::state::SharedState;

/// Build the phased shutdown bus, wire it to `SharedState`'s
/// `ShutdownWatch` and system metrics, and — only when
/// `NODEDB_TEST_SLOW_DRAIN_TASK=1` — register a task that deliberately
/// never reports drained, to verify the offender-abort path in
/// integration tests. Pure relocation of what used to be inline in
/// `main()`.
///
/// Returns `(shutdown_rx, shutdown_bus, loop_registry_supervisor)`. All
/// shutdown signals flow through the canonical `ShutdownWatch` held on
/// `SharedState`; the returned raw receiver is a view of that same watch,
/// preserved so the existing listener APIs (`PgListener::run`,
/// `HttpServer::run`, `IlpListener::run`, `RespListener::run`,
/// `spawn_cold_storage_loop`, `spawn_checkpoint_loop`, and the lease renewal
/// loop) keep their `watch::Receiver<bool>` parameter unchanged. New code
/// SHOULD use `shared.shutdown.subscribe()`.
pub(crate) fn wire_shutdown_bus(
    shared: &Arc<SharedState>,
    system_metrics: &Arc<nodedb::control::metrics::SystemMetrics>,
) -> (
    tokio::sync::watch::Receiver<bool>,
    ShutdownBus,
    tokio::task::JoinHandle<()>,
) {
    let shutdown_rx = shared.shutdown.raw_receiver();

    // Unified shutdown bus: phased drain with per-phase 500 ms budgets.
    // `ShutdownBus::initiate()` signals the flat `ShutdownWatch` so all
    // existing `watch::Receiver<bool>` subscribers wake up as well.
    let (shutdown_bus, _shutdown_bus_handle) = ShutdownBus::new(Arc::clone(&shared.shutdown));
    // Wire system metrics so the bus records `nodedb_shutdown_phase_duration_seconds{phase}`
    // for each phase transition during graceful shutdown.
    shutdown_bus.set_metrics(Arc::clone(system_metrics));

    // This is deliberately startup-owned rather than signal-handler-owned:
    // every initiator of this exact bus must wait for every registry loop
    // before Event Plane drain can advance to durable finalization.
    let loop_registry_supervisor = spawn_loop_registry_shutdown_supervisor(
        Arc::clone(&shared.loop_registry),
        Arc::clone(&shared.shutdown),
        shared.tuning.shutdown.deadline(),
        shutdown_bus.clone(),
    );

    // Test-only injection: if NODEDB_TEST_SLOW_DRAIN_TASK=1, register a drain
    // task that sleeps for 2s without calling report_drained, to verify the
    // offender-abort path in integration tests. This code path is guarded
    // by an env var so it is never activated in production.
    if std::env::var("NODEDB_TEST_SLOW_DRAIN_TASK").as_deref() == Ok("1") {
        let mut guard = shutdown_bus.register_task(
            nodedb::control::shutdown::ShutdownPhase::DrainingListeners,
            "test_slow_task",
            None,
        );
        tokio::spawn(async move {
            guard.await_signal().await;
            // Intentionally do NOT call report_drained — tests the offender path.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(guard); // This will log the "dropped without report_drained" warning.
        });
    }

    (shutdown_rx, shutdown_bus, loop_registry_supervisor)
}

/// Register the one canonical LoopRegistry drain barrier and own its task
/// outside the registry itself, so strict shutdown never attempts to join its
/// own supervisor.
fn spawn_loop_registry_shutdown_supervisor(
    loop_registry: Arc<LoopRegistry>,
    shutdown: Arc<ShutdownWatch>,
    deadline: std::time::Duration,
    shutdown_bus: ShutdownBus,
) -> tokio::task::JoinHandle<()> {
    let mut guard = shutdown_bus.register_critical_task(
        nodedb::control::shutdown::ShutdownPhase::DrainingEventPlane,
        "shutdown::loop_registry",
    );

    tokio::spawn(async move {
        guard.await_signal().await;
        let report = loop_registry.shutdown_all_strict(&shutdown, deadline).await;
        if report.is_clean() {
            tracing::info!(
                clean = report.exited_clean.len(),
                total = ?report.total,
                "all background loops exited cleanly before the shutdown deadline"
            );
        } else {
            tracing::error!(
                clean = report.exited_clean.len(),
                laggards = ?report.laggards,
                total = ?report.total,
                "background loops exceeded shutdown deadline but all handles terminated before Event Plane drain completed"
            );
        }
        guard.report_drained();
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use nodedb::control::shutdown::{LoopHandle, ShutdownPhase};

    use super::*;

    #[tokio::test]
    async fn direct_bus_initiation_waits_for_strict_no_abort_loop() {
        let shutdown = Arc::new(ShutdownWatch::new());
        let loop_registry = Arc::new(LoopRegistry::new());
        let (shutdown_bus, mut shutdown_handle) = ShutdownBus::new(Arc::clone(&shutdown));
        let supervisor = spawn_loop_registry_shutdown_supervisor(
            Arc::clone(&loop_registry),
            Arc::clone(&shutdown),
            Duration::from_millis(20),
            shutdown_bus.clone(),
        );

        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let held_loop = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        loop_registry
            .register("test::held_no_abort", LoopHandle::AsyncNoAbort(held_loop))
            .expect("late registration before shutdown must be included");
        assert_eq!(
            loop_registry.live_count(),
            1,
            "supervisor must not self-register"
        );

        let sequencer = shutdown_bus.initiate();
        shutdown_handle
            .await_phase(ShutdownPhase::DrainingEventPlane)
            .await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            shutdown_bus.current_phase(),
            ShutdownPhase::DrainingEventPlane,
            "strict no-abort loop must block later shutdown phases"
        );

        release_tx.send(()).expect("release held loop");
        shutdown_handle.await_phase(ShutdownPhase::Closed).await;
        sequencer.await.expect("shutdown sequencer");
        supervisor.await.expect("loop registry supervisor");
    }
}
