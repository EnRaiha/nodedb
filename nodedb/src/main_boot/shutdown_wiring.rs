// SPDX-License-Identifier: BUSL-1.1

//! Unified shutdown-bus wiring, plus the test-only slow-drain-task
//! injection used by integration tests to exercise the offender-abort
//! path.

use std::sync::Arc;

use nodedb::control::shutdown::ShutdownBus;
use nodedb::control::state::SharedState;

/// Build the phased shutdown bus, wire it to `SharedState`'s
/// `ShutdownWatch` and system metrics, and — only when
/// `NODEDB_TEST_SLOW_DRAIN_TASK=1` — register a task that deliberately
/// never reports drained, to verify the offender-abort path in
/// integration tests. Pure relocation of what used to be inline in
/// `main()`.
///
/// Returns `(shutdown_rx, shutdown_bus)`. All shutdown signals flow
/// through the canonical `ShutdownWatch` held on `SharedState`; the
/// returned raw receiver is a view of that same watch, preserved so the
/// existing listener APIs (`PgListener::run`, `HttpServer::run`,
/// `IlpListener::run`, `RespListener::run`, `spawn_cold_storage_loop`,
/// `spawn_checkpoint_loop`, and the lease renewal loop) keep their
/// `watch::Receiver<bool>` parameter unchanged. New code SHOULD use
/// `shared.shutdown.subscribe()`.
pub(crate) fn wire_shutdown_bus(
    shared: &Arc<SharedState>,
    system_metrics: &Arc<nodedb::control::metrics::SystemMetrics>,
) -> (tokio::sync::watch::Receiver<bool>, ShutdownBus) {
    let shutdown_rx = shared.shutdown.raw_receiver();

    // Unified shutdown bus: phased drain with per-phase 500 ms budgets.
    // `ShutdownBus::initiate()` signals the flat `ShutdownWatch` so all
    // existing `watch::Receiver<bool>` subscribers wake up as well.
    let (shutdown_bus, _shutdown_bus_handle) = ShutdownBus::new(Arc::clone(&shared.shutdown));
    // Wire system metrics so the bus records `nodedb_shutdown_phase_duration_seconds{phase}`
    // for each phase transition during graceful shutdown.
    shutdown_bus.set_metrics(Arc::clone(system_metrics));

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

    (shutdown_rx, shutdown_bus)
}
