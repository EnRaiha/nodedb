// SPDX-License-Identifier: BUSL-1.1

//! Unified shutdown bus: phased drain with a 500 ms per-phase budget.
//!
//! # Overview
//!
//! `ShutdownBus` orchestrates an ordered shutdown across all NodeDB
//! subsystems. It advances through [`ShutdownPhase`]s in sequence,
//! waiting up to `PHASE_BUDGET` for all tasks registered to that phase
//! to call [`DrainGuard::report_drained`]. Normal tasks that miss the budget
//! are aborted (async) or logged (blocking) as offenders. Explicitly critical
//! tasks log once at the budget but hold their phase until cleanup completes.
//!
//! # Usage
//!
//! ```ignore
//! let (bus, handle) = ShutdownBus::new();
//! // Register a task for the DrainingListeners phase:
//! let guard = bus.register_task(ShutdownPhase::DrainingListeners, "pgwire");
//! // In the task:
//! guard.await_signal().await;
//! do_cleanup();
//! guard.report_drained();
//!
//! // Trigger shutdown from signal handler:
//! bus.initiate();
//! handle.await_phase(ShutdownPhase::Closed).await;
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info};

use super::phase::ShutdownPhase;
use super::{LoopHandle, LoopRegistry, ShutdownWatch};
use crate::control::metrics::SystemMetrics;

/// Per-phase drain budget. Normal tasks are aborted and logged on expiry;
/// explicitly critical cleanup is logged once and remains a phase barrier.
pub const PHASE_BUDGET: Duration = Duration::from_millis(500);

/// Unique task identifier within the bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(u64);

/// Internal record per registered task.
struct TaskEntry {
    name: &'static str,
    phase: ShutdownPhase,
    /// Set to true when `DrainGuard::report_drained` is called.
    drained: bool,
    /// Tokio join handle for abort on budget expiry. `None` for tasks
    /// whose join handle was not provided (blocking threads).
    abort_handle: Option<tokio::task::AbortHandle>,
    /// Critical tasks are allowed to exceed the phase budget because later
    /// phases would violate correctness while their cleanup is incomplete.
    critical: bool,
    /// Ensures an over-budget critical task is logged once, rather than once
    /// per shutdown-loop poll.
    over_budget_logged: bool,
}

#[derive(Default)]
struct BusState {
    tasks: BTreeMap<TaskId, TaskEntry>,
    next_id: u64,
    initiated: bool,
    /// Optional metrics sink — set after construction via `ShutdownBus::set_metrics`.
    metrics: Option<Arc<SystemMetrics>>,
}

impl BusState {
    fn alloc_id(&mut self) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;
        id
    }

    fn pending_for_phase(&self, phase: ShutdownPhase) -> Vec<(TaskId, &'static str)> {
        self.tasks
            .iter()
            .filter(|(_, e)| e.phase == phase && !e.drained)
            .map(|(id, e)| (*id, e.name))
            .collect()
    }

    fn handle_phase_budget_expiry(&mut self, phase: ShutdownPhase) {
        for entry in self.tasks.values_mut() {
            if entry.phase != phase || entry.drained {
                continue;
            }
            if entry.critical {
                if !entry.over_budget_logged {
                    error!(
                        target: "shutdown",
                        phase = %phase,
                        offender = entry.name,
                        "critical task exceeded 500ms drain budget — waiting for correctness-critical cleanup"
                    );
                    entry.over_budget_logged = true;
                }
                continue;
            }
            if let Some(ref handle) = entry.abort_handle {
                handle.abort();
            }
            error!(
                target: "shutdown",
                phase = %phase,
                offender = entry.name,
                "task exceeded 500ms drain budget — aborting"
            );
            // Normal task behavior intentionally remains bounded: even a task
            // without an abort handle cannot hold shutdown indefinitely.
            entry.drained = true;
        }
    }
}

/// The unified shutdown bus. Held by `main.rs` (or `SharedState`).
///
/// Clone-cheap: all clones share the same underlying state.
#[derive(Clone)]
pub struct ShutdownBus {
    state: Arc<Mutex<BusState>>,
    phase_tx: Arc<watch::Sender<ShutdownPhase>>,
    /// The underlying flat watch. All existing `ShutdownWatch`-based
    /// subscribers (listeners, Event Plane, etc.) keep working —
    /// `initiate()` also signals this watch.
    flat_watch: Arc<ShutdownWatch>,
}

/// Subscriber handle — allows waiting for a specific phase.
#[derive(Clone)]
pub struct ShutdownHandle {
    phase_rx: watch::Receiver<ShutdownPhase>,
    flat_watch: Arc<ShutdownWatch>,
}

/// Returned by `ShutdownBus::register_task` or
/// [`ShutdownBus::register_critical_task`]. Normal tasks must call
/// `report_drained()` before the per-phase budget expires or are treated as
/// offenders; critical tasks hold their phase until they report completion.
///
/// Dropping without calling `report_drained()` is treated as a missed drain.
/// Normal tasks allow phase advancement after the budget; a critical task
/// intentionally keeps its phase blocked until some owner reports completion.
pub struct DrainGuard {
    task_id: TaskId,
    phase: ShutdownPhase,
    state: Arc<Mutex<BusState>>,
    phase_rx: watch::Receiver<ShutdownPhase>,
    /// False until `report_drained` is called. Used in `Drop`.
    reported: bool,
    name: &'static str,
}

impl DrainGuard {
    /// Async wait: resolves when the bus enters the phase this task was
    /// registered for. The task should then perform its cleanup and call
    /// `report_drained()`.
    pub async fn await_signal(&mut self) {
        // Fast path: already at or past our phase.
        if *self.phase_rx.borrow() >= self.phase {
            return;
        }
        while self.phase_rx.changed().await.is_ok() {
            if *self.phase_rx.borrow() >= self.phase {
                return;
            }
        }
    }

    /// Report that this task has finished its drain work. Must be called
    /// before the phase budget expires to avoid being logged as an offender.
    pub fn report_drained(mut self) {
        self.reported = true;
        let mut guard = lock_bus(&self.state);
        if let Some(entry) = guard.tasks.get_mut(&self.task_id) {
            entry.drained = true;
        }
    }
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        if !self.reported {
            // Log as offender but don't abort — the task body may have
            // already exited (e.g. future dropped). The phase budget timer
            // handles abort on its own schedule.
            tracing::warn!(
                target: "shutdown",
                phase = %self.phase,
                offender = self.name,
                "DrainGuard dropped without report_drained — task may be a shutdown offender"
            );
        }
    }
}

fn lock_bus(state: &Mutex<BusState>) -> std::sync::MutexGuard<'_, BusState> {
    match state.lock() {
        Ok(g) => g,
        Err(p) => {
            error!(target: "shutdown", "ShutdownBus mutex poisoned — recovering");
            p.into_inner()
        }
    }
}

impl ShutdownBus {
    /// Create a new `ShutdownBus`. Returns the bus (for registering tasks
    /// and initiating shutdown) and a `ShutdownHandle` (for waiting on
    /// specific phases from other contexts).
    ///
    /// The `flat_watch` is the node's canonical `ShutdownWatch` held on
    /// `SharedState`. When `initiate()` is called it also signals the flat
    /// watch so all existing `watch::Receiver<bool>` subscribers wake up.
    pub fn new(flat_watch: Arc<ShutdownWatch>) -> (Self, ShutdownHandle) {
        let (phase_tx, phase_rx) = watch::channel(ShutdownPhase::Running);
        let phase_tx = Arc::new(phase_tx);
        let bus = Self {
            state: Arc::new(Mutex::new(BusState::default())),
            phase_tx,
            flat_watch: Arc::clone(&flat_watch),
        };
        let handle = ShutdownHandle {
            phase_rx,
            flat_watch,
        };
        (bus, handle)
    }

    /// Register a task for the given drain phase. Returns a `DrainGuard`
    /// the task must hold until its cleanup is complete.
    ///
    /// `abort_handle`: if `Some`, the task will be aborted if it misses
    /// the budget. Pass `None` for blocking threads.
    pub fn register_task(
        &self,
        drain_at: ShutdownPhase,
        name: &'static str,
        abort_handle: Option<tokio::task::AbortHandle>,
    ) -> DrainGuard {
        self.register(drain_at, name, abort_handle, false)
    }

    /// Register cleanup whose completion is a correctness barrier for every
    /// subsequent shutdown phase. A critical task is never aborted or marked
    /// drained on the phase budget; shutdown waits for its `DrainGuard`.
    pub fn register_critical_task(
        &self,
        drain_at: ShutdownPhase,
        name: &'static str,
    ) -> DrainGuard {
        self.register(drain_at, name, None, true)
    }

    fn register(
        &self,
        drain_at: ShutdownPhase,
        name: &'static str,
        abort_handle: Option<tokio::task::AbortHandle>,
        critical: bool,
    ) -> DrainGuard {
        let mut guard = lock_bus(&self.state);
        let id = guard.alloc_id();
        guard.tasks.insert(
            id,
            TaskEntry {
                name,
                phase: drain_at,
                drained: false,
                abort_handle,
                critical,
                over_budget_logged: false,
            },
        );
        let phase_rx = self.phase_tx.subscribe();
        DrainGuard {
            task_id: id,
            phase: drain_at,
            state: Arc::clone(&self.state),
            phase_rx,
            reported: false,
            name,
        }
    }

    /// Initiate graceful shutdown. Idempotent — second call is a no-op.
    ///
    /// This spawns a background Tokio task that advances through phases
    /// sequentially, each with a 500 ms budget. The caller does not need
    /// to await the returned handle — the phase watch is observable via
    /// `ShutdownHandle::await_phase`.
    pub fn initiate(&self) -> JoinHandle<()> {
        {
            let mut guard = lock_bus(&self.state);
            if guard.initiated {
                // Already initiated — return a no-op future.
                return tokio::spawn(async {});
            }
            guard.initiated = true;
        }

        info!(target: "shutdown", "shutdown initiated");

        // Signal the flat watch so all existing `watch::Receiver<bool>`
        // subscribers (listeners, loops registered via spawn_loop) wake up.
        self.flat_watch.signal();

        let state = Arc::clone(&self.state);
        let phase_tx = Arc::clone(&self.phase_tx);

        tokio::spawn(async move {
            let mut current = ShutdownPhase::Running;

            while let Some(next) = current.next() {
                // Signal all tasks for `current` phase that drain time has arrived.
                phase_tx.send_replace(current);

                // Wait up to PHASE_BUDGET for all tasks registered at `current`
                // to call report_drained().
                let phase_start = std::time::Instant::now();
                let deadline = tokio::time::Instant::now() + PHASE_BUDGET;
                let mut budget_expired = false;
                loop {
                    let pending = lock_bus(&state).pending_for_phase(current);
                    if pending.is_empty() {
                        break;
                    }
                    if !budget_expired && tokio::time::Instant::now() >= deadline {
                        lock_bus(&state).handle_phase_budget_expiry(current);
                        budget_expired = true;
                    }
                    // Critical tasks intentionally hold this phase after its
                    // budget expires; poll without re-logging their timeout.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                let phase_ms = phase_start.elapsed().as_millis() as u64;
                // Record phase duration into the metrics sink if one is wired.
                {
                    let guard = lock_bus(&state);
                    if let Some(ref m) = guard.metrics {
                        m.record_shutdown_phase_duration(&current.to_string(), phase_ms);
                    }
                }

                info!(
                    target: "shutdown",
                    phase = %current,
                    next_phase = %next,
                    duration_ms = phase_ms,
                    "shutdown phase complete"
                );

                current = next;
            }

            // Advance to Closed.
            phase_tx.send_replace(ShutdownPhase::Closed);
            info!(target: "shutdown", "shutdown complete");
        })
    }

    /// Current phase. Non-blocking poll.
    pub fn current_phase(&self) -> ShutdownPhase {
        *self.phase_tx.borrow()
    }

    /// Wire a metrics sink so the bus records `nodedb_shutdown_phase_duration_seconds{phase}`
    /// for each phase transition during shutdown.
    ///
    /// Must be called before `initiate()` to have effect. Idempotent.
    pub fn set_metrics(&self, metrics: Arc<SystemMetrics>) {
        let mut guard = lock_bus(&self.state);
        guard.metrics = Some(metrics);
    }

    /// Subscribe a new `ShutdownHandle`.
    pub fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            phase_rx: self.phase_tx.subscribe(),
            flat_watch: Arc::clone(&self.flat_watch),
        }
    }
}

impl ShutdownHandle {
    /// Async wait: resolves when the bus has reached or passed `phase`.
    pub async fn await_phase(&mut self, phase: ShutdownPhase) {
        if *self.phase_rx.borrow() >= phase {
            return;
        }
        while self.phase_rx.changed().await.is_ok() {
            if *self.phase_rx.borrow() >= phase {
                return;
            }
        }
    }

    /// Whether shutdown has been initiated (phase > Running).
    pub fn is_shutting_down(&self) -> bool {
        *self.phase_rx.borrow() > ShutdownPhase::Running
    }

    /// Returns a clone of the underlying flat `ShutdownWatch`.
    pub fn flat_watch(&self) -> &Arc<ShutdownWatch> {
        &self.flat_watch
    }
}

/// Register a loop with both the `LoopRegistry` (flat await) AND the
/// `ShutdownBus` (phased drain). The loop gets a `DrainGuard` it should
/// hold and call `report_drained()` on when cleanup finishes, plus it
/// is registered in the registry so `shutdown_all` can wait for its
/// join handle.
///
/// Use this instead of `spawn_loop` for tasks that participate in
/// phased shutdown.
pub fn spawn_drainable<F, Fut>(
    registry: &LoopRegistry,
    bus: &ShutdownBus,
    drain_at: ShutdownPhase,
    name: &'static str,
    body: F,
) where
    F: FnOnce(super::ShutdownReceiver, DrainGuard) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let rx = bus.flat_watch.subscribe();
    // We need the abort handle before spawning, so we use a oneshot channel.
    // Instead, spawn first and register the abort handle via the bus after.
    // The simplest approach: register without an abort handle initially (the
    // LoopRegistry's abort via JoinHandle covers the same task).
    let guard = bus.register_task(drain_at, name, None);
    let handle = tokio::spawn(async move { body(rx, guard).await });
    let abort = handle.abort_handle();
    // Patch the abort handle into the bus entry — we re-register with the
    // correct abort handle using a separate method.
    // For simplicity, patch via the shared state directly.
    // (The DrainGuard's task_id is inside the spawned closure now, so
    //  we can't easily patch. Use a different approach: register the guard
    //  before spawning, then wire abort separately via the join handle.)
    //
    // Since we can't patch after the fact without exposing internals,
    // we register the join handle with the LoopRegistry for flat abort.
    if let Err(e) = registry.register(name, LoopHandle::Async(handle)) {
        tracing::warn!(
            error = %e,
            "spawn_drainable after registry close — task will run to completion \
             but shutdown_all will not wait for it"
        );
    }
    drop(abort); // Suppress unused warning — abort via JoinHandle in registry.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn initiate_is_idempotent() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, mut handle) = ShutdownBus::new(Arc::clone(&watch));
        bus.initiate();
        bus.initiate(); // second call must not panic or double-advance
        handle.await_phase(ShutdownPhase::Closed).await;
        assert_eq!(bus.current_phase(), ShutdownPhase::Closed);
    }

    #[tokio::test]
    async fn flat_watch_signaled_on_initiate() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, _) = ShutdownBus::new(Arc::clone(&watch));
        assert!(!watch.is_shutdown());
        bus.initiate();
        // Give the spawned task a tick to run.
        tokio::task::yield_now().await;
        assert!(watch.is_shutdown());
    }

    #[tokio::test]
    async fn registered_task_receives_drain_signal() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, mut global_handle) = ShutdownBus::new(Arc::clone(&watch));

        let drained = Arc::new(AtomicBool::new(false));
        let drained_c = Arc::clone(&drained);

        let mut guard = bus.register_task(ShutdownPhase::DrainingListeners, "test_task", None);
        tokio::spawn(async move {
            guard.await_signal().await;
            drained_c.store(true, Ordering::SeqCst);
            guard.report_drained();
        });

        bus.initiate();
        global_handle.await_phase(ShutdownPhase::Closed).await;
        assert!(drained.load(Ordering::SeqCst), "task did not drain");
    }

    #[tokio::test]
    async fn connection_listener_drain_barrier_blocks_later_phases_until_drained() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, mut handle) = ShutdownBus::new(Arc::clone(&watch));
        // Connection-owning listeners (pgwire, HTTP, ILP, RESP) can exceed
        // the normal 500ms task budget while they finish their own drain.
        let guard = bus.register_critical_task(
            ShutdownPhase::DrainingListeners,
            "connection_listener_drain_barrier",
        );

        bus.initiate();
        handle.await_phase(ShutdownPhase::DrainingListeners).await;
        tokio::time::sleep(PHASE_BUDGET + Duration::from_millis(50)).await;

        assert_eq!(bus.current_phase(), ShutdownPhase::DrainingListeners);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                handle.await_phase(ShutdownPhase::DrainingDataPlane),
            )
            .await
            .is_err(),
            "an over-budget listener drain must block Data-Plane shutdown"
        );

        guard.report_drained();
        handle.await_phase(ShutdownPhase::Closed).await;
        assert_eq!(bus.current_phase(), ShutdownPhase::Closed);
    }

    #[tokio::test]
    async fn offender_aborted_after_budget() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, mut handle) = ShutdownBus::new(Arc::clone(&watch));

        // Register a task that NEVER calls report_drained and never runs.
        let _guard = bus.register_task(ShutdownPhase::DrainingListeners, "offender_task", None);
        // Don't spawn anything — the guard is held in the test, report_drained is never called.
        // The DrainGuard drop will log a warning; the phase budget will expire and advance.

        let start = tokio::time::Instant::now();
        bus.initiate();
        handle.await_phase(ShutdownPhase::Closed).await;

        // Should complete within ~600ms (budget 500ms + some overhead for 7 phases,
        // but DrainingListeners is the first non-Running phase and the guard is dropped
        // which triggers the warning path, but does NOT mark as drained. The budget
        // timer fires after 500ms and aborts).
        let elapsed = start.elapsed();
        // 7 phases × 500ms = 3.5s max. We just verify it terminates.
        assert!(
            elapsed < Duration::from_secs(10),
            "shutdown did not terminate: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn await_phase_returns_immediately_if_already_past() {
        let watch = Arc::new(ShutdownWatch::new());
        let (bus, _) = ShutdownBus::new(Arc::clone(&watch));
        bus.initiate();

        let mut handle = bus.handle();
        // Wait for Closed, then check that a subsequent await_phase(Running)
        // returns immediately.
        handle.await_phase(ShutdownPhase::Closed).await;

        let mut handle2 = bus.handle();
        tokio::time::timeout(
            Duration::from_millis(10),
            handle2.await_phase(ShutdownPhase::Running),
        )
        .await
        .expect("await_phase(Running) should be immediate when already Closed");
    }
}
