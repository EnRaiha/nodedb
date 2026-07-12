// SPDX-License-Identifier: BUSL-1.1

//! Loop-spawning helpers that atomically (1) subscribe to the
//! shutdown watch, (2) spawn the body, and (3) register the
//! join handle in the loop registry.
//!
//! Every background loop in the Control Plane and Event Plane
//! MUST go through these helpers. An unregistered loop is
//! invisible to `LoopRegistry::shutdown_all` and cannot be
//! waited on during graceful shutdown — that is the bug this
//! batch fixes.
//!
//! Naming: pass a `&'static str` literal so the name is
//! compile-time and cheap to log. Dynamic names would defeat
//! the purpose of the registry's structured reporting.

use std::future::Future;

use super::{LoopHandle, LoopRegistry, ShutdownReceiver, ShutdownWatch};

/// Spawn an async loop on the tokio runtime. The `body` closure
/// receives a [`ShutdownReceiver`] it MUST select on to observe
/// shutdown.
///
/// Example:
///
/// ```ignore
/// use crate::control::shutdown::spawn_loop;
///
/// spawn_loop(
///     &shared.loop_registry,
///     &shared.shutdown,
///     "wal_catchup",
///     |mut shutdown| async move {
///         let mut tick = tokio::time::interval(Duration::from_millis(50));
///         loop {
///             tokio::select! {
///                 _ = shutdown.wait_cancelled() => break,
///                 _ = tick.tick() => { /* work */ }
///             }
///         }
///     },
/// );
/// ```
///
/// The return value is `()` — the handle is owned by the
/// registry. If registration fails (registry already closed),
/// the task is still spawned to completion and a WARN is
/// logged. This keeps callers simple: there is no error case
/// the caller can meaningfully handle, because a registration
/// race means shutdown has already fired.
pub fn spawn_loop<F, Fut>(
    registry: &LoopRegistry,
    shutdown: &ShutdownWatch,
    name: &'static str,
    body: F,
) where
    F: FnOnce(ShutdownReceiver) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let rx = shutdown.subscribe();
    let handle = tokio::spawn(async move { body(rx).await });
    if let Err(e) = registry.register(name, LoopHandle::Async(handle)) {
        tracing::warn!(
            error = %e,
            "spawn_loop after registry close — task will run to completion \
             but shutdown_all will not wait for it"
        );
    }
}

/// Spawn an async loop that is joined on shutdown but NEVER
/// force-aborted, even if it outlives the shutdown deadline.
///
/// Same registration and signal semantics as [`spawn_loop`],
/// except the registered handle is [`LoopHandle::AsyncNoAbort`]:
/// `shutdown_all` still waits for it, but a laggard past the
/// deadline is reported and LEFT RUNNING rather than
/// `.abort()`'d.
///
/// Use for determinism- or durability-critical loops that MUST
/// exit only at a safe internal boundary — a Calvin `Scheduler`
/// (a mid-epoch `.abort()` would diverge this node's replicated
/// state machine from its peers), or the raft apply loop (an
/// abort mid-apply would strand committed-but-unapplied
/// entries). The body MUST observe shutdown promptly via the
/// [`ShutdownReceiver`] so it reaches that boundary well within
/// the deadline; the no-abort guarantee is a correctness
/// backstop, not a licence to shut down slowly.
pub fn spawn_loop_no_abort<F, Fut>(
    registry: &LoopRegistry,
    shutdown: &ShutdownWatch,
    name: &'static str,
    body: F,
) where
    F: FnOnce(ShutdownReceiver) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let rx = shutdown.subscribe();
    let handle = tokio::spawn(async move { body(rx).await });
    if let Err(e) = registry.register(name, LoopHandle::AsyncNoAbort(handle)) {
        tracing::warn!(
            error = %e,
            "spawn_loop_no_abort after registry close — task will run to \
             completion but shutdown_all will not wait for it"
        );
    }
}

/// Spawn a blocking loop via `tokio::task::spawn_blocking`.
/// Same semantics as [`spawn_loop`] except the body runs on a
/// blocking thread and is NOT abort-able by the registry —
/// laggards are logged, not force-cancelled.
///
/// Use for loops that call synchronous I/O or long-running
/// computation. The body closure receives a
/// [`ShutdownReceiver`] that can be polled via
/// `is_cancelled()` inside its hot loop, or passed to a tokio
/// runtime if the blocking thread drives one.
pub fn spawn_blocking_loop<F>(
    registry: &LoopRegistry,
    shutdown: &ShutdownWatch,
    name: &'static str,
    body: F,
) where
    F: FnOnce(ShutdownReceiver) + Send + 'static,
{
    let rx = shutdown.subscribe();
    let handle = tokio::task::spawn_blocking(move || body(rx));
    if let Err(e) = registry.register(name, LoopHandle::Blocking(handle)) {
        tracing::warn!(
            error = %e,
            "spawn_blocking_loop after registry close — thread will run to \
             completion but shutdown_all will not wait for it"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn spawn_loop_registers_and_runs() {
        let registry = LoopRegistry::new();
        let watch = Arc::new(ShutdownWatch::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_c = Arc::clone(&counter);

        spawn_loop(&registry, &watch, "test_loop", |mut rx| async move {
            loop {
                tokio::select! {
                    _ = rx.wait_cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        counter_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        // Let it tick a few times.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(registry.live_count(), 1);
        assert!(counter.load(Ordering::Relaxed) >= 1);

        watch.signal();
        let report = registry.shutdown_all(Duration::from_millis(100)).await;
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.exited_clean, vec!["test_loop"]);
    }

    #[tokio::test]
    async fn spawn_loop_body_receives_signal() {
        let registry = LoopRegistry::new();
        let watch = Arc::new(ShutdownWatch::new());

        spawn_loop(&registry, &watch, "wait_only", |mut rx| async move {
            rx.wait_cancelled().await;
        });

        watch.signal();
        let report = registry.shutdown_all(Duration::from_millis(50)).await;
        assert!(report.is_clean());
    }

    #[tokio::test]
    async fn spawn_blocking_loop_runs_on_blocking_thread() {
        let registry = LoopRegistry::new();
        let watch = Arc::new(ShutdownWatch::new());
        let done = Arc::new(AtomicUsize::new(0));
        let done_c = Arc::clone(&done);

        spawn_blocking_loop(&registry, &watch, "blocker", move |rx| {
            // Poll is_cancelled() periodically — this is the
            // correct idiom for a blocking thread that wants
            // to observe shutdown without blocking on async.
            while !rx.is_cancelled() {
                std::thread::sleep(Duration::from_millis(2));
            }
            done_c.store(1, Ordering::SeqCst);
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        watch.signal();
        let report = registry.shutdown_all(Duration::from_millis(200)).await;
        assert!(report.is_clean(), "{report}");
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }
}
