// SPDX-License-Identifier: BUSL-1.1

//! Registry of every background loop on this node.
//!
//! Holds the join handles so `shutdown_all` can actually
//! observe "did loop X exit?" rather than hoping the watch
//! signal was honored. Async laggards are aborted after the
//! deadline; blocking laggards are loudly logged (tokio cannot
//! abort a blocking thread — that's a platform fact, not a
//! shortcut).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use super::report::{LaggardReport, ShutdownReport};

/// Handle variant — async (tokio task) vs blocking (spawn_blocking).
#[derive(Debug)]
pub enum LoopHandle {
    /// Tokio task. Can be `.abort()`'d on laggard.
    Async(JoinHandle<()>),
    /// Tokio task that MUST NOT be `.abort()`'d — it advances a
    /// replicated / durable state machine and a mid-flight
    /// cancellation would leave it diverged from its peers or
    /// strand committed-but-unapplied work. Joined on shutdown
    /// exactly like [`Self::Async`], but a laggard past the
    /// deadline is reported and LEFT RUNNING rather than
    /// force-cancelled. The loop is responsible for observing
    /// shutdown promptly at a safe internal boundary (e.g. a
    /// Calvin scheduler breaking at an epoch boundary, or the
    /// raft apply loop finishing its current drain batch).
    AsyncNoAbort(JoinHandle<()>),
    /// `spawn_blocking` task. The join handle still exists,
    /// but aborting is a no-op — it only cancels scheduling,
    /// not the running thread. Laggards are logged, not
    /// aborted.
    Blocking(JoinHandle<()>),
}

impl LoopHandle {
    fn take_handle(self) -> (JoinHandle<()>, bool) {
        match self {
            Self::Async(h) => (h, true),
            Self::AsyncNoAbort(h) => (h, false),
            Self::Blocking(h) => (h, false),
        }
    }
}

/// Error returned by [`LoopRegistry::register`] if
/// `shutdown_all` has already been invoked. Prevents a race
/// where a background spawn completes registration after the
/// registry has moved past the shutdown report.
#[derive(Debug, thiserror::Error)]
#[error("loop registry is closed — cannot register \"{name}\"")]
pub struct RegistryClosed {
    /// Name of the loop that attempted to register too late.
    pub name: &'static str,
}

/// Per-handle record, carrying just enough context to produce
/// a useful shutdown report and to decide abort vs log.
#[derive(Debug)]
struct LoopEntry {
    name: &'static str,
    handle: LoopHandle,
    registered_at: Instant,
}

#[derive(Debug, Default)]
struct Inner {
    handles: Vec<LoopEntry>,
    closed: bool,
}

/// Shared registry. Held by `SharedState` in an `Arc`.
#[derive(Debug, Default)]
pub struct LoopRegistry {
    inner: Mutex<Inner>,
}

/// Lock the registry's inner state. On poisoning — which only
/// happens if a previous holder panicked while mutating the
/// registry — log loudly at `error!` before recovering the
/// guard. Shutdown must still proceed (aborting it on a panic
/// somewhere else would leak background loops), but operators
/// MUST see the signal.
fn lock_inner<'a>(inner: &'a Mutex<Inner>, site: &'static str) -> std::sync::MutexGuard<'a, Inner> {
    match inner.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!(
                site,
                "LoopRegistry mutex poisoned — a previous holder panicked while mutating \
                 the registry. Recovering the guard so shutdown can still proceed, but \
                 this is a bug and the panic source should be investigated."
            );
            poisoned.into_inner()
        }
    }
}

impl LoopRegistry {
    /// New empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a join handle under a stable name. Rejects
    /// once `shutdown_all` has been invoked.
    pub fn register(&self, name: &'static str, handle: LoopHandle) -> Result<(), RegistryClosed> {
        let mut guard = lock_inner(&self.inner, "register");
        if guard.closed {
            return Err(RegistryClosed { name });
        }
        guard.handles.push(LoopEntry {
            name,
            handle,
            registered_at: Instant::now(),
        });
        Ok(())
    }

    /// Number of registered handles currently alive. Useful
    /// for tests and for observability counters.
    pub fn live_count(&self) -> usize {
        lock_inner(&self.inner, "live_count").handles.len()
    }

    /// Close the registry and await every registered handle
    /// with a shared `deadline`. Handles that do not complete
    /// by the deadline:
    ///
    /// - Async handles are `.abort()`'d and recorded as
    ///   laggards with `aborted = true`.
    /// - Blocking handles are left running (there is no way
    ///   to force-kill them from tokio) and recorded as
    ///   laggards with `aborted = false`.
    ///
    /// The returned [`ShutdownReport`] lists every handle in
    /// exactly one of the two vectors.
    pub async fn shutdown_all(&self, deadline: Duration) -> ShutdownReport {
        let start = Instant::now();

        // Drain handles under the lock, then release it so
        // we're not holding a Mutex across `.await`.
        let entries: Vec<LoopEntry> = {
            let mut guard = lock_inner(&self.inner, "shutdown_all");
            guard.closed = true;
            std::mem::take(&mut guard.handles)
        };

        let mut exited_clean: Vec<&'static str> = Vec::with_capacity(entries.len());
        let mut laggards: Vec<LaggardReport> = Vec::new();

        for entry in entries {
            let LoopEntry {
                name,
                handle,
                registered_at,
            } = entry;
            let (mut join, can_abort) = handle.take_handle();

            let elapsed_budget = deadline.saturating_sub(start.elapsed());
            if elapsed_budget.is_zero() {
                // Deadline already consumed — treat anything
                // still outstanding as a laggard without
                // awaiting.
                laggards.push(
                    abort_and_report_laggard(&mut join, name, registered_at, start, can_abort)
                        .await,
                );
                continue;
            }

            match tokio::time::timeout(elapsed_budget, &mut join).await {
                Ok(Ok(())) => exited_clean.push(name),
                Ok(Err(join_err)) => {
                    // Task panicked or was previously
                    // cancelled. Treat as exited — shutdown
                    // doesn't care whether the body
                    // completed normally; a panic is already
                    // a bug that surfaced elsewhere.
                    tracing::warn!(
                        loop_name = name,
                        error = %join_err,
                        "background loop exited with error during shutdown"
                    );
                    exited_clean.push(name);
                }
                Err(_) => {
                    laggards.push(
                        abort_and_report_laggard(&mut join, name, registered_at, start, can_abort)
                            .await,
                    );
                }
            }
        }

        ShutdownReport {
            exited_clean,
            laggards,
            total: start.elapsed(),
        }
    }
}

async fn abort_and_report_laggard(
    handle: &mut JoinHandle<()>,
    name: &'static str,
    registered_at: Instant,
    shutdown_start: Instant,
    can_abort: bool,
) -> LaggardReport {
    if can_abort {
        handle.abort();
        let _ = handle.await;
    }
    LaggardReport {
        name,
        uptime: registered_at.elapsed(),
        wait_elapsed: shutdown_start.elapsed(),
        aborted: can_abort,
    }
}

#[cfg(test)]
mod tests {
    use super::super::ShutdownWatch;
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn clean_exit_all_handles_finish() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        for name in ["a", "b", "c"] {
            let mut rx = watch.subscribe();
            let handle = tokio::spawn(async move {
                rx.wait_cancelled().await;
            });
            registry
                .register(name, LoopHandle::Async(handle))
                .expect("register");
        }

        assert_eq!(registry.live_count(), 3);
        watch.signal();

        let report = registry.shutdown_all(Duration::from_millis(200)).await;
        assert!(report.is_clean(), "{report}");
        assert_eq!(report.exited_clean.len(), 3);
        assert!(report.total < Duration::from_millis(150));
    }

    #[tokio::test]
    async fn laggard_detected_and_aborted() {
        let registry = LoopRegistry::new();
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        registry
            .register("sleepy", LoopHandle::Async(handle))
            .expect("register");

        let report = registry.shutdown_all(Duration::from_millis(50)).await;
        assert!(!report.is_clean());
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "sleepy");
        assert!(report.laggards[0].aborted);
    }

    #[tokio::test]
    async fn register_after_close_rejected() {
        let registry = LoopRegistry::new();
        let _ = registry.shutdown_all(Duration::from_millis(10)).await;

        let late = tokio::spawn(async {});
        let err = registry
            .register("late", LoopHandle::Async(late))
            .unwrap_err();
        assert_eq!(err.name, "late");
    }

    #[tokio::test]
    async fn empty_registry_returns_empty_report() {
        let registry = LoopRegistry::new();
        let report = registry.shutdown_all(Duration::from_millis(10)).await;
        assert!(report.is_clean());
        assert_eq!(report.loop_count(), 0);
    }

    #[tokio::test]
    async fn blocking_loop_laggard_is_reported_not_aborted() {
        let registry = LoopRegistry::new();
        // spawn_blocking with a short sleep that will exceed
        // our deadline — the report must flag it as a
        // laggard but NOT mark it aborted, because
        // `LoopHandle::Blocking` leaves it running.
        let handle = tokio::task::spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(500));
        });
        registry
            .register("blocking", LoopHandle::Blocking(handle))
            .expect("register");

        let report = registry.shutdown_all(Duration::from_millis(30)).await;
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "blocking");
        assert!(!report.laggards[0].aborted);
    }

    #[tokio::test]
    async fn mixed_clean_and_laggard_accounting() {
        let watch = Arc::new(ShutdownWatch::new());
        let registry = LoopRegistry::new();

        let mut r1 = watch.subscribe();
        let quick = tokio::spawn(async move { r1.wait_cancelled().await });
        registry
            .register("quick", LoopHandle::Async(quick))
            .unwrap();

        let slow = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        registry.register("slow", LoopHandle::Async(slow)).unwrap();

        watch.signal();
        let report = registry.shutdown_all(Duration::from_millis(100)).await;
        assert_eq!(report.exited_clean, vec!["quick"]);
        assert_eq!(report.laggards.len(), 1);
        assert_eq!(report.laggards[0].name, "slow");
    }
}
