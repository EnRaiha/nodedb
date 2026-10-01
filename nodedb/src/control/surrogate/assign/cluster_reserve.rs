// SPDX-License-Identifier: BUSL-1.1

//! Cross-node HiLo reservation path for the surrogate assigner.
//!
//! Each node reserves a disjoint `[start, end)` batch from the
//! metadata-Raft-replicated global watermark `G` and hands those out
//! locally (lock-free) until the batch drains, then reserves another.
//! [`SurrogateAssigner::run_refill_loop`] owns every reservation
//! round-trip. A draw that finds the batch empty awaits
//! [`SurrogateAssigner::await_reserved_batch`] and never blocks its worker.
//!
//! Which path a node uses (`Local` `alloc_one` vs `Cluster` HiLo
//! reservation) is decided once, at process start, by
//! [`SurrogateRegistry`]'s static mode — never inferred here from live
//! topology.

use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::oneshot;

use nodedb_types::Surrogate;

use super::super::registry::{RESERVE_BATCH_SIZE, SurrogateRegistry, SurrogateRegistryMode};
use super::core::SurrogateAssigner;
use crate::control::state::SharedState;

/// Upper bound on how long a cluster-mode reservation waits for its batch
/// to commit AND apply before failing. Must exceed the metadata-group
/// propose timeout so the commit-wait inside the proposer is the binding
/// deadline, not this outer guard.
const RESERVE_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Low-watermark for proactive background refill: once the reserved batch
/// drops below this many surrogates, the hot path nudges the background
/// refiller to reserve the next batch so the pool never drains on the
/// latency-critical insert path. A quarter of a batch leaves ample runway
/// for one metadata-Raft round-trip to complete before exhaustion.
const RESERVE_LOW_WATERMARK: u64 = (RESERVE_BATCH_SIZE / 4) as u64;

/// Backoff between refill-loop retries when a reservation fails transiently
/// (e.g. the metadata leader is not yet elected at startup). Short enough
/// that the eager first batch lands promptly once the leader is ready.
const REFILL_RETRY_BACKOFF: Duration = Duration::from_millis(100);

impl SurrogateAssigner {
    /// Allocate one surrogate from the registry under the write guard,
    /// dispatching on the registry's static mode.
    ///
    /// - `Local`: `alloc_one`. Returns `Ok(Some(s))` or propagates
    ///   `Exhausted`.
    /// - `Cluster`: `try_alloc_reserved`. `Ok(Some(s))` when the batch has
    ///   capacity; `Ok(None)` when it is empty — the caller drops the
    ///   lock and awaits `await_reserved_batch`. `ClusterCounter` has no `alloc_one`,
    ///   so `G` can only ever advance via `SurrogateReserve` apply.
    pub(super) fn alloc_locked(
        &self,
        registry: &SurrogateRegistry,
    ) -> crate::Result<Option<Surrogate>> {
        match registry.mode() {
            SurrogateRegistryMode::Local(local) => {
                let s = local.alloc_one()?;
                registry.record_allocs(1);
                Ok(Some(s))
            }
            SurrogateRegistryMode::Cluster(cluster) => Ok(cluster.try_alloc_reserved()),
        }
    }

    /// Proactive top-up trigger. When in `Cluster` mode and the node's
    /// reserved batch has dipped below the low-watermark, nudge the
    /// background refiller so it reserves the next batch BEFORE the current
    /// one drains. This is the mechanism that keeps the blocking
    /// metadata-Raft round-trip off the hot `assign` path in steady state.
    ///
    /// Called under the registry write guard (so the `remaining_reserved`
    /// read is consistent with the draw that happened). `Notify` is
    /// non-blocking and coalescing — a nudge while the refiller is already
    /// reserving is remembered as a single pending permit.
    pub(super) fn nudge_refill_if_low(&self, registry: &SurrogateRegistry) {
        let SurrogateRegistryMode::Cluster(cluster) = registry.mode() else {
            return;
        };
        if cluster.remaining_reserved() < RESERVE_LOW_WATERMARK {
            self.refill_notify.notify_one();
        }
    }

    /// Background reservation loop. Owns ALL eager + threshold batch
    /// reservation. A draw never runs the metadata-Raft round-trip itself: it
    /// awaits the batch this loop installs.
    ///
    /// Spawned once per node by `start_raft`. Self-gates on the
    /// registry's static `Cluster` mode, so it is a cheap no-op on a
    /// `Local`-mode (single-node) deployment. On a `Cluster`-mode node it:
    ///
    ///   1. Eagerly reserves the first batch on its very first iteration so a
    ///      batch is ready before any insert arrives.
    ///   2. Then waits on `refill_notify`, woken by the hot path when a draw
    ///      fails or the batch dips below the low-watermark, and tops the
    ///      batch back up by awaiting `reserve_batch`.
    ///
    /// The reservation runs on a dedicated background task, not the insert
    /// path. Transient
    /// failures (leader not yet elected at startup) are retried with a short
    /// backoff; the loop never panics and exits cleanly when `shared`'s weak
    /// upgrade fails (shutdown).
    pub async fn run_refill_loop(self: std::sync::Arc<Self>, shared: Weak<SharedState>) {
        // First iteration runs immediately (eager first-batch reservation);
        // every subsequent iteration waits for a nudge from the hot path.
        let mut eager = true;
        loop {
            if !eager {
                self.refill_notify.notified().await;
            }
            eager = false;

            // Shutdown: SharedState dropped → stop the loop.
            if shared.upgrade().is_none() {
                tracing::debug!("surrogate refill loop exiting: SharedState dropped");
                return;
            }

            // Self-gate + low-watermark check in one registry read:
            // `Local`-mode nodes have no background batch to maintain;
            // `Cluster`-mode nodes skip until the batch is genuinely low
            // (coalesced nudges can wake us after it's already full again).
            let remaining = {
                let guard = self.registry.read().unwrap_or_else(|p| p.into_inner());
                match guard.mode() {
                    SurrogateRegistryMode::Local(_) => continue,
                    SurrogateRegistryMode::Cluster(cluster) => cluster.remaining_reserved(),
                }
            };
            if remaining >= RESERVE_LOW_WATERMARK {
                continue;
            }

            // Perform the reservation off the hot path. Retry transient
            // failures (e.g. leader-not-ready at startup) with a small
            // backoff so the eager first batch lands as soon as the metadata
            // group is up; never panic.
            match self.reserve_batch().await {
                Ok(()) => {}
                Err(e) => {
                    tracing::debug!(error = %e, "surrogate background reservation failed; retrying");
                    tokio::time::sleep(REFILL_RETRY_BACKOFF).await;
                    // Re-arm so the loop retries promptly without needing a
                    // fresh hot-path nudge.
                    self.refill_notify.notify_one();
                }
            }
        }
    }

    /// Cluster-mode batch refill. Serialized so only one reservation is
    /// in flight per node. Registers a oneshot keyed by a fresh
    /// `request_id`, proposes `SurrogateReserve` (which commits then
    /// applies), and waits for BOTH the commit (`propose_surrogate_reserve`)
    /// AND the apply-time completion signal (the oneshot the applier
    /// fires once it has carved + installed the batch). On return the
    /// node's reserved batch is non-empty (unless another waiter drained
    /// it first, in which case the caller's retry reserves again).
    ///
    /// Driven only by the background [`run_refill_loop`], which awaits it off
    /// the insert path. MUST be called WITHOUT the registry write lock held — it does a Raft
    /// propose+wait whose apply handler needs registry (read) access.
    ///
    /// [`run_refill_loop`]: SurrogateAssigner::run_refill_loop
    pub(super) async fn reserve_batch(&self) -> crate::Result<()> {
        let shared =
            self.shared
                .get()
                .and_then(|w| w.upgrade())
                .ok_or_else(|| crate::Error::Internal {
                    detail: "surrogate reserve: SharedState unavailable in cluster mode".into(),
                })?;

        // Serialize reservations across this node so a burst of empty-
        // batch allocators doesn't over-reserve.
        let _gate = self.reserve_gate.lock().await;

        // After acquiring the gate, another reservation can have already
        // refilled the batch. Re-check before proposing to avoid wasting
        // a batch.
        {
            let guard = self.registry.read().unwrap_or_else(|p| p.into_inner());
            if matches!(guard.mode(), SurrogateRegistryMode::Cluster(c) if c.has_reserved()) {
                return Ok(());
            }
        }

        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        if let Ok(mut pending) = self.pending_reservations.lock() {
            pending.insert(request_id, tx);
        } else {
            return Err(crate::Error::Internal {
                detail: "surrogate reserve: pending map poisoned".into(),
            });
        }

        // Propose + wait for COMMIT. The carved range is NOT learned
        // here (wait_for returns on commit, before apply runs).
        let propose_result = crate::control::metadata_proposer::propose_surrogate_reserve(
            &shared,
            shared.node_id,
            request_id,
            RESERVE_BATCH_SIZE,
        )
        .await;
        if let Err(e) = propose_result {
            // Drop the dangling oneshot so the map doesn't leak.
            if let Ok(mut pending) = self.pending_reservations.lock() {
                pending.remove(&request_id);
            }
            return Err(crate::Error::Internal {
                detail: format!("surrogate reserve propose failed: {e}"),
            });
        }

        // Wait for APPLY: the applier fires the oneshot once it has
        // carved + installed the batch on this node. Bound the wait so a
        // lost apply (e.g. leadership churn) surfaces as a typed error
        // rather than hanging the allocation forever.
        let wait = tokio::time::timeout(RESERVE_WAIT_TIMEOUT, rx).await;
        match wait {
            Ok(Ok((_start, _end))) => Ok(()),
            Ok(Err(_recv_err)) => {
                // Sender dropped without sending — applier never fired.
                if let Ok(mut pending) = self.pending_reservations.lock() {
                    pending.remove(&request_id);
                }
                Err(crate::Error::Internal {
                    detail: "surrogate reserve: completion signal dropped before apply".into(),
                })
            }
            Err(_timeout) => {
                if let Ok(mut pending) = self.pending_reservations.lock() {
                    pending.remove(&request_id);
                }
                Err(crate::Error::Internal {
                    detail: "surrogate reserve: timed out waiting for batch apply".into(),
                })
            }
        }
    }

    /// Wait until this node's reserved batch has room, without blocking the
    /// runtime. The background refill loop reserves the batch, and
    /// `complete_reservation` wakes this wait once it installs it. Fails
    /// after `RESERVE_WAIT_TIMEOUT`.
    ///
    /// A `Local`-mode registry never runs empty, so the wait returns at once.
    pub(super) async fn await_reserved_batch(&self) -> crate::Result<()> {
        let wait = async {
            loop {
                // Registered before the check, so an install between the
                // check and the await still wakes this wait.
                let installed = self.batch_ready.notified();
                tokio::pin!(installed);
                installed.as_mut().enable();
                if self.has_reserved_room() {
                    return;
                }
                self.refill_notify.notify_one();
                installed.await;
            }
        };
        tokio::time::timeout(RESERVE_WAIT_TIMEOUT, wait)
            .await
            .map_err(|_| crate::Error::Internal {
                detail: "surrogate reserve: timed out waiting for the refill loop to install a \
                         batch"
                    .into(),
            })
    }

    /// Whether a draw from this node's registry can succeed now.
    fn has_reserved_room(&self) -> bool {
        let guard = self.registry.read().unwrap_or_else(|p| p.into_inner());
        match guard.mode() {
            SurrogateRegistryMode::Local(_) => true,
            SurrogateRegistryMode::Cluster(cluster) => cluster.has_reserved(),
        }
    }

    /// Register a waiter for `request_id`, as a reservation in flight does.
    #[cfg(test)]
    pub(crate) fn await_reservation_for_test(
        &self,
        request_id: u64,
    ) -> oneshot::Receiver<(u32, u32)> {
        let (tx, rx) = oneshot::channel();
        self.pending_reservations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(request_id, tx);
        rx
    }

    /// Called by the metadata applier on the owning node once a
    /// `SurrogateReserve` entry has carved the range `[start, end)`.
    ///
    /// The batch install is gated on a LIVE pending waiter: a oneshot for
    /// `request_id` only exists during a genuine in-process reservation, so
    /// its presence distinguishes a live reservation from a metadata-log
    /// REPLAY (where `pending_reservations` is empty after restart). This is
    /// the restart-safety hinge for the HiLo allocator:
    ///
    ///   - Live reservation: install the batch via `set_reserved_batch`
    ///     (BEFORE waking the waiter, so the woken allocator observes a
    ///     non-empty batch), then fire the oneshot to unblock `reserve_batch`.
    ///   - No waiter (replay of a historical reservation, or a request that
    ///     already timed out): NO-OP. We must NOT install the batch — on
    ///     replay the node can have already (partly) consumed its pre-crash
    ///     batch, so re-installing `[start, end)` hands those surrogates
    ///     out AGAIN. The global watermark `G` was already advanced
    ///     deterministically in the applier; the node reserves a fresh
    ///     batch on its next allocation (the pre-crash tail is abandoned,
    ///     which is the declared gap-tolerant design).
    ///
    /// A read guard on the registry is sufficient: `set_reserved_batch`
    /// mutates via interior atomics, preserving the no-deadlock property of
    /// the allocation path (which re-takes the write lock to retry).
    pub fn complete_reservation(&self, request_id: u64, start: u32, end: u32) {
        if let Ok(mut pending) = self.pending_reservations.lock()
            && let Some(tx) = pending.remove(&request_id)
        {
            // Live reservation: install the batch FIRST so the woken
            // allocator immediately sees a non-empty batch, THEN wake it.
            if let Ok(reg) = self.registry.read()
                && let SurrogateRegistryMode::Cluster(cluster) = reg.mode()
            {
                cluster.set_reserved_batch(start, end);
            }
            // Receiver can have already gone (timeout); ignore send error.
            let _ = tx.send((start, end));
            self.batch_ready.notify_waiters();
        }
        // No pending waiter → replay or timed-out request: do NOT install a
        // stale batch (see method doc). `G` was already advanced in the
        // applier, so this no-op is correct on every node.
    }
}
