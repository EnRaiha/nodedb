// SPDX-License-Identifier: BUSL-1.1

//! Cross-node HiLo reservation path for the surrogate assigner.
//!
//! In a multi-node cluster every surrogate must be globally unique, so a
//! node cannot just `alloc_one` from its local counter — it would collide
//! with peers. Instead each node reserves a disjoint `[start, end)` batch
//! from the metadata-Raft-replicated global watermark `G` and hands those
//! out locally (lock-free) until the batch drains, then reserves another.
//!
//! The reservation itself is a BLOCKING metadata-Raft round-trip. To keep
//! that off the latency-critical `assign` insert path, reservation is owned
//! by a per-node background task ([`SurrogateAssigner::run_refill_loop`],
//! spawned in `start_raft`): it eagerly reserves the first batch at startup
//! and tops the batch up whenever the hot path nudges it below
//! [`RESERVE_LOW_WATERMARK`]. The hot path itself only ever performs the
//! lock-free `try_alloc_reserved` draw; the synchronous [`ensure_batch`]
//! refill remains solely as a rare safety net to preserve liveness if the
//! refiller has not yet caught up.
//!
//! [`ensure_batch`]: SurrogateAssigner::ensure_batch

use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::oneshot;

use nodedb_types::Surrogate;

use super::super::registry::{RESERVE_BATCH_SIZE, SurrogateRegistry};
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
    /// branching on whether the cross-node reservation path is required
    /// (`should_use_reservation`).
    ///
    /// - Local path (single-member metadata group or no metadata Raft):
    ///   `alloc_one`. Returns `Ok(Some(s))` or propagates `Exhausted`.
    /// - Reservation path (multi-member metadata group):
    ///   `try_alloc_reserved`. `Ok(Some(s))` when the batch has capacity;
    ///   `Ok(None)` when it is empty — the caller must drop the lock and
    ///   call `ensure_batch`. Never calls `alloc_one` so the global
    ///   watermark is advanced ONLY by `SurrogateReserve` apply.
    pub(super) fn alloc_locked(
        &self,
        registry: &SurrogateRegistry,
    ) -> crate::Result<Option<Surrogate>> {
        if self.should_use_reservation() {
            Ok(registry.try_alloc_reserved())
        } else {
            Ok(Some(registry.alloc_one()?))
        }
    }

    /// Proactive top-up trigger. When the reservation path is active and the
    /// node's reserved batch has dipped below the low-watermark, nudge the
    /// background refiller so it reserves the next batch BEFORE the current
    /// one drains. This is the mechanism that keeps the blocking
    /// metadata-Raft round-trip off the hot `assign` path in steady state.
    ///
    /// Called under the registry write guard (so the `remaining_reserved`
    /// read is consistent with the draw that just happened). `Notify` is
    /// non-blocking and coalescing — a nudge while the refiller is already
    /// reserving is remembered as a single pending permit.
    pub(super) fn nudge_refill_if_low(&self, registry: &SurrogateRegistry) {
        if !self.should_use_reservation() {
            return;
        }
        if registry.remaining_reserved() < RESERVE_LOW_WATERMARK {
            self.refill_notify.notify_one();
        }
    }

    /// Background reservation loop. Owns ALL eager + threshold batch
    /// reservation so the latency-critical `assign` path never blocks on the
    /// metadata-Raft round-trip in the common case.
    ///
    /// Spawned once per node by `start_raft`. Self-gates via
    /// `should_use_reservation`, so it is a cheap no-op on single-node /
    /// single-member deployments (those allocate locally via `alloc_one` and
    /// never reserve). On a genuine multi-node cluster it:
    ///
    ///   1. Eagerly reserves the first batch on its very first iteration so a
    ///      batch is ready before any insert arrives.
    ///   2. Then waits on `refill_notify`, woken by the hot path when a draw
    ///      fails or the batch dips below the low-watermark, and tops the
    ///      batch back up via the existing blocking `ensure_batch` mechanics.
    ///
    /// The blocking wait inside `ensure_batch` is acceptable HERE because this
    /// runs on a dedicated background task, not the insert path. Transient
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

            // Self-gate: only multi-node clusters use the reservation path.
            // Single-node nodes never need a background batch; just park
            // until something (a future promotion) nudges us again.
            if !self.should_use_reservation() {
                continue;
            }

            // Only reserve when the batch is genuinely low. Coalesced nudges
            // may wake us after the batch is already full again.
            let remaining = self
                .registry
                .read()
                .map(|r| r.remaining_reserved())
                .unwrap_or_else(|p| p.into_inner().remaining_reserved());
            if remaining >= RESERVE_LOW_WATERMARK {
                continue;
            }

            // Perform the reservation off the hot path. Retry transient
            // failures (e.g. leader-not-ready at startup) with a small
            // backoff so the eager first batch lands as soon as the metadata
            // group is up; never panic.
            match self.ensure_batch() {
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

    /// True when surrogate allocation must go through the HiLo
    /// cross-node reservation path instead of the fast local
    /// `alloc_one`.
    ///
    /// The reservation exists for ONE reason: to prevent CROSS-NODE
    /// surrogate collisions. A reservation costs a once-per-batch
    /// BLOCKING metadata-Raft round-trip; with the background refiller this
    /// is paid OFF the `assign()` hot path (see module docs). On a
    /// single-node-with-Raft deployment that round-trip contends with the
    /// shared raft tick loop that also drives other groups (e.g. the Calvin
    /// sequencer), so we must NOT pay it when there is no peer to collide
    /// with. Concretely we return `true` only when BOTH hold:
    ///   (a) `metadata_raft` is present, AND
    ///   (b) the METADATA Raft group (group 0) has MORE THAN ONE member.
    /// A single-member metadata group has no collision risk → fast local
    /// path. If the member count is unavailable for any reason we treat
    /// the node as single-node (return `false`, the safe local path).
    /// The read is cheap and read-only.
    ///
    /// DECLARED, OUT-OF-SCOPE FOLLOW-UPS (surfaced, not buried):
    ///
    /// (1) **single→multi-node transition barrier.** A node that
    ///     allocated surrogates via the local `alloc_one` path and then
    ///     gains a peer could collide: the first multi-node reservation
    ///     must start PAST every locally-allocated surrogate. The
    ///     node-add / rebalance flow must, at join time, flush this
    ///     node's local hwm into the metadata watermark `G` (a barrier
    ///     owned by that flow) before the first reservation is carved.
    ///     Until that barrier exists, do not promote a single node that
    ///     has been allocating locally into a multi-node group.
    ///
    /// (2) **blocking once-per-batch round-trip on the hot path — RESOLVED.**
    ///     Batch reservation is now owned by a per-node background task
    ///     (`run_refill_loop`, spawned in `start_raft`): it eagerly reserves
    ///     the first batch at startup and tops up whenever the hot path
    ///     nudges it (`refill_notify`) below `RESERVE_LOW_WATERMARK`. The
    ///     hot path only ever does the lock-free `try_alloc_reserved`; the
    ///     synchronous `ensure_batch` call remains solely as a rare
    ///     safety-net fallback for the (now near-impossible) case where the
    ///     refiller has not caught up, preserving liveness.
    pub(super) fn should_use_reservation(&self) -> bool {
        // Monotonic latch: this is on the per-row allocation hot path
        // (every `assign` on the coordinator AND on every node's apply
        // loop). Once the node has observed a genuine multi-node cluster
        // we cache that decision and never re-read the contended topology /
        // routing RwLocks again — a cluster that became multi-node stays
        // on the reservation path (staying there is always correct, just
        // unoptimized, even if it later shrinks). This keeps steady-state
        // allocation free of Arc upgrade + RwLock + membership lookup.
        if self.reservation_latched.load(Ordering::Relaxed) {
            return true;
        }
        let Some(shared) = self.shared.get().and_then(|w| w.upgrade()) else {
            return false;
        };
        // (a) metadata Raft must be present at all.
        if shared.metadata_raft.get().is_none() {
            return false;
        }
        // Topology is the collision-risk source of truth. During join/startup
        // a node's routing table can still show metadata group 0 as
        // single-member, while topology already knows there are peer nodes
        // receiving logs. Latch from topology first so those nodes don't fall
        // back to local `alloc_one` and periodic `SurrogateAlloc` flushes.
        if let Some(topology) = shared.cluster_topology.as_ref() {
            let member_count = match topology.read() {
                Ok(guard) => guard
                    .all_nodes()
                    .filter(|node| node.state.receives_log())
                    .count(),
                Err(_poisoned) => return false,
            };
            if member_count > 1 {
                self.reservation_latched.store(true, Ordering::Relaxed);
                return true;
            }
        }
        // (b) the metadata group (group 0) must have more than one
        // member. Unavailable routing → treat as single-node.
        let Some(routing) = shared.cluster_routing.as_ref() else {
            return false;
        };
        let member_count = match routing.read() {
            Ok(guard) => guard
                .group_info(nodedb_cluster::METADATA_GROUP_ID)
                .map(|info| info.members.len())
                .unwrap_or(0),
            Err(_poisoned) => return false,
        };
        let multi = member_count > 1;
        if multi {
            // Latch so subsequent hot-path calls skip the RwLock read.
            self.reservation_latched.store(true, Ordering::Relaxed);
        }
        multi
    }

    /// Latch the assigner into cluster reservation mode from startup wiring
    /// that already has authoritative topology membership. This avoids making
    /// the per-row allocator infer cluster shape from a routing table that may
    /// not yet be fully visible on every node.
    pub fn enable_reservation_mode(&self) {
        self.reservation_latched.store(true, Ordering::Relaxed);
    }

    /// Cluster-mode batch refill. Serialized so only one reservation is
    /// in flight per node. Registers a oneshot keyed by a fresh
    /// `request_id`, proposes `SurrogateReserve` (which commits then
    /// applies), and waits for BOTH the commit (`propose_surrogate_reserve`)
    /// AND the apply-time completion signal (the oneshot the applier
    /// fires once it has carved + installed the batch). On return the
    /// node's reserved batch is non-empty (unless another waiter drained
    /// it first, in which case the caller's retry simply reserves again).
    ///
    /// Driven primarily by the background [`run_refill_loop`], where the
    /// blocking propose+wait is off the insert path; `assign` only calls it
    /// as a rare safety-net fallback. MUST be called WITHOUT the registry
    /// write lock held — it does a Raft propose+wait whose apply handler
    /// needs registry (read) access.
    ///
    /// [`run_refill_loop`]: SurrogateAssigner::run_refill_loop
    pub(super) fn ensure_batch(&self) -> crate::Result<()> {
        let shared =
            self.shared
                .get()
                .and_then(|w| w.upgrade())
                .ok_or_else(|| crate::Error::Internal {
                    detail: "surrogate reserve: SharedState unavailable in cluster mode".into(),
                })?;

        // Serialize reservations across this node so a burst of empty-
        // batch allocators doesn't over-reserve. Block synchronously on
        // the async gate — `assign` is a sync API called within the tokio
        // runtime (same contract as the existing propose path).
        let handle = tokio::runtime::Handle::current();
        let _gate = tokio::task::block_in_place(|| handle.block_on(self.reserve_gate.lock()));

        // After acquiring the gate, another reservation may have already
        // refilled the batch. Re-check before proposing to avoid wasting
        // a batch.
        if self
            .registry
            .read()
            .map(|r| r.has_reserved())
            .unwrap_or_else(|p| p.into_inner().has_reserved())
        {
            return Ok(());
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
        );
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
        let wait = tokio::task::block_in_place(|| {
            handle.block_on(async { tokio::time::timeout(RESERVE_WAIT_TIMEOUT, rx).await })
        });
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
    ///     non-empty batch), then fire the oneshot to unblock `ensure_batch`.
    ///   - No waiter (replay of a historical reservation, or a request that
    ///     already timed out): NO-OP. We must NOT install the batch — on
    ///     replay the node may have already (partly) consumed its pre-crash
    ///     batch, so re-installing `[start, end)` would hand those surrogates
    ///     out AGAIN. The global watermark `G` was already advanced
    ///     deterministically in the applier; the node simply reserves a fresh
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
            if let Ok(reg) = self.registry.read() {
                reg.set_reserved_batch(start, end);
            }
            // Receiver may have already gone (timeout); ignore send error.
            let _ = tx.send((start, end));
        }
        // No pending waiter → replay or timed-out request: do NOT install a
        // stale batch (see method doc). `G` was already advanced in the
        // applier, so this no-op is correct on every node.
    }
}
