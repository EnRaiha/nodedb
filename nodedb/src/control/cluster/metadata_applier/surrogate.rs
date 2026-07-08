// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine surrogate high-watermark and HiLo-batch-reservation
//! host-side effects.

use tracing::{debug, warn};

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// Advance the in-memory surrogate high-watermark on every
    /// node. `restore_hwm` is idempotent and monotonic: calling
    /// it with a value at or below the current HWM is a no-op,
    /// so duplicate or reordered delivery cannot push the
    /// counter backwards. Also persist the hwm to the catalog so
    /// the local node survives a restart without re-reading the
    /// full log.
    pub(super) fn apply_surrogate_alloc(
        &self,
        hwm: u32,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let reg = shared
                .surrogate_assigner
                .registry_handle()
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let restored = reg.restore_hwm(hwm);
            drop(reg);
            // The in-memory HWM advance is correctness-critical: if it
            // fails this replica could re-issue a surrogate the cluster
            // already allocated. Do not advance past this entry — retry.
            if let Err(e) = restored {
                warn!(hwm, error = %e, "surrogate_alloc apply: restore_hwm failed — halting watermark for retry");
                return Err(crate::Error::Internal {
                    detail: format!("surrogate_alloc apply: restore_hwm failed: {e}"),
                });
            }
            // Best-effort catalog persist: a failure means the
            // next restart will re-derive the HWM from the log
            // (the log is the source of truth), which is correct —
            // just slightly slower. Tolerate and continue.
            let catalog = self.credentials.catalog();
            if let Err(e) = catalog.put_surrogate_hwm(hwm) {
                warn!(
                    hwm,
                    error = %e,
                    "surrogate_alloc apply: failed to persist hwm to catalog (tolerable; log is authoritative)"
                );
            }
            debug!(hwm, raft_index, "surrogate hwm advanced via raft");
        }
        Ok(())
    }

    /// HiLo batch reservation. The carved range is computed
    /// HERE — deterministically — by advancing the global
    /// watermark on EVERY node in identical Raft log order, so
    /// all nodes agree which `[start, end)` this reservation
    /// owns and no two nodes ever mint the same surrogate.
    ///
    /// RESTART SAFETY (critical): the metadata Raft group has no
    /// snapshot, so on every (re)start `last_applied` resets to 0
    /// and the FULL committed log is replayed from index 1. This
    /// arm therefore runs once per historical reservation on each
    /// start. Three consequences drive the design:
    ///
    ///   1. `G` must advance EXACTLY ONCE per reservation across the
    ///      lifetime of the node — not once per replay. The carved
    ///      hwm AND the applied-reserve cursor (`raft_index`) are
    ///      persisted to the catalog ATOMICALLY on first
    ///      application; on restart the registry is seeded with both
    ///      via `from_persisted`, and `reserve_at_index` skips every
    ///      reservation whose index `<= cursor` (already folded into
    ///      the seeded `G`). Entries committed-but-not-yet-persisted
    ///      before a crash have index `> cursor` and are re-applied
    ///      (correct — they were not in the seed). Because the carve
    ///      is computed identically on every node, the persisted hwm
    ///      is EQUAL cluster-wide.
    ///
    ///   2. The reserved batch must NOT be installed during replay.
    ///      A node that crashed mid-batch already consumed part of
    ///      its pre-crash `[start, end)`; re-installing it on replay
    ///      would hand those surrogates out AGAIN. So `G` advances
    ///      (deterministic, every node) but the batch install is
    ///      gated on a LIVE pending waiter, which only exists during
    ///      a genuine in-process reservation (`pending_reservations`
    ///      is empty after restart). On replay no waiter exists → no
    ///      batch is installed → the node reserves a fresh batch on
    ///      first alloc; the crashed node's pre-crash batch tail is
    ///      abandoned (the declared gap-tolerant design).
    ///
    ///   3. A replayed/duplicate reservation (`reserve_at_index`
    ///      returns `None`) is a strict no-op: `G` is not advanced,
    ///      nothing is persisted, no batch is installed.
    pub(super) fn apply_surrogate_reserve(
        &self,
        node_id: u64,
        request_id: u64,
        batch_size: u32,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            // Read guard is sufficient: `reserve_at_index` mutates via
            // interior atomics (counter + last_reserve_index). Taking a
            // write guard here would risk deadlocking the allocation
            // path, which holds no registry lock across the propose+wait
            // but does re-take it to retry.
            let reg = shared
                .surrogate_assigner
                .registry_handle()
                .read()
                .unwrap_or_else(|p| p.into_inner());
            // Advancing the global watermark is correctness-critical and
            // must be deterministic across nodes incl. replay: an
            // exhaustion error must NOT advance the apply watermark past
            // this entry, or replicas would diverge on `G`. Surface it
            // so Raft re-delivers.
            let reserved = match reg.reserve_at_index(raft_index, batch_size) {
                Ok(r) => r,
                Err(e) => {
                    drop(reg);
                    warn!(
                        node_id,
                        request_id,
                        batch_size,
                        error = %e,
                        "surrogate_reserve apply: reserve_at_index failed — halting watermark for retry"
                    );
                    return Err(crate::Error::Internal {
                        detail: format!("surrogate_reserve apply: reserve_at_index failed: {e}"),
                    });
                }
            };
            drop(reg);

            let Some((start, end)) = reserved else {
                // Already applied (full-log replay / duplicate
                // delivery): do NOT advance `G`, do NOT persist, do NOT
                // install a batch. Advancing the apply watermark past a
                // replayed entry is correct — its effect is already in
                // the seeded state.
                debug!(
                    node_id,
                    request_id,
                    raft_index,
                    "surrogate_reserve apply: index already applied (replay/dup) — skipped"
                );
                return Ok(());
            };

            // First application: persist `(hwm = end - 1, cursor =
            // raft_index)` ATOMICALLY so a restart can skip this
            // reservation on replay (no double-count) and seed an
            // already-equal `G` on every node. Best-effort (warn on
            // fail) like the `SurrogateAlloc` arm: if the persist fails,
            // the log is still authoritative and the next restart
            // re-derives `G` by replaying from the last durable cursor —
            // correct, just slightly slower. The hwm and cursor are
            // written together in one redb txn, so a crash can never
            // leave them inconsistent.
            let catalog = self.credentials.catalog();
            if let Err(e) = catalog.put_surrogate_reserve_state(end - 1, raft_index) {
                warn!(
                    node_id,
                    request_id,
                    hwm = end - 1,
                    raft_index,
                    error = %e,
                    "surrogate_reserve apply: failed to persist reserve state to catalog \
                     (tolerable; log is authoritative)"
                );
            }

            if node_id == shared.node_id {
                // Install the batch + wake the waiter ONLY when a live
                // pending reservation exists for this `request_id`.
                // During replay there is no waiter, so
                // `complete_reservation` is a no-op — but replay never
                // reaches here anyway (it returns `None` above). The
                // install happens BEFORE the wake (inside
                // `complete_reservation`) so the woken allocator
                // immediately observes a non-empty batch.
                shared
                    .surrogate_assigner
                    .complete_reservation(request_id, start, end);
            }
            debug!(
                node_id,
                request_id, start, end, raft_index, "surrogate batch reserved via raft"
            );
        }
        Ok(())
    }
}
