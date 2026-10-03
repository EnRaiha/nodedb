// SPDX-License-Identifier: BUSL-1.1

//! Cross-engine surrogate high-watermark and HiLo-batch-reservation
//! host-side effects.

use tracing::{debug, warn};

use crate::control::surrogate::SurrogateRegistryMode;

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// Advance the in-memory surrogate high-watermark on every
    /// node. `restore_hwm` is idempotent and monotonic: calling
    /// it with a value at or below the current HWM is a no-op,
    /// so duplicate or reordered delivery cannot push the
    /// counter backwards. The hwm is persisted before the entry counts as
    /// applied, so boot never depends on re-reading the log.
    pub(super) fn apply_surrogate_alloc(
        &self,
        hwm: u32,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        {
            let reg = shared
                .surrogate_assigner
                .registry_handle()
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let restored = reg.restore_hwm(hwm);
            // The raised watermark, never below what this node already
            // issued: persisting the entry's `hwm` can lower the catalog
            // copy and reissue surrogates after a restart.
            let current = reg.current_hwm();
            drop(reg);
            // The in-memory HWM advance is correctness-critical: if it
            // fails this replica can re-issue a surrogate the cluster
            // already allocated. Do not advance past this entry — retry.
            if let Err(e) = restored {
                warn!(hwm, error = %e, "surrogate_alloc apply: restore_hwm failed — halting watermark for retry");
                return Err(crate::Error::Internal {
                    detail: format!("surrogate_alloc apply: restore_hwm failed: {e}"),
                });
            }
            // A failed persist returns `Err`: the entry is re-delivered, and
            // `restore_hwm` is a no-op on the retry, which persists again.
            self.credentials.catalog().put_surrogate_hwm(current)?;
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
    ///      via `from_persisted_cluster`, and `reserve_at_index` skips every
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
    ///      will hand those surrogates out AGAIN. So `G` advances
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
        let shared = self.shared_state()?;
        {
            // Read guard is sufficient: `reserve_at_index` mutates via
            // interior atomics (counter + last_reserve_index). Taking a
            // write guard here will risk deadlocking the allocation
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
            // this entry, or replicas will diverge on `G`. Surface it
            // so Raft re-delivers. This apply path only runs when
            // `start_raft` is active, which only happens when
            // `config.cluster.is_some()` — the same condition that puts
            // the registry in `Cluster` mode — so `Local` here means that
            // invariant broke. That is also a hard, retried-forever
            // error, not a silent fallback.
            let reserved = match reg.mode() {
                SurrogateRegistryMode::Cluster(cluster) => {
                    match cluster.reserve_at_index(raft_index, batch_size) {
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
                                detail: format!(
                                    "surrogate_reserve apply: reserve_at_index failed: {e}"
                                ),
                            });
                        }
                    }
                }
                SurrogateRegistryMode::Local(_) => {
                    drop(reg);
                    warn!(
                        node_id,
                        request_id,
                        "surrogate_reserve apply: registry is Local-mode but received a \
                         SurrogateReserve entry — this apply path should only run when \
                         config.cluster.is_some(), which also selects Cluster mode"
                    );
                    return Err(crate::Error::Internal {
                        detail: "surrogate_reserve apply: registry is Local-mode but the \
                                 metadata Raft apply path is running (should be impossible \
                                 when config.cluster.is_some() selects Cluster mode)"
                            .into(),
                    });
                }
            };
            let current_hwm = reg.current_hwm();
            drop(reg);

            let catalog = self.credentials.catalog();
            let Some((start, end)) = reserved else {
                // Already applied in memory (replay, duplicate delivery, or
                // a re-delivery after a failed persist): do NOT advance `G`.
                // When the persisted cursor is behind this entry, the earlier
                // persist failed, so this delivery writes it. The applier
                // stops at a failed entry, so the in-memory `G` is exactly
                // this entry's carve.
                if catalog.get_surrogate_reserve_index()? < raft_index {
                    catalog.put_surrogate_reserve_state(current_hwm, raft_index)?;
                }
                // The carve kept from the failed first application goes to
                // the allocator still waiting on it. After a restart no carve
                // is kept and no allocator waits.
                let kept = self
                    .unpersisted_carve
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take_if(|(index, _, _)| *index == raft_index);
                if let Some((_, start, end)) = kept
                    && node_id == shared.node_id
                {
                    shared
                        .surrogate_assigner
                        .complete_reservation(request_id, start, end);
                }
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
            // reservation (no double-count) and seed an already-equal `G`
            // on every node. A failed persist returns `Err`: the entry is
            // re-delivered, and the branch above persists it and hands the
            // kept carve to the waiting allocator.
            if let Err(e) = catalog.put_surrogate_reserve_state(end - 1, raft_index) {
                *self
                    .unpersisted_carve
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()) = Some((raft_index, start, end));
                warn!(
                    node_id,
                    request_id,
                    hwm = end - 1,
                    raft_index,
                    error = %e,
                    "surrogate_reserve apply: failed to persist reserve state; the entry is \
                     re-delivered"
                );
                return Err(e);
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

#[cfg(test)]
mod tests {
    use nodedb_cluster::{MetadataApplier, MetadataEntry, encode_entry};

    use super::super::test_fixture::{applier_with_shared_at, cluster_applier_with_shared_at};

    /// A failed hwm persist stops the entry. The re-delivered entry persists
    /// and applies.
    #[tokio::test(flavor = "multi_thread")]
    async fn surrogate_hwm_persist_error_stops_the_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (applier, state) = applier_with_shared_at(dir.path(), "test.wal");
        let entry = encode_entry(&MetadataEntry::SurrogateAlloc { hwm: 500 }).unwrap();
        let catalog = state.credentials.catalog();

        catalog.fail_next_surrogate_write_for_test();
        assert_eq!(applier.apply(&[(3, entry.clone())]).await, 0);

        assert_eq!(applier.apply(&[(3, entry)]).await, 3);
        assert!(catalog.get_surrogate_hwm().unwrap() >= 500);
    }

    /// A reserve whose persist failed keeps its carve. The re-delivered entry
    /// persists it and hands that exact batch to the allocator waiting on it.
    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_redelivery_hands_the_kept_carve_to_the_waiter() {
        let dir = tempfile::tempdir().unwrap();
        let (applier, state) = cluster_applier_with_shared_at(dir.path(), "test.wal");
        let catalog = state.credentials.catalog();
        let mut waiter = state.surrogate_assigner.await_reservation_for_test(77);
        let entry = encode_entry(&MetadataEntry::SurrogateReserve {
            node_id: state.node_id,
            request_id: 77,
            batch_size: 16,
        })
        .unwrap();

        catalog.fail_next_surrogate_write_for_test();
        assert_eq!(applier.apply(&[(5, entry.clone())]).await, 0);
        assert!(
            waiter.try_recv().is_err(),
            "no batch before the carve is durable"
        );

        assert_eq!(applier.apply(&[(5, entry)]).await, 5);
        let (start, end) = waiter
            .try_recv()
            .expect("the kept carve reaches the waiter");
        assert_eq!(end - start, 16);
        assert_eq!(catalog.get_surrogate_reserve_index().unwrap(), 5);
        assert_eq!(catalog.get_surrogate_hwm().unwrap(), end - 1);
    }
}
