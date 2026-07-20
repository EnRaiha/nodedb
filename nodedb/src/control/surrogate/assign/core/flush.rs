// SPDX-License-Identifier: BUSL-1.1

//! Local flush-trigger + durable checkpoint persistence for
//! [`super::SurrogateAssigner`].

use std::sync::Arc;

use super::super::super::persist::SurrogateHwmPersist;
use super::super::super::registry::SurrogateRegistry;
use super::super::super::wal_appender::SurrogateWalAppender;
use super::types::SurrogateAssigner;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

impl SurrogateAssigner {
    /// Highest surrogate ever issued by this assigner.  Used by `CLONE
    /// DATABASE` to capture the source's surrogate high-water at the
    /// AS-OF point — every binding allocated *after* this value belongs
    /// strictly to source-side writes that must NOT be visible from the
    /// resulting clone.  Returns `0` on a fresh assigner.
    pub fn current_hwm(&self) -> u32 {
        self.registry
            .read()
            .map(|reg| reg.current_hwm())
            .unwrap_or_else(|p| p.into_inner().current_hwm())
    }

    /// Local flush trigger: durably checkpoint the new hwm if the ops or
    /// elapsed-time threshold has tripped. This runs whenever the node is
    /// NOT using the cross-node reservation path — i.e. on a single-node
    /// (no Raft) deployment OR a single-member-with-Raft deployment. In
    /// the latter case the flush's `CombinedPersist` also proposes
    /// `SurrogateAlloc { hwm }` so the metadata watermark `G` stays in
    /// sync with the locally-allocated hwm; this gives a future node-join
    /// a correct base to advance past (see `should_use_reservation`
    /// follow-up (1)).
    ///
    /// When the reservation path IS in use (multi-member metadata group)
    /// this is a no-op — the global watermark is advanced and persisted
    /// by the `SurrogateReserve` apply path, so running the local flush
    /// here would double-advance `counter` (which is `G` in that mode)
    /// and corrupt determinism.
    pub(in crate::control::surrogate::assign) fn maybe_flush(
        &self,
        registry: &SurrogateRegistry,
        catalog: &SystemCatalog,
    ) -> crate::Result<()> {
        if self.should_use_reservation() {
            return Ok(());
        }
        if registry.should_flush() {
            let combined = CombinedPersist {
                catalog,
                wal_appender: self.wal_appender.as_ref(),
                raft_shared: self.shared.get().and_then(|w| w.upgrade()),
            };
            registry.flush(&combined)?;
        }
        Ok(())
    }
}

/// `SurrogateHwmPersist` impl that writes the catalog row AND emits
/// the WAL record on every checkpoint. When `raft_shared` is set and
/// the node is in cluster mode, also proposes `SurrogateAlloc { hwm }`
/// to the metadata Raft group so followers advance their in-memory HWM.
struct CombinedPersist<'a> {
    catalog: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
    /// Present when the Raft cluster is active; drives the Raft propose.
    raft_shared: Option<Arc<SharedState>>,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)?;
        self.wal_appender.record_alloc_to_wal(hwm)?;
        // Propose to Raft when in cluster mode so followers advance their
        // in-memory HWM. This is dispatched off the caller's thread and
        // never awaited here: the local write is ALREADY durable via the
        // catalog and WAL above, so the propose carries no correctness
        // weight for it — it only advances peers' (and a future joiner's)
        // view of the watermark.
        //
        // Awaiting it inline was a liveness bug. `propose_surrogate_hwm`
        // blocks for `DEFAULT_PROPOSE_TIMEOUT` (5s) waiting for the entry
        // to commit, and surrogate assignment runs on the Raft apply loop
        // as well as on the coordinator — so a flush triggered from the
        // apply path parked the very loop that had to commit the entry,
        // and only unwound when the timeout fired. Every such write ate a
        // 5s stall, which is what made Lite's sync deltas time out before
        // Origin could ack them.
        //
        // Out-of-order or duplicate delivery is safe: `apply_surrogate_alloc`
        // advances the watermark through `restore_hwm`, which is idempotent
        // and monotonic and never moves the counter backwards.
        if let Some(shared) = &self.raft_shared {
            spawn_hwm_propose(Arc::clone(shared), hwm);
        }
        Ok(())
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}

/// Dispatch the `SurrogateAlloc { hwm }` metadata propose without blocking
/// the caller.
///
/// Spawned as a normal runtime task rather than via `spawn_blocking` because
/// `propose_surrogate_hwm` uses `block_in_place` internally, which is only
/// legal on a multi-threaded runtime worker.
///
/// A missing reactor means this checkpoint ran outside a Tokio context, where
/// the propose could not have been issued at all. That is not silent data
/// loss — the hwm is already durable in the catalog and WAL, and the next
/// flush that does run under a reactor re-proposes the (higher) watermark —
/// but it is logged so a node that never advances peer watermarks is
/// diagnosable rather than invisible.
fn spawn_hwm_propose(shared: Arc<SharedState>, hwm: u32) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            hwm,
            "surrogate hwm checkpoint ran outside a Tokio runtime; \
             skipping the metadata propose (hwm is durable locally; \
             the next flush under a reactor re-proposes it)"
        );
        return;
    };
    handle.spawn(async move {
        if let Err(e) = crate::control::metadata_proposer::propose_surrogate_hwm(&shared, hwm) {
            tracing::warn!(hwm, error = %e, "surrogate hwm raft propose failed; followers may lag");
        }
    });
}
