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
    /// elapsed-time threshold has tripped. Runs only for a `Local`-mode
    /// registry; `CombinedPersist` also proposes `SurrogateAlloc { hwm }`
    /// so peers advance their watermark in step.
    ///
    /// A `Cluster`-mode registry is a no-op here — its watermark is
    /// advanced and persisted by the `SurrogateReserve` apply path, so a
    /// local flush double-advances `G` and corrupts determinism.
    pub(in crate::control::surrogate::assign) fn maybe_flush(
        &self,
        registry: &SurrogateRegistry,
        catalog: &SystemCatalog,
    ) -> crate::Result<()> {
        if registry.mode().is_cluster() {
            return Ok(());
        }
        if registry.should_flush() {
            self.flush_with(registry, Some(catalog))?;
        }
        Ok(())
    }

    /// The watermark a binding batch writes in its own catalog transaction:
    /// `Some(hwm)` when a `Local`-mode registry's flush threshold has tripped.
    /// The caller commits the row with its bindings, then calls
    /// [`Self::finish_committed_flush`] under the same registry lock.
    pub(in crate::control::surrogate::assign) fn due_flush_hwm(
        &self,
        registry: &SurrogateRegistry,
    ) -> Option<u32> {
        if registry.mode().is_cluster() || !registry.should_flush() {
            return None;
        }
        Some(registry.current_hwm())
    }

    /// Complete a flush whose catalog watermark row committed with a binding
    /// batch: append the WAL record, propose to peers, and reset the flush
    /// counters. Writes no catalog row.
    pub(in crate::control::surrogate::assign) fn finish_committed_flush(
        &self,
        registry: &SurrogateRegistry,
    ) -> crate::Result<()> {
        self.flush_with(registry, None)
    }

    fn flush_with(
        &self,
        registry: &SurrogateRegistry,
        catalog: Option<&SystemCatalog>,
    ) -> crate::Result<()> {
        let combined = CombinedPersist {
            catalog,
            load_from: self.credential_store.catalog(),
            wal_appender: self.wal_appender.as_ref(),
            raft_shared: self.shared.get().and_then(|w| w.upgrade()),
        };
        registry.flush(&combined)?;
        Ok(())
    }
}

/// `SurrogateHwmPersist` impl that writes the catalog row AND emits
/// the WAL record on every checkpoint. When `raft_shared` is set and
/// the node is in cluster mode, also proposes `SurrogateAlloc { hwm }`
/// to the metadata Raft group so followers advance their in-memory HWM.
struct CombinedPersist<'a> {
    /// The catalog the checkpoint writes the watermark row to. `None` when
    /// the row already committed with a binding batch.
    catalog: Option<&'a SystemCatalog>,
    /// The catalog `load` reads.
    load_from: &'a SystemCatalog,
    wal_appender: &'a dyn SurrogateWalAppender,
    /// Present when the Raft cluster is active; drives the Raft propose.
    raft_shared: Option<Arc<SharedState>>,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        if let Some(catalog) = self.catalog {
            catalog.put_surrogate_hwm(hwm)?;
        }
        self.wal_appender.record_alloc_to_wal(hwm)?;
        // Propose to Raft when in cluster mode so followers advance their
        // in-memory HWM. This is dispatched off the caller's thread and
        // never awaited here: the local write is ALREADY durable via the
        // catalog and WAL above, so the propose carries no correctness
        // weight for it — it only advances peers' (and a future joiner's)
        // view of the watermark.
        //
        // Awaiting it inline stalls the writer. `propose_surrogate_hwm`
        // waits up to the statement deadline for the entry to apply.
        // Surrogate assignment runs on the Raft apply loop as well as on the
        // coordinator. A flush from the apply path parks the very loop that
        // must commit the entry until the deadline passes.
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
        self.load_from.get_surrogate_hwm()
    }
}

/// Dispatch the `SurrogateAlloc { hwm }` metadata propose without blocking
/// the caller. The spawned task awaits the propose, so it runs on any runtime
/// flavor.
///
/// A missing reactor means this checkpoint ran outside a Tokio context, where
/// no propose can be issued at all. That is not silent data
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
        if let Err(e) = crate::control::metadata_proposer::propose_surrogate_hwm(&shared, hwm).await
        {
            tracing::warn!(hwm, error = %e, "surrogate hwm raft propose failed; followers may lag");
        }
    });
}
