// SPDX-License-Identifier: BUSL-1.1

//! Local flush-trigger + durable checkpoint persistence for
//! [`super::SurrogateAssigner`].

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
            let raft_shared = self.shared.get().and_then(|w| w.upgrade());
            let combined = CombinedPersist {
                catalog,
                wal_appender: self.wal_appender.as_ref(),
                raft_shared: raft_shared.as_deref(),
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
    raft_shared: Option<&'a SharedState>,
}

impl SurrogateHwmPersist for CombinedPersist<'_> {
    fn checkpoint(&self, hwm: u32) -> crate::Result<()> {
        self.catalog.put_surrogate_hwm(hwm)?;
        self.wal_appender.record_alloc_to_wal(hwm)?;
        // Propose to Raft when in cluster mode so followers advance
        // their in-memory HWM. Failure is non-fatal for the local
        // write (which is already durable via the catalog and WAL);
        // the follower will catch up on the next flush cycle or via
        // snapshot. We log at warn so operators can detect systemic
        // issues without breaking the local write path.
        if let Some(shared) = self.raft_shared
            && let Err(e) = crate::control::metadata_proposer::propose_surrogate_hwm(shared, hwm)
        {
            tracing::warn!(hwm, error = %e, "surrogate hwm raft propose failed; followers may lag");
        }
        Ok(())
    }

    fn load(&self) -> crate::Result<u32> {
        self.catalog.get_surrogate_hwm()
    }
}
