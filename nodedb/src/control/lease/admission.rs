// SPDX-License-Identifier: BUSL-1.1

//! The refcount units a plan admission reserved and has not yet handed to a
//! [`QueryLeaseScope`].
//!
//! Admission reserves every descriptor under the admission gate, then grants
//! each one. A grant awaits the metadata group, so the admission future can be
//! cancelled at any point in between. [`PendingAdmission`] owns the units
//! meanwhile:
//!
//! - on success, [`PendingAdmission::into_scope`] hands them to the scope;
//! - on error, [`PendingAdmission::rollback`] gives them back and awaits the
//!   release of every descriptor no statement holds any more;
//! - on cancellation, `Drop` gives them back and hands that release to the
//!   background releaser, so nothing blocks.

use nodedb_cluster::DescriptorId;

use super::QueryLeaseScope;
use super::releaser::ReleaseRequest;
use crate::control::state::SharedState;
use crate::error::Error;

/// Refcount units one admission reserved.
pub(crate) struct PendingAdmission<'a> {
    shared: &'a SharedState,
    held: Vec<(DescriptorId, u64)>,
}

impl<'a> PendingAdmission<'a> {
    pub(crate) fn new(shared: &'a SharedState) -> Self {
        Self {
            shared,
            held: Vec::new(),
        }
    }

    /// Reserve one unit of `(id, version)`. The caller holds the admission
    /// gate.
    pub(crate) fn reserve(&mut self, id: &DescriptorId, version: u64) {
        self.shared.lease_refcount.increment(id, version);
        self.held.push((id.clone(), version));
    }

    /// The units reserved so far, in admission order.
    pub(crate) fn held(&self) -> &[(DescriptorId, u64)] {
        &self.held
    }

    /// Hand the units to a new scope. A full holder table rolls them back.
    pub(crate) async fn into_scope(mut self) -> Result<QueryLeaseScope, Error> {
        match QueryLeaseScope::new(self.held.clone(), self.shared) {
            Ok(scope) => {
                self.held.clear();
                Ok(scope)
            }
            Err(error) => {
                self.rollback().await;
                Err(error)
            }
        }
    }

    /// Give every unit back and await the release of each descriptor no
    /// statement holds any more, so a failed first-holder grant leaves no
    /// local lease behind. A failed release is logged: the lease expires.
    pub(crate) async fn rollback(mut self) {
        let unheld = self.give_back();
        if let Err(error) = super::release::release_unheld_leases(self.shared, unheld).await {
            tracing::warn!(%error, "plan lease admission rollback: lease release failed");
        }
    }

    /// Decrement every unit. Returns the descriptors left with no holder.
    fn give_back(&mut self) -> Vec<DescriptorId> {
        let mut unheld = Vec::new();
        let mut drained_hold_ended = false;
        for (id, version) in self.held.drain(..) {
            self.shared.lease_refcount.decrement(&id, version);
            drained_hold_ended |= self.shared.lease_drain.is_draining(&id, version);
            if self.shared.lease_refcount.current(&id) == 0 && !unheld.contains(&id) {
                unheld.push(id);
            }
        }
        // A drain counts this node's holds directly, so it re-counts now.
        if drained_hold_ended {
            self.shared.lease_drain.wake_drain_waiters();
        }
        unheld
    }
}

impl Drop for PendingAdmission<'_> {
    fn drop(&mut self) {
        let unheld = self.give_back();
        if !unheld.is_empty() {
            self.shared
                .lease_runtime
                .releaser
                .submit(ReleaseRequest::UnheldDescriptors(unheld));
        }
    }
}
