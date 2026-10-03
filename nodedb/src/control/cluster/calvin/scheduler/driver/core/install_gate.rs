// SPDX-License-Identifier: BUSL-1.1

//! The scheduler's admission of a flush against its data group's snapshots.
//!
//! A flush installs a committed transaction's slice on the vShard's core.
//! The data group's apply gate orders it against the group's snapshots:
//!
//! - A snapshot capture on this node holds the gate exclusive. A flush holds
//!   it shared from its dispatch until its position is marked applied, so
//!   the capture reads storage and applied positions that agree.
//! - A snapshot install on this node holds it exclusive too, and replaces
//!   the vShard's base. A scheduler started under an older base installs
//!   nothing more: the next scheduler reconcile starts it again from the
//!   installed state.
//!
//! A flush the gate refuses waits for the gate's release and is tried again.

use tokio::sync::watch;

use super::scheduler::Scheduler;
use crate::control::security::auth_fence::cluster::group_of_vshard;
use crate::control::state::SharedState;

/// What the gate says to one flush.
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum FlushAdmission {
    /// Dispatch, holding the permit until the position is marked applied.
    /// `None` when no apply gate is wired here.
    Go(Option<nodedb_cluster::ApplyPermit>),
    /// A snapshot holds the gate: try again once it releases.
    Wait,
    /// A snapshot install replaced this vShard's base since this scheduler
    /// started: install nothing.
    Retired,
}

/// The scheduler's view of its data group's apply gate.
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct InstallGate {
    /// The vShard's base generation this scheduler started under.
    generation: u64,
    /// Changes each time a snapshot releases a group's gate.
    released: Option<watch::Receiver<u64>>,
    /// Whether a flush waits for a release.
    waiting: bool,
}

impl InstallGate {
    /// The gate of a scheduler starting for `vshard_id` now.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn new(
        shared: &SharedState,
        vshard_id: u32,
    ) -> Self {
        Self {
            generation: shared.calvin.bases.generation(vshard_id),
            released: shared
                .raft_apply_gates
                .get()
                .map(|gates| gates.subscribe_released()),
            waiting: false,
        }
    }

    /// Admit one flush of `vshard_id`.
    fn admit(&mut self, shared: &SharedState, vshard_id: u32) -> FlushAdmission {
        if shared.calvin.bases.generation(vshard_id) != self.generation {
            return FlushAdmission::Retired;
        }
        let Some(gates) = shared.raft_apply_gates.get() else {
            return FlushAdmission::Go(None);
        };
        let Ok(group_id) = group_of_vshard(shared, vshard_id) else {
            // No routing table holds the vShard: no snapshot of it runs.
            return FlushAdmission::Go(None);
        };
        // Marked as seen before the try, so a release after it still wakes
        // the wait.
        if let Some(released) = self.released.as_mut() {
            drop(released.borrow_and_update());
        }
        match gates.try_apply(group_id) {
            Some(permit) => {
                self.waiting = false;
                FlushAdmission::Go(Some(permit))
            }
            None => {
                self.waiting = true;
                FlushAdmission::Wait
            }
        }
    }

    /// Whether a flush waits for the gate's release.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_waiting(&self) -> bool {
        self.waiting
    }

    /// Resolves once a snapshot released a group's gate since the last
    /// admission.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn released(&mut self) {
        match self.released.as_mut() {
            Some(released) => {
                if released.changed().await.is_err() {
                    // The gates are gone: no snapshot holds one again.
                    self.waiting = false;
                }
            }
            None => std::future::pending().await,
        }
    }
}

impl Scheduler {
    /// Admit the flush of the lowest unfinished transaction.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn admit_flush(
        &mut self,
    ) -> FlushAdmission {
        let vshard_id = self.vshard_id;
        self.install_gate.admit(&self.shared, vshard_id)
    }

    /// Record that this replica's Calvin state of the vShard has a hole, and
    /// make its data-group replica refuse log entries until a snapshot
    /// brings the state back.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn lose_calvin_base(&self) {
        if let Err(error) = self
            .shared
            .calvin
            .bases
            .record_lost(self.shared.credentials.catalog(), self.vshard_id)
        {
            tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: the vShard's lost base did not persist"
            );
        }
        match group_of_vshard(&self.shared, self.vshard_id) {
            Ok(group_id) => {
                self.multi_raft
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .set_snapshot_required(group_id, true);
            }
            Err(error) => tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: no data group for the vShard; its replica cannot ask for a snapshot"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use nodedb_cluster::{GroupApplyGates, RoutingTable};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A snapshot capture holds the group's gate exclusive. A flush that
    /// starts meanwhile waits, and a capture that starts while a flush is
    /// out waits until the flush marked its position and dropped its
    /// permit. So every capture sees each vShard's flushes either whole or
    /// not at all, on every core. A snapshot install that replaced the
    /// vShard's base stops the scheduler started before it.
    #[tokio::test]
    async fn a_capture_never_sees_a_flush_half_applied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("gate.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        Arc::get_mut(&mut state)
            .expect("sole owner of fresh state")
            .cluster_routing = Some(Arc::new(RwLock::new(RoutingTable::uniform(2, &[1], 1))));
        let gates = Arc::new(GroupApplyGates::new());
        let vshard_id = 0;
        let group_id = group_of_vshard(&state, vshard_id).expect("the vShard has a group");
        gates.mount(group_id);
        assert!(state.raft_apply_gates.set(Arc::clone(&gates)).is_ok());

        let mut gate = InstallGate::new(&state, vshard_id);
        let capture = gates.install(group_id).await;
        assert!(matches!(
            gate.admit(&state, vshard_id),
            FlushAdmission::Wait
        ));
        assert!(gate.is_waiting());
        drop(capture);
        tokio::time::timeout(Duration::from_secs(5), gate.released())
            .await
            .expect("the capture's release wakes the waiting flush");

        let FlushAdmission::Go(Some(permit)) = gate.admit(&state, vshard_id) else {
            panic!("an open gate admits the flush with a permit");
        };
        assert!(!gate.is_waiting());
        let next_capture = {
            let gates = Arc::clone(&gates);
            tokio::spawn(async move { drop(gates.install(group_id).await) })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !next_capture.is_finished(),
            "a capture waits for the flush in flight"
        );
        drop(permit);
        tokio::time::timeout(Duration::from_secs(5), next_capture)
            .await
            .expect("the capture starts once the flush finished")
            .expect("capture task");

        state
            .calvin
            .bases
            .record_snapshot(state.credentials.catalog(), &[vshard_id], 9)
            .expect("record the installed base");
        assert!(matches!(
            gate.admit(&state, vshard_id),
            FlushAdmission::Retired
        ));
    }
}
