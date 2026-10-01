// SPDX-License-Identifier: BUSL-1.1

//! Per-group gate between applying committed entries and installing a
//! snapshot.
//!
//! An install restores the state machine to a snapshot, then adopts the
//! snapshot as the group's Raft boundary. An entry the snapshot covers must
//! never reach the state machine after the restore starts. Appliers hold the
//! gate shared from their floor check until the entry is handed to the state
//! machine. An install holds it exclusive from its need check through the
//! boundary advance. Each check therefore sees either no install started, or
//! the adopted snapshot index.
//!
//! The gate is a `tokio::sync::RwLock` for two reasons:
//! - The install holds it across the Data-Plane restore await. That await is
//!   local (every core's SPSC queue), never a network round-trip.
//! - The host apply loop holds it across a write's enqueue await.
//!
//! Appliers share the gate, so the tick and the host apply loop never block
//! each other. The sync tick never waits: it takes the gate with `try_read`
//! and requeues a batch the gate refuses. A refused applier retries when the
//! release generation changes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, watch};

/// One gate per Raft group. The protected value is the highest snapshot index
/// an install adopted through the gate.
///
/// A group gets its gate entry when it mounts. The entry stays when the group
/// unmounts, so a later mount keeps its installed index. The entries stay
/// bounded by the groups of the cluster.
///
/// The `gates` mutex is a LEAF lock: no other lock is taken while it is held.
#[derive(Debug)]
pub struct GroupApplyGates {
    gates: Mutex<HashMap<u64, Arc<RwLock<u64>>>>,
    released: watch::Sender<u64>,
}

impl Default for GroupApplyGates {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupApplyGates {
    pub fn new() -> Self {
        let (released, _) = watch::channel(0);
        Self {
            gates: Mutex::new(HashMap::new()),
            released,
        }
    }

    /// Create the gate of `group_id` when the group mounts. A gate that
    /// exists is kept, with its installed index.
    pub fn mount(&self, group_id: u64) {
        self.gates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(group_id)
            .or_insert_with(|| Arc::new(RwLock::new(0)));
    }

    /// The gate of `group_id`. A group this node does not mount has no gate
    /// entry: it gets an open gate of its own, and no entry is created.
    fn gate(&self, group_id: u64) -> Arc<RwLock<u64>> {
        let gates = self.gates.lock().unwrap_or_else(|p| p.into_inner());
        match gates.get(&group_id) {
            Some(gate) => Arc::clone(gate),
            None => Arc::new(RwLock::new(0)),
        }
    }

    /// Number of gate entries.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.gates.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Admit an apply of `group_id`. `None` while an install holds or awaits
    /// the gate.
    pub fn try_apply(&self, group_id: u64) -> Option<ApplyPermit> {
        self.gate(group_id)
            .try_read_owned()
            .ok()
            .map(|guard| ApplyPermit { guard })
    }

    /// Wait for exclusive hold of `group_id`'s gate for a snapshot install.
    pub async fn install(self: &Arc<Self>, group_id: u64) -> InstallPermit {
        let guard = self.gate(group_id).write_owned().await;
        InstallPermit {
            guard: Some(guard),
            gates: Arc::clone(self),
        }
    }

    /// A receiver whose value changes each time an install releases a gate.
    pub fn subscribe_released(&self) -> watch::Receiver<u64> {
        self.released.subscribe()
    }
}

/// Shared hold of one group's gate.
#[derive(Debug)]
pub struct ApplyPermit {
    guard: OwnedRwLockReadGuard<u64>,
}

impl ApplyPermit {
    /// Highest snapshot index an install adopted for this group. An entry at
    /// or below it must not apply.
    pub fn installed_through(&self) -> u64 {
        *self.guard
    }
}

/// Exclusive hold of one group's gate for a snapshot install.
#[derive(Debug)]
pub struct InstallPermit {
    guard: Option<OwnedRwLockWriteGuard<u64>>,
    gates: Arc<GroupApplyGates>,
}

impl InstallPermit {
    /// Record that the group adopted a snapshot at `index`.
    pub fn adopted(&mut self, index: u64) {
        if let Some(guard) = self.guard.as_mut() {
            **guard = (**guard).max(index);
        }
    }
}

impl Drop for InstallPermit {
    fn drop(&mut self) {
        // Release before the bump, so a woken applier finds the gate open.
        drop(self.guard.take());
        self.gates
            .released
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn appliers_share_and_an_install_excludes() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        let first = gates.try_apply(1).expect("open gate admits");
        let second = gates.try_apply(1).expect("appliers share the gate");
        drop((first, second));

        let mut install = gates.install(1).await;
        assert!(gates.try_apply(1).is_none(), "install excludes appliers");
        assert!(gates.try_apply(2).is_some(), "other groups stay open");

        let released = gates.subscribe_released();
        install.adopted(9);
        drop(install);
        assert!(released.has_changed().expect("sender alive"));

        let permit = gates.try_apply(1).expect("released gate admits");
        assert_eq!(permit.installed_through(), 9);
    }

    #[tokio::test]
    async fn installed_index_never_moves_back() {
        let gates = Arc::new(GroupApplyGates::new());
        gates.mount(1);
        gates.install(1).await.adopted(9);
        gates.install(1).await.adopted(4);
        let permit = gates.try_apply(1).expect("open gate admits");
        assert_eq!(permit.installed_through(), 9);
    }

    /// Only a mounted group holds a gate entry. Applying or installing for
    /// another group creates none.
    #[tokio::test]
    async fn gate_entries_follow_mounted_groups() {
        let gates = Arc::new(GroupApplyGates::new());
        assert!(gates.try_apply(5).is_some(), "an unmounted group is open");
        drop(gates.install(6).await);
        assert_eq!(gates.len(), 0, "no entry for unmounted groups");

        gates.mount(1);
        gates.install(1).await.adopted(3);
        gates.mount(1);
        assert_eq!(
            gates.try_apply(1).expect("open").installed_through(),
            3,
            "a remount keeps the installed index"
        );
        assert_eq!(gates.len(), 1);
    }
}
