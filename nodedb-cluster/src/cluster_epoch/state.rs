// SPDX-License-Identifier: BUSL-1.1

//! Per-node cluster-epoch state: what this node has applied, and what it has
//! merely overheard.
//!
//! These are two different facts and the distinction is the whole point.
//!
//! * `applied` is authoritative. It advances only when this node applies a
//!   committed epoch bump from the metadata group (or loads one persisted by a
//!   previous run). It is the generation of topology this node is actually
//!   operating on, so it is what every outbound frame stamps.
//! * `observed` is hearsay. It is the highest epoch seen on any inbound frame.
//!   A peer's stamp is evidence about the peer, never about this node.
//!
//! Collapsing the two — advancing one counter on receipt and then stamping it
//! outbound — makes a node claim a generation it has not reached. It would
//! start rejecting peers on the strength of a number it overheard while its own
//! routing table still held an older topology. Reading a number off the wire is
//! not the same as having applied the change that number refers to.
//!
//! `observed > applied` therefore means exactly one thing: this node has missed
//! a topology transition that some peer has already applied. That is a
//! statement about itself, which it can act on, rather than a guess about a
//! peer, which it cannot.

use std::sync::atomic::{AtomicU64, Ordering};

/// One node's view of the cluster epoch.
///
/// Held per node (on the transport's `AuthContext`) rather than per process:
/// several nodes share a process in the test harness and in embedded use, and a
/// process-global counter silently aliases them into one, which would make the
/// fence unobservable exactly where it most needs testing.
#[derive(Debug)]
pub struct ClusterEpochState {
    applied: AtomicU64,
    observed: AtomicU64,
}

impl ClusterEpochState {
    /// State for a node whose last applied epoch is `applied` (0 at genesis,
    /// or the value persisted by a previous run).
    pub fn new(applied: u64) -> Self {
        Self {
            applied: AtomicU64::new(applied),
            observed: AtomicU64::new(applied),
        }
    }

    /// The epoch this node has applied — and the only value it may stamp on an
    /// outbound frame.
    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// The highest epoch seen on an inbound frame.
    pub fn observed(&self) -> u64 {
        self.observed.load(Ordering::Acquire)
    }

    /// Record the epoch carried by an inbound frame.
    ///
    /// Raises `observed` only. Hearing a higher number is evidence that this
    /// node is behind; it never advances what this node claims to have applied.
    pub fn observe(&self, peer_epoch: u64) {
        self.observed.fetch_max(peer_epoch, Ordering::AcqRel);
    }

    /// Advance the applied epoch, on applying a committed bump from the
    /// metadata group or loading the persisted value at boot.
    ///
    /// Monotonic, and lifts `observed` with it so a node that applies ahead of
    /// anything it has heard does not read as behind itself.
    pub fn advance_applied(&self, epoch: u64) {
        self.applied.fetch_max(epoch, Ordering::AcqRel);
        self.observed.fetch_max(epoch, Ordering::AcqRel);
    }

    /// Whether this node has missed a topology transition a peer has applied.
    ///
    /// True while `observed > applied`. Clears on its own once the metadata
    /// group delivers the bump this node had only overheard.
    pub fn is_behind(&self) -> bool {
        self.observed() > self.applied()
    }

    /// How many generations behind this node is, for diagnostics.
    pub fn generations_behind(&self) -> u64 {
        self.observed().saturating_sub(self.applied())
    }
}

impl Default for ClusterEpochState {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_node_stamps_what_it_applied_not_what_it_heard() {
        let state = ClusterEpochState::new(3);
        state.observe(9);
        assert_eq!(
            state.applied(),
            3,
            "overhearing a peer must not promote this node's own generation"
        );
        assert_eq!(state.observed(), 9);
    }

    #[test]
    fn hearing_a_higher_epoch_means_this_node_is_behind() {
        let state = ClusterEpochState::new(3);
        assert!(!state.is_behind());
        state.observe(4);
        assert!(state.is_behind());
        assert_eq!(state.generations_behind(), 1);
    }

    #[test]
    fn applying_the_bump_clears_the_backlog() {
        let state = ClusterEpochState::new(3);
        state.observe(5);
        assert!(state.is_behind());
        state.advance_applied(5);
        assert!(!state.is_behind());
        assert_eq!(state.applied(), 5);
    }

    #[test]
    fn an_older_stamp_moves_nothing() {
        let state = ClusterEpochState::new(7);
        state.observe(2);
        assert_eq!(state.applied(), 7);
        assert_eq!(state.observed(), 7);
        assert!(!state.is_behind());
    }

    #[test]
    fn applied_never_regresses() {
        let state = ClusterEpochState::new(7);
        state.advance_applied(4);
        assert_eq!(state.applied(), 7, "a replayed older bump must not regress");
    }

    #[test]
    fn applying_ahead_of_anything_heard_does_not_read_as_behind() {
        let state = ClusterEpochState::new(0);
        state.advance_applied(6);
        assert!(
            !state.is_behind(),
            "a node that applied a bump first is ahead, not behind"
        );
        assert_eq!(state.observed(), 6);
    }
}
