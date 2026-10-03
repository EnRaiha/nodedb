// SPDX-License-Identifier: BUSL-1.1

//! Per-node lease liveness and revocation state, owned by `SharedState`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use super::holders::LeaseHolders;

/// This node's metadata-group term while it leads the group, else `None`.
pub type LeaderTermFn = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Whether this node heard from the metadata leader within the window. The
/// leader always has.
pub type LeaderContactFn = Arc<dyn Fn(Duration) -> bool + Send + Sync>;

/// Everything the lease module tracks per node beyond the lease cache itself.
#[derive(Default)]
pub struct LeaseRuntime {
    /// SWIM Dead records and the leader's Raft contact samples. Shared with
    /// the SWIM detector and the metadata leader's lease-GC sweep.
    pub holder_liveness: Arc<nodedb_cluster::LeaseHolderLiveness>,
    /// In-flight statement holds per descriptor. A lease this node loses
    /// revokes the statements still running under it.
    pub holders: Arc<LeaseHolders>,
    /// Reads one Raft group, so the drainer can poll it cheaply. `start_raft`
    /// installs it.
    pub metadata_leader_term: OnceLock<LeaderTermFn>,
    /// Contact only: a replica behind on apply still counts as in contact.
    /// `start_raft` installs it.
    pub metadata_contact: OnceLock<LeaderContactFn>,
    /// Releases synchronous code hands off. `start_raft` spawns the task
    /// that proposes them.
    pub releaser: super::releaser::LeaseReleaser,
}

impl LeaseRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}
