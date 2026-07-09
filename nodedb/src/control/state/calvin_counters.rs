// SPDX-License-Identifier: BUSL-1.1

//! Node-global Calvin observability counters.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Node-global Calvin observability counters, incremented by the per-vShard
/// schedulers (which hold `Arc<SharedState>`) and read by tests and metrics
/// without reaching into the `!Send` Data-Plane state. All start at 0 and stay
/// 0 in single-node / no-Calvin deployments.
pub struct CalvinCounters {
    /// Count of committed Calvin applies whose write versions were recorded
    /// into the per-core write-version index, incremented once per apply for
    /// which a scheduler dispatched a write-version record op.
    pub write_versions_recorded: Arc<AtomicU64>,
    /// Count of committed Calvin applies whose participant reported that its
    /// slice of the transaction's reads was no longer current at apply time.
    /// Observation only — the apply still committed.
    pub read_set_validation_failures: Arc<AtomicU64>,
    /// Count of staged Calvin transactions the scheduler resolved to COMMIT
    /// by dispatching a flush of their commit-pending buffer to base.
    pub commits_flushed: Arc<AtomicU64>,
    /// Count of staged Calvin transactions the scheduler resolved to ABORT
    /// by dispatching a drop of their commit-pending buffer, mirroring
    /// [`CalvinCounters::commits_flushed`].
    pub commits_dropped: Arc<AtomicU64>,
}
