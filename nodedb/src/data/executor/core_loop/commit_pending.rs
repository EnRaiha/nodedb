// SPDX-License-Identifier: BUSL-1.1

//! Staged Calvin write plans awaiting the local commit verdict.

use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;

/// A Calvin transaction's write plans staged for commit, held between the
/// validate-and-stage step and the verdict-driven flush-or-drop.
///
/// `CalvinExecuteStatic` inserts this into the core's commit-pending buffer
/// WITHOUT mutating base or firing side effects, and returns the local commit
/// verdict to the scheduler. A verdict-driven `CalvinFlush` replays `plans`
/// through the durable apply funnel (setting the epoch time anchor and
/// leadership scope captured here); a `CalvinDrop` discards it. Nothing here is
/// observable in the base engines until a flush.
pub(in crate::data::executor) struct PendingCommit {
    /// Physical write plans replayed through `execute_transaction_batch` on flush.
    pub plans: Vec<PhysicalPlan>,
    /// Tenant scope for `plans`, applied when the flush replays them.
    pub tenant_id: TenantId,
    /// Deterministic epoch timestamp anchor, restored on flush so time-dependent
    /// writes (bitemporal sys_from, KV TTL, timeseries system_ms) stay identical
    /// across replicas.
    pub epoch_system_ms: i64,
    /// Whether this node led the data-group at stage time; restored on flush so
    /// the leader-only OLLP verification runs on the same participant.
    pub is_group_leader: bool,
}
