// SPDX-License-Identifier: BUSL-1.1

//! The neutral write-admission gate.
//!
//! Every write-class `PhysicalPlan` — regardless of transport or path —
//! passes through [`admit`] before it is enqueued to a Data-Plane core. The
//! gate is the SINGLE place a write is admitted: it stamps
//! [`Admission::Admitted`] on writes and [`Admission::Exempt`]`(Read)` on
//! non-writes.
//!
//! Lock acquisition against the per-vShard deterministic lock manager (holding
//! the write's read/write-slice locks from the Control Plane through apply, so
//! the fast path serializes behind a pending cross-shard commit's fence) is
//! wired in a follow-up change. Today the lock is an always-ready no-op, so
//! this seam changes NO behavior — it only funnels writes through one place
//! and marks them, making the "no write reaches a core without passing the
//! seam" property enforceable before locking semantics land.

use crate::bridge::envelope::{Admission, ExemptReason, PhysicalPlan};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::predicate::plan_is_write;

/// The shard slice a write targets. Carries what the per-vShard deterministic
/// lock manager will need to acquire the write's locks in a follow-up change;
/// today only the plan's write-class is consulted.
pub struct WriteTarget<'a> {
    /// Tenant scope of the write.
    pub tenant_id: TenantId,
    /// Database (catalog namespace) scope of the write.
    pub database_id: DatabaseId,
    /// Target virtual shard whose lock manager will gate the write.
    pub vshard_id: VShardId,
    /// The plan being admitted — its write-class decides the marker and (later)
    /// the lock key set.
    pub plan: &'a PhysicalPlan,
}

/// Admit a plan destined for a Data-Plane core.
///
/// Returns [`Admission::Admitted`] for write-class plans (the single admission
/// point for the shard) and [`Admission::Exempt`]`(`[`ExemptReason::Read`]`)`
/// for non-writes. In this change the gate acquires NO lock — see the module
/// docs; the marker is the only effect.
pub fn admit(shared: &SharedState, target: &WriteTarget<'_>) -> Admission {
    if plan_is_write(target.plan) {
        acquire_write_locks_noop(shared, target);
        Admission::Admitted
    } else {
        // A non-write reaching the gate is a read / meta op — it never needs
        // the write fence.
        Admission::Exempt(ExemptReason::Read)
    }
}

/// Per-vShard deterministic lock acquisition for an admitted write.
///
/// Always-ready no-op placeholder: real lock acquisition (the per-vShard
/// deterministic lock managers the Calvin scheduler already owns, held from
/// here through Data-Plane apply) is wired in a follow-up change. Kept as a
/// named seam so that change attaches the lock here without touching any call
/// site.
fn acquire_write_locks_noop(_shared: &SharedState, _target: &WriteTarget<'_>) {
    // lock acquisition is wired in a follow-up change
}
