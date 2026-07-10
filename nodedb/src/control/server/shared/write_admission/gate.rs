// SPDX-License-Identifier: BUSL-1.1

//! The write-admission gate.
//!
//! Every write-class `PhysicalPlan` — regardless of transport or path — passes
//! through [`admit`] before it is enqueued to a Data-Plane core. The gate
//! decides one of three outcomes per write:
//!
//! - [`WriteAdmission::FastPath`] — an uncontended POINT write whose exact
//!   deterministic lock keys were acquired here. It carries a RAII
//!   [`WriteAdmissionGuard`] the caller holds across the enqueue + response; the
//!   guard releases the keys on drop. This is the normal autocommit path.
//! - [`WriteAdmission::RouteToCalvin`] — a point write whose keys are currently
//!   held by a pending commit (acquire returned `Blocked`), OR any predicate /
//!   bulk / multi-home write. The caller submits it through the deterministic
//!   scheduler, which queues it FIFO behind the holder and applies it in order.
//! - [`WriteAdmission::ExemptRead`] — a non-write (read / meta op), or a
//!   Calvin-scheduled apply that already holds its locks.
//!
//! The fence holds because the fast path and the scheduler share the SAME
//! `Arc<Mutex<LockManager>>` (via [`SharedState::calvin_lock_managers`]): a
//! commit's lock validation calls `acquire` on the same key, is `Blocked`, and
//! waits; whoever takes the OS mutex first wins, with no time-of-check /
//! time-of-use gap.
//!
//! [`SharedState::calvin_lock_managers`]: crate::control::state::SharedState::calvin_lock_managers

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{PlanRouting, plan_vshard};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::planner::calvin::is_dependent_predicate;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

use super::lock_keys::plan_lock_keys;
use super::predicate::plan_is_write;

/// Count of writes the gate routed to the deterministic scheduler instead of
/// the fast path (either a `Blocked` point write or a non-point write). Read by
/// the fence tests.
static ROUTED_TO_CALVIN: AtomicU64 = AtomicU64::new(0);

/// Number of writes routed to the deterministic Calvin scheduler by the gate.
pub fn cp_routed_to_calvin() -> u64 {
    ROUTED_TO_CALVIN.load(Ordering::Relaxed)
}

/// The shard slice a write targets, plus the plan whose write-class and point
/// identity the gate consults.
pub struct WriteTarget<'a> {
    /// Tenant scope of the write.
    pub tenant_id: TenantId,
    /// Database (catalog namespace) scope of the write.
    pub database_id: DatabaseId,
    /// Target virtual shard whose lock manager gates the write.
    pub vshard_id: VShardId,
    /// The plan being admitted.
    pub plan: &'a PhysicalPlan,
}

/// The gate's decision for one write. See the module docs.
pub enum WriteAdmission {
    /// Uncontended point write admitted to the fast path. `guard` is `Some` when
    /// real keys were acquired (a scheduler is active for the vShard) and `None`
    /// when no lock manager is registered (single-node / no-Calvin — nothing to
    /// fence against). Either way the caller holds it across enqueue + response.
    FastPath { guard: Option<WriteAdmissionGuard> },
    /// Submit the write through the deterministic Calvin scheduler.
    RouteToCalvin,
    /// A non-write, or an already-locked Calvin apply — no fence needed.
    ExemptRead,
}

/// RAII holder of a fast-path write's deterministic locks.
///
/// Holds the shared lock table and the reserved autocommit holder id. `Drop`
/// releases every key held by that holder under a short guard (the lock table
/// tracks the key set by holder, so the guard needs only the id). No waiter
/// notification is needed: the fast path only ever holds UNCONTENDED keys, so no
/// Calvin waiter is ever queued behind it — the scheduler wakes its own waiters.
pub struct WriteAdmissionGuard {
    lock_manager: Arc<Mutex<LockManager>>,
    txn: TxnId,
}

impl Drop for WriteAdmissionGuard {
    fn drop(&mut self) {
        // Release under a short guard. `release` returns any promoted waiters;
        // the fast path holds only uncontended keys so this set is always empty,
        // and there is nothing for the Control Plane to dispatch.
        let _ = self
            .lock_manager
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release(self.txn);
    }
}

/// Admit a plan destined for a Data-Plane core.
///
/// Synchronous: never awaits and never parks. `RouteToCalvin` is returned ONLY
/// when a deterministic scheduler is actually registered for the write's vShard;
/// with no scheduler (single-node / no-Calvin — the common case) every write
/// fast-paths exactly as it did before the fence existed.
pub fn admit(shared: &SharedState, target: &WriteTarget<'_>) -> WriteAdmission {
    // A Calvin-scheduled apply already holds its locks (acquired by the
    // scheduler); it must never re-acquire at the gate. Defensive — these ops
    // do not normally reach the gate.
    if matches!(
        target.plan,
        PhysicalPlan::Meta(
            MetaOp::CalvinExecuteStatic { .. }
                | MetaOp::CalvinExecuteActive { .. }
                | MetaOp::CalvinFlush { .. }
                | MetaOp::CalvinDrop { .. }
        )
    ) {
        return WriteAdmission::ExemptRead;
    }

    if !plan_is_write(target.plan) {
        return WriteAdmission::ExemptRead;
    }

    // Only two write shapes participate in the fence: a single-home POINT write
    // (Document / KV / Vector / single-home graph edge — a statically-known
    // deterministic key) and a single-shard PREDICATE write (BulkUpdate /
    // BulkDelete — its write set discovered by scheduler reconnaissance). Every
    // other write — batch, INSERT..SELECT, upsert, CRDT, columnar / timeseries /
    // spatial / array, and cross-home edges — has no Calvin lock representation
    // and fast-paths unchanged.
    let point_keys = plan_lock_keys(target.plan);
    let is_predicate = is_dependent_predicate(target.plan);
    let vshard = match &point_keys {
        Some((v, _)) => *v,
        None if is_predicate => match plan_vshard(target.plan) {
            PlanRouting::Vshards(v) => match v.as_slice() {
                [v] => *v,
                _ => return WriteAdmission::FastPath { guard: None },
            },
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                return WriteAdmission::FastPath { guard: None };
            }
        },
        None => return WriteAdmission::FastPath { guard: None },
    };

    // Availability gate: with no scheduler registered for this vShard there is no
    // fence to serialize against — admit on the fast path with no lock held.
    // This makes the no-Calvin path byte-for-byte identical to pre-fence
    // behavior for point AND predicate writes.
    let Some(lock_manager) = shared
        .calvin_lock_managers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&vshard.as_u32())
        .map(Arc::clone)
    else {
        return WriteAdmission::FastPath { guard: None };
    };

    // Calvin IS running for this vShard. A predicate write has no static point
    // key to acquire; the scheduler discovers its write set, so route it.
    let Some((_v, keys)) = point_keys else {
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
        return WriteAdmission::RouteToCalvin;
    };

    // Point write: mint a holder id in the reserved band so it never collides
    // with a real Calvin schedule position, then probe the exact keys WITHOUT
    // blocking. `try_acquire` never enqueues a waiter on the contended path, so a
    // routed write leaves no orphaned autocommit holder that a later `release`
    // could promote to an unowned (never-released) lock.
    let txn = TxnId::new(
        TxnId::AUTOCOMMIT_EPOCH,
        shared.autocommit_lock_seq.fetch_add(1, Ordering::Relaxed),
    );
    let acquired = {
        let mut lm = lock_manager.lock().unwrap_or_else(|p| p.into_inner());
        lm.try_acquire(txn, keys)
    };
    if acquired {
        WriteAdmission::FastPath {
            guard: Some(WriteAdmissionGuard { lock_manager, txn }),
        }
    } else {
        // A pending commit (or another fast-path write) holds a key: route behind
        // it via the scheduler. Nothing was acquired or enqueued here.
        ROUTED_TO_CALVIN.fetch_add(1, Ordering::Relaxed);
        WriteAdmission::RouteToCalvin
    }
}
