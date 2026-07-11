// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane write-admission gate: the single seam every write-class
//! `PhysicalPlan` passes through before it is enqueued to a Data-Plane core.
//! Uncontended point writes take the fast path holding per-vShard deterministic
//! locks; contended and bulk writes route to the Calvin scheduler.
pub mod gate;
pub mod lock_keys;
pub mod predicate;
pub mod route;
pub mod write_order_lock;

pub use gate::{WriteAdmission, WriteAdmissionGuard, WriteTarget, admit, cp_routed_to_calvin};
pub use predicate::{plan_is_write, plan_requires_txn_buffering};
pub use route::{bare_ok_response, route_write_to_calvin};
pub use write_order_lock::KeyedWriteOrderLock;
