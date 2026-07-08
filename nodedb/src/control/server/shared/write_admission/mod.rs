// SPDX-License-Identifier: BUSL-1.1

//! Neutral Control-Plane write-admission gate: the single seam every
//! write-class `PhysicalPlan` passes through before it is enqueued to a
//! Data-Plane core.
pub mod gate;
pub mod predicate;

pub use gate::{WriteTarget, admit};
pub use predicate::plan_is_write;
