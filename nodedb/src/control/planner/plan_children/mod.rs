// SPDX-License-Identifier: BUSL-1.1

//! The child plans of a `PhysicalPlan`, shared by every whole-plan walk.

mod document;
mod query;
mod vector;
mod walk;

pub use walk::children;
