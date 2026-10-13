// SPDX-License-Identifier: Apache-2.0

//! JOIN planning: extract left/right tables, equi-join keys, join type.

pub mod array_arm;
pub mod constraint;
mod derived_side;
pub mod plan;

pub use plan::plan_join_from_select;
