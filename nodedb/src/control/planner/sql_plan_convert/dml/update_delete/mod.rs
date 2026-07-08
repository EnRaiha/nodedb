// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::Update` / `UpdateFrom` / `Delete` → `PhysicalTask` lowering.

mod delete;
mod shared;
mod update;
mod update_from;

#[cfg(test)]
mod tests;

pub(in crate::control::planner::sql_plan_convert) use delete::convert_delete;
pub(in crate::control::planner::sql_plan_convert) use update::{UpdateParams, convert_update};
pub(in crate::control::planner::sql_plan_convert) use update_from::{
    UpdateFromParams, convert_update_from,
};
