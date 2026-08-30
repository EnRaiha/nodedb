// SPDX-License-Identifier: Apache-2.0

//! Planner for `ARRAY_*` table-valued / scalar functions.
//!
//! The functions live in two AST shapes:
//!
//! * **Read** (`ARRAY_SLICE`, `ARRAY_PROJECT`, `ARRAY_AGG`,
//!   `ARRAY_ELEMENTWISE`) — `SELECT * FROM array_xxx(...)`. Parsed
//!   by sqlparser as `TableFactor::Table { name, args: Some(_), .. }`
//!   (Postgres-style table-valued function). [`try_plan_array_table_fn`]
//!   intercepts these before catalog resolution.
//! * **Maintenance** (`ARRAY_FLUSH`, `ARRAY_COMPACT`) — bare
//!   `SELECT array_flush(name)` with no FROM clause.
//!   [`try_plan_array_maint_fn`] intercepts these from the constant-
//!   query path.

mod helpers;
mod maint_fn;
mod table_fn;

pub use maint_fn::try_plan_array_maint_fn;
pub use table_fn::try_plan_array_table_fn;
