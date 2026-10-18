// SPDX-License-Identifier: Apache-2.0

//! INSERT, UPSERT, UPDATE, DELETE, and TRUNCATE planning.

mod bulk_load;
mod insert;
mod target;
mod update_delete;
mod update_from;
mod upsert;

pub use bulk_load::plan_bulk_load;
pub use insert::plan_insert;
pub use update_delete::{plan_delete, plan_truncate_stmt, plan_update};
pub use upsert::plan_upsert;
