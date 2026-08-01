// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral collection DML: INSERT INTO / UPSERT INTO.

mod insert;
mod parse;
mod triggers;
mod upsert;

pub use insert::insert_document;
pub use upsert::upsert_document;

// Shared with the `copy_from` bulk-import handlers (same collection-DML
// family): building INSERT SQL from a field map and planning/dispatching it.
pub(in crate::control::server::shared::ddl::neutral::collection) use parse::{
    authorize_write_target, fields_to_insert_sql, plan_and_dispatch,
};
