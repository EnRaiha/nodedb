// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral INSERT/UPSERT parsing, SQL generation, and dispatch
//! helpers.

mod dispatch;
mod dispatch_write;
mod encoding;
mod statement;
mod types;

pub(in crate::control::server::shared::ddl::neutral::collection) use dispatch::plan_and_dispatch;
pub(in crate::control::server::shared::ddl::neutral::collection) use dispatch_write::{
    authorize_write_target, dispatch_plan,
};
pub(in crate::control::server::shared::ddl::neutral::collection) use encoding::fields_to_insert_sql;
pub(super) use encoding::fields_to_upsert_sql;
pub(super) use statement::parse_write_statement;
pub(super) use types::{ParsedInsert, extract_vector_fields};
