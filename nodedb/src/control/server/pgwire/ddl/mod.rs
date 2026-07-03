// SPDX-License-Identifier: BUSL-1.1

pub mod catalog_propose;
pub mod collection;
pub mod database;
pub mod field_def;
#[path = "router/mod.rs"]
pub mod router;
pub mod schema_validation;
pub(crate) mod sql_parse;
pub mod sync_dispatch;
pub mod temp_table;

pub use router::dispatch;
