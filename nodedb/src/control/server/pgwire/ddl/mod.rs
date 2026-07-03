// SPDX-License-Identifier: BUSL-1.1

pub mod catalog_propose;
pub mod collection;
pub mod convert;
pub mod database;
pub mod field_def;
pub mod impersonation_ddl;
pub mod ownership;
pub(crate) mod parse_utils;
pub mod pubsub;
#[path = "router/mod.rs"]
pub mod router;
pub mod schema_validation;
pub mod session_ddl;
pub(crate) mod sql_parse;
pub mod stream_select;
pub mod streaming_mv;
pub mod sync_dispatch;
pub mod temp_table;
pub mod tenant;

pub use router::dispatch;
