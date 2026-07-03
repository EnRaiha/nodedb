// SPDX-License-Identifier: BUSL-1.1

//! `CREATE COLLECTION` / `CREATE TABLE` DDL — split by concern.
//!
//! Relocated from `pgwire::ddl::collection::create` (now deleted):
//! - [`build`] — the shared `build_and_persist` body + `Variant`
//! - [`engine_option`] — `WITH (engine='...')` parsing/validation
//! - [`handler`] — the `create_collection` entry point
//! - [`table`] — the `create_table` entry point
//! - [`request`] — `CreateCollectionRequest`

pub mod build;
pub mod engine_option;
pub mod handler;
pub mod request;
pub mod table;

pub use handler::create_collection;
pub use request::CreateCollectionRequest;
pub use table::create_table;
