// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral collection DDL family: DESCRIBE COLLECTION / SHOW
//! COLLECTIONS / SHOW INDEXES / UNDROP COLLECTION / CREATE COLLECTION /
//! CREATE TABLE.
//!
//! `drop`, `index` (CREATE/DROP INDEX), and `purge` remain on the
//! transitional pgwire path (`pgwire::ddl::collection::{drop, index, purge}`)
//! pending a later migration unit.

pub mod create;
pub mod describe;
pub mod enforcement;
pub mod register;
pub mod show_indexes;
pub mod undrop;

pub use create::{CreateCollectionRequest, create_collection, create_table};
pub use describe::{describe_collection, show_collections};
pub use register::{dispatch_register_by_name, dispatch_register_from_stored};
pub use show_indexes::show_indexes;
pub use undrop::undrop_collection;
