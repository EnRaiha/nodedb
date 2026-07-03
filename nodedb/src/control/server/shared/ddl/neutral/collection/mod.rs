// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral collection DDL family: DESCRIBE COLLECTION / SHOW
//! COLLECTIONS / SHOW INDEXES / UNDROP COLLECTION / CREATE COLLECTION /
//! CREATE TABLE / DROP COLLECTION / CREATE INDEX / DROP INDEX, plus the
//! collection purge helpers.

pub mod alter;
pub mod create;
pub mod describe;
pub mod drop;
pub mod enforcement;
pub mod index;
pub(super) mod index_fanout;
pub mod purge;
pub mod register;
pub mod show_indexes;
pub mod undrop;
pub mod vector_metadata;

pub use alter::dispatch_alter_collection;
pub use create::{CreateCollectionRequest, create_collection, create_table};
pub use describe::{describe_collection, show_collections};
pub use drop::drop_collection;
pub use index::{CreateIndexRequest, create_index, drop_index};
pub use register::{dispatch_register_by_name, dispatch_register_from_stored};
pub use show_indexes::show_indexes;
pub use undrop::undrop_collection;
pub use vector_metadata::{
    handle_set_vector_metadata, handle_show_vector_models, handle_vector_metadata_query,
};
