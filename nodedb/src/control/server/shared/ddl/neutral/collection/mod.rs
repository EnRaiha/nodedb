// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral collection introspection DDL family: DESCRIBE COLLECTION /
//! SHOW COLLECTIONS / SHOW INDEXES / UNDROP COLLECTION.

pub mod describe;
pub mod show_indexes;
pub mod undrop;

pub use describe::{describe_collection, show_collections};
pub use show_indexes::show_indexes;
pub use undrop::undrop_collection;
