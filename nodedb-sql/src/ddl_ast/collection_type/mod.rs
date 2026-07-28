// SPDX-License-Identifier: Apache-2.0

//! Engine-name → `CollectionType` mapping for `CREATE COLLECTION` / `CREATE TABLE` DDL.

pub mod build;
pub mod designated;
pub mod kv;
pub mod strict;
pub mod type_str;

pub use build::build_collection_type;
pub use type_str::parse_column_type_str_full;
