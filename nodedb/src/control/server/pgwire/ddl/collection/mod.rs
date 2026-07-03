// SPDX-License-Identifier: BUSL-1.1

//! Collection DDL: COPY and DML (insert/upsert). (CREATE COLLECTION / CREATE
//! TABLE, DROP COLLECTION, CREATE / DROP INDEX, DESCRIBE / SHOW COLLECTIONS /
//! SHOW INDEXES / UNDROP COLLECTION, collection purge, every ALTER COLLECTION
//! sub-command, and the vector-model metadata forms are served by the
//! protocol-neutral DDL router.)

pub mod check_constraint;
pub mod copy_from;
pub mod copy_to;
pub mod helpers;
pub mod insert;
pub(super) mod insert_parse;
pub mod upsert;

// Re-export all public functions so existing callers via `super::collection::*` continue to work.
pub use copy_from::copy_from_file;
pub use copy_to::copy_to_file;
pub use insert::insert_document;
pub use upsert::upsert_document;

// Re-export validate_document_schema from schema_validation (was re-exported from old collection.rs).
pub use super::schema_validation::validate_document_schema;
