// SPDX-License-Identifier: BUSL-1.1

//! Collection DDL: ALTER commands, COPY, and DML (insert/upsert). (CREATE
//! COLLECTION / CREATE TABLE, DROP COLLECTION, CREATE / DROP INDEX, DESCRIBE /
//! SHOW COLLECTIONS / SHOW INDEXES / UNDROP COLLECTION, and collection purge
//! are served by the protocol-neutral DDL router.)

pub mod alter;
pub mod check_constraint;
pub mod copy_from;
pub mod copy_to;
pub mod helpers;
pub mod insert;
pub(super) mod insert_parse;
pub mod upsert;
pub mod vector_metadata;

// Re-export all public functions so existing callers via `super::collection::*` continue to work.
pub use alter::{
    alter_collection_alter_column_type, alter_collection_drop_column,
    alter_collection_rename_column, alter_collection_set_append_only,
    alter_collection_set_last_value_cache, alter_collection_set_legal_hold,
    alter_collection_set_retention, alter_table_add_column,
};
pub use copy_from::copy_from_file;
pub use copy_to::copy_to_file;
pub use insert::insert_document;
pub use upsert::upsert_document;
pub use vector_metadata::{
    handle_set_vector_metadata, handle_show_vector_models, handle_vector_metadata_query,
};

// Re-export validate_document_schema from schema_validation (was re-exported from old collection.rs).
pub use super::schema_validation::validate_document_schema;
