// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP/ALTER/DESCRIBE/SHOW for COLLECTION (and TABLE alias).
//!
//! `DROP COLLECTION` extensions (sqlparser 0.61 does not tokenize
//! these, hence custom-handled upper-case keyword scan):
//! - `PURGE` — hard-delete, skipping the retention window
//! - `CASCADE` — recursively drop dependents
//! - `CASCADE FORCE` — cascade through dynamic-SQL schedules
//!
//! `UNDROP COLLECTION <name>` restores a soft-deleted record.

mod alter_ops;
mod body;
mod column_list;
mod dispatcher;
mod engine_suffix;
mod with_clause;

pub(super) use dispatcher::try_parse;
