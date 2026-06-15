// SPDX-License-Identifier: BUSL-1.1

//! In-process SQL evaluator for virtual catalog tables.
//!
//! Virtual tables (`_system.*`, `pg_catalog.*`) are Control-Plane synthetic
//! relations whose backing data lives entirely in `SharedState`. They never
//! cross the SPSC bridge, so the full planner / Data Plane path is the wrong
//! model for them. Instead, this module:
//!
//! 1. Materializes each referenced virtual table as a typed [`VTable`]
//!    (`value`, `table`) and joins them into a single combined relation.
//! 2. Parses the client SELECT into a [`VSelect`] (`select`), including its
//!    FROM/JOIN tree, casts, catalog functions, and `ANY`/`ALL` predicates.
//! 3. Evaluates WHERE / projection / aggregate / ORDER BY / LIMIT against the
//!    combined row set (`expr`, `exec`).
//! 4. Encodes the result back to pgwire (`encode`).

pub mod encode;
pub mod exec;
pub mod expr;
pub mod select;
pub mod table;
pub mod value;

pub use exec::{ExecError, ResultSet, execute};
pub use expr::{CatalogResolver, EvalCtx};
pub use select::{parse_select, parse_select_with_params};
pub use table::VTable;
