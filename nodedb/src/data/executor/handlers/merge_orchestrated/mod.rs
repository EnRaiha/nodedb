// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane-orchestrated MERGE passes: RESOLVE (read) and APPLY (atomic).
//!
//! Autocommit `MERGE` is driven from the Control Plane
//! (`control::merge_orchestrator`) so every NOT-MATCHED insert row receives its
//! OWN globally-unique, catalog-registered surrogate — surrogate registration
//! is Control-Plane-only, and the Data Plane never touches the catalog. The
//! orchestrator round-trips through the two passes here:
//!
//! 1. [`crate::data::executor::core_loop::CoreLoop::execute_merge_resolve`] —
//!    classify the merge WITHOUT writing and return the NOT-MATCHED insert
//!    rows as msgpack `Vec<(join_key, body)>`. The orchestrator allocates a
//!    fresh surrogate per row.
//! 2. [`crate::data::executor::core_loop::CoreLoop::execute_merge_apply`] —
//!    re-derive the classification, VERIFY the recomputed insert-key set
//!    still equals the orchestrator's predicted set (returning
//!    [`crate::bridge::envelope::ErrorCode::OllpRetryRequired`] WITHOUT
//!    writing on drift, closing the resolve→apply TOCTOU), then apply every
//!    arm's writes with the pre-assigned surrogates. The matched UPDATE and
//!    NOT-MATCHED INSERT arms share ONE redb transaction so a UNIQUE
//!    violation on any insert rolls back the whole set (all-or-nothing). Both
//!    insert and update land through
//!    [`crate::data::executor::core_loop::CoreLoop::apply_point_put`], which
//!    maintains the document store, FTS, HNSW vector, spatial, and secondary
//!    indexes keyed by the row's surrogate — so a merge-inserted row is
//!    immediately resolvable from any cross-engine search. DELETE arms hit
//!    existing registered rows via
//!    [`crate::data::executor::core_loop::CoreLoop::apply_point_delete`],
//!    each in its own transaction, after the put commit.

mod abort;
mod apply;
mod apply_support;
mod delete_arms;
pub(super) mod plan;
mod resolve;
