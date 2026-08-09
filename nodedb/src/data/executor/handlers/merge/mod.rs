// SPDX-License-Identifier: BUSL-1.1

//! Handler for `DocumentOp::Merge`: implements the MERGE statement execution.
//!
//! Execution model (mirroring SQL MERGE semantics):
//!
//! Phase 1: Build a join map from the source collection:
//!   source_join_value → source_document
//!
//! Phase 2: Walk all target rows.  For each target row:
//!   - If the source map has a matching entry, evaluate WHEN MATCHED arms in
//!     order; apply the first arm whose extra_predicate is satisfied.
//!   - If no source row matches, evaluate WHEN NOT MATCHED BY SOURCE arms.
//!
//! Phase 3: Walk source rows that had no target match.  Evaluate WHEN NOT
//!   MATCHED arms in order; apply the first whose extra_predicate is satisfied.

pub(in crate::data::executor) mod dispatch;
pub(in crate::data::executor) mod source_map;
pub(in crate::data::executor) mod target_docs;

pub(in crate::data::executor) use dispatch::MergeParams;
