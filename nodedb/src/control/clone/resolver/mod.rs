// SPDX-License-Identifier: BUSL-1.1

//! Copy-on-write read resolution algorithm.
//!
//! For reads targeting a `Shadowed` or `Materializing` clone, this module
//! produces an augmented task list: one task for the target database (post-clone
//! writes) and one task for the source database (source rows at
//! `effective_source_lsn`).  Both tasks are dispatched by the caller using the
//! normal SPSC path; the results are then merged via `merge_clone_responses`.
//!
//! Non-cloned databases and `Materialized` clones return the original task list
//! unchanged — zero overhead.

pub mod filter;
pub mod refusal;
pub mod resolve;
pub mod rewrite;

pub use filter::filter_tombstoned_rows;
pub use refusal::SourceRewrite;
pub use resolve::{CloneReadParams, ResolveOutcome, resolve_read};
