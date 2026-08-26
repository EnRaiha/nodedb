// SPDX-License-Identifier: BUSL-1.1

//! Copy-on-write read resolution algorithm.
//!
//! For a `Shadowed` or `Materializing` clone, produces an augmented task
//! list: one task for the target database (post-clone writes), one for the
//! source (rows at `effective_source_lsn`), merged via
//! `merge_clone_responses`. Other clones return the task list unchanged.

pub mod filter;
pub mod refusal;
pub mod resolve;
pub mod rewrite;

pub use filter::filter_tombstoned_rows;
pub use refusal::SourceRewrite;
pub use resolve::{CloneReadParams, ResolveOutcome, resolve_read};
