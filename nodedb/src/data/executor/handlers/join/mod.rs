// SPDX-License-Identifier: BUSL-1.1

//! Join execution handlers — hash, sort-merge, broadcast, nested-loop, and lateral.

mod budget_guard;
mod grace_drive;
pub(super) mod grace_partitioner;
mod grace_repartition;
mod grace_spill;
pub mod hash;
mod hash_handlers;
pub mod lateral;
pub mod nested_loop;
pub mod params;
mod row_source;
pub mod sort_merge;
mod spill;
mod support;

pub(crate) use params::{HashJoinParams, JoinParams, NestedLoopJoinParams, SortMergeJoinParams};

// `merge_join_docs_binary` is exercised directly by an integration test, so it
// stays crate-public; the rest are join-internal helpers (private re-export,
// visible to the join submodules that consume them via `super::`).
pub use support::merge_join_docs_binary;
use support::{binary_row_matches_filters, binary_row_project, compare_preextracted};
