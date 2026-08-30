// SPDX-License-Identifier: BUSL-1.1

//! Streaming aggregate accumulators for the generic GROUP BY path.
//!
//! Each `AggAccum` variant holds only the derived state needed to compute the
//! final aggregate result — no raw document bytes are retained.  Memory per
//! group is O(num_aggregates × accumulator_size) regardless of how many
//! documents match that group.
//!
//! Module split:
//! - `state` — `AggAccum` / `GroupState` type definitions + `GroupState` impl.
//! - `new` — `AggAccum::new`.
//! - `feed` — `AggAccum::feed` (folds one document in).
//! - `finalize` — `AggAccum::finalize` (produces the result `Value`).
//! - `merge` — `merge_accum` / `merge_group_state` for spilled-run merge.

mod feed;
mod finalize;
mod merge;
mod new;
mod state;

pub(crate) use state::GroupState;
