// SPDX-License-Identifier: BUSL-1.1

//! Shared parameter structs for join execution handlers.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::JoinProjection;

/// Common join configuration shared across join variants.
pub(crate) struct JoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    pub limit: usize,
    pub projection: &'a [JoinProjection],
    pub post_filter_bytes: &'a [u8],
}

/// Hash join: scans both sides from storage or executes resolved child sub-plans.
///
/// When `left_input` or `right_input` is `Some`, the executor runs that sub-plan
/// (e.g. a `ProviderScan` after coordinator resolution) and uses the resulting
/// rows as the corresponding join side. When `None`, the side is scanned locally
/// by `left_collection` / `right_collection`.
///
/// `left_bitmap` / `right_bitmap`, when `Some`, are executed first to build a
/// surrogate prefilter that is injected into the local scan for the corresponding
/// side, pushing the filter into the document engine before any msgpack decode.
pub(crate) struct HashJoinParams<'a> {
    pub join: JoinParams<'a>,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub left_alias: Option<&'a str>,
    pub right_alias: Option<&'a str>,
    /// Resolved child plan for the left side (e.g. `ProviderScan`). `None` =
    /// scan locally by `left_collection`.
    pub left_input: Option<&'a PhysicalPlan>,
    /// Resolved child plan for the right side. Same semantics as `left_input`.
    pub right_input: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the left side. When `Some`, the executor
    /// runs this sub-plan first, collects surrogates, and injects the bitmap
    /// into the left side's scan prefilter.
    pub left_bitmap: Option<&'a PhysicalPlan>,
    /// Bitmap-producer sub-plan for the right side. Same semantics as
    /// `left_bitmap` but applied to the right collection.
    pub right_bitmap: Option<&'a PhysicalPlan>,
}

/// Nested-loop join: O(N×M) fallback for non-equi, theta, and cross joins.
///
/// `condition` is a msgpack-encoded `Vec<ScanFilter>` (empty = cross join).
pub(crate) struct NestedLoopJoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub condition: &'a [u8],
    pub join_type: &'a str,
    pub limit: usize,
}

/// Sort-merge join: O((N+M)·log N) equi-join with optional pre-sorted inputs.
///
/// `on` is a slice of `(left_key, right_key)` column pairs. `pre_sorted`
/// skips the sort phase when the planner guarantees inputs arrive in key order.
pub(crate) struct SortMergeJoinParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub left_collection: &'a str,
    pub right_collection: &'a str,
    pub on: &'a [(String, String)],
    pub join_type: &'a str,
    pub limit: usize,
    pub pre_sorted: bool,
}

impl JoinParams<'_> {
    /// Apply post-join WHERE filters and projection to result rows.
    ///
    /// Shared tail logic for hash joins and lateral joins:
    /// deserializes post-filter predicates, retains matching rows, then
    /// applies column projection — all on raw msgpack bytes.
    pub fn filter_and_project(&self, results: &mut Vec<Vec<u8>>) {
        if !self.post_filter_bytes.is_empty() {
            let filters: Vec<ScanFilter> =
                zerompk::from_msgpack(self.post_filter_bytes).unwrap_or_default();
            if !filters.is_empty() {
                results.retain(|row| super::binary_row_matches_filters(row, &filters));
            }
        }

        if !self.projection.is_empty() {
            for row in results.iter_mut() {
                *row = super::binary_row_project(row, self.projection);
            }
        }
    }
}
