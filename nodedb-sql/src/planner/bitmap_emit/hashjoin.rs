// SPDX-License-Identifier: Apache-2.0

//! HashJoin bitmap-emission analysis.
//!
//! Given the two children of a `SqlPlan::Join`, this module determines which
//! side(s) qualify for bitmap-producer pushdown and returns a `BitmapJoinHints`
//! struct. The converter layer (`nodedb`'s `sql_plan_convert::scan`) uses these
//! hints to populate `QueryOp::HashJoin::left_bitmap` and
//! `right_bitmap` with the appropriate `PhysicalPlan` sub-plans.
//!
//! Selection policy (no cost model; no statistics required):
//! - Left child qualifies → emit `left_bitmap` hint.
//! - Right child qualifies → emit `right_bitmap` hint.
//! - Both qualify → emit the side that is already a `DocumentIndexLookup`
//!   (already indexed), preferring left if both are the same shape.
//! - Neither qualifies → both hints are `None`.

use crate::types::SqlPlan;

use super::predicate::{self, BitmapHint};

/// Bitmap-pushdown hints for both sides of a join.
#[derive(Debug, Default)]
pub struct BitmapJoinHints {
    /// Hint for the left join child. When `Some`, the converter should build an
    /// `IndexedFetch` (or `Scan`) sub-plan and place it in `left_bitmap`.
    pub left: Option<BitmapHint>,
    /// Hint for the right join child. Same semantics as `left`.
    pub right: Option<BitmapHint>,
}

/// Analyze both join children and return bitmap-pushdown hints.
///
/// Neither side is penalized for being analyzed — if both qualify, both hints
/// are emitted so the executor can inject prefilters on both probe scans.
/// This is safe: the executor will only use the bitmap that matches the
/// collection it is about to scan.
pub fn analyze_join_sides(left: &SqlPlan, right: &SqlPlan) -> BitmapJoinHints {
    BitmapJoinHints {
        left: predicate::analyze(left),
        right: predicate::analyze(right),
    }
}
