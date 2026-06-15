// SPDX-License-Identifier: BUSL-1.1

//! HashJoin bitmap-prefilter injection.
//!
//! When `QueryOp::HashJoin` carries a `left_bitmap` or
//! `right_bitmap` sub-plan, the executor calls `run_bitmap_subplan`
//! to execute that sub-plan and collect the resulting surrogates into a
//! `SurrogateBitmap`. The bitmap is then used to build a prefiltered
//! `DocumentOp::Scan` for the probe side, replacing the generic
//! `scan_collection` call so non-member rows never enter the hash-join loop.
//!
//! If the sub-plan returns no decodable rows (e.g. the collection doesn't
//! exist or the index lookup yields nothing), `run_bitmap_subplan` returns an
//! empty bitmap — the probe scan proceeds without any prefilter in that case.

use nodedb_types::SurrogateBitmap;

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::DocumentOp;

use super::materialize::collect_surrogates;

/// Execute a bitmap-producer sub-plan and return the resulting `SurrogateBitmap`.
///
/// Returns an empty bitmap when the sub-plan produces no decodable rows —
/// the caller should treat an empty bitmap as "no prefilter" and fall back
/// to a full probe scan.
pub(crate) fn run_bitmap_subplan(
    core: &mut CoreLoop,
    task: &ExecutionTask,
    sub_plan: &PhysicalPlan,
) -> SurrogateBitmap {
    let sub_response = core.execute_plan(task, sub_plan);
    let docs = crate::data::executor::response_codec::decode_response_to_docs(&sub_response)
        .unwrap_or_default();
    collect_surrogates(&docs)
}

/// Build a `DocumentOp::Scan` physical plan with a surrogate prefilter injected.
///
/// Used by the hash-join executor to replace the plain `scan_collection` call
/// for the probe side when a bitmap sub-plan was provided. The scan will skip
/// rows whose surrogate is absent from `bitmap`, pushing the filter into the
/// document engine before any msgpack decode.
///
/// If `bitmap` is empty, returns `None` — the caller falls back to the normal
/// `scan_collection` path (no-op: an empty bitmap would block all rows).
pub(crate) fn prefiltered_scan_plan(
    collection: &str,
    limit: usize,
    bitmap: SurrogateBitmap,
) -> Option<PhysicalPlan> {
    if bitmap.is_empty() {
        return None;
    }
    Some(PhysicalPlan::Document(DocumentOp::Scan {
        collection: collection.to_string(),
        limit,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: Some(bitmap),
    }))
}
