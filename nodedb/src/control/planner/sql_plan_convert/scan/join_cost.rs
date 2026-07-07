// SPDX-License-Identifier: BUSL-1.1

//! Cost-based broadcast-vs-shuffle join selection.
//!
//! This is the automatic, ANALYZE-driven half of the distributed-join planner.
//! The manual override (`nodedb.force_shuffle_join`) always wins; when it is
//! off, [`cost_model_picks_shuffle`] decides whether a whole-join shuffle is
//! cheaper than the default broadcast plan, using the per-collection statistics
//! that `ANALYZE` persists into the system catalog.
//!
//! The decision mirrors Postgres' textbook flow: estimate each side's byte
//! size from `row_count * per_row_width`, then defer to
//! `nodedb_cluster::distributed_join::select_strategy`, which returns
//! `Shuffle` only when NEITHER side is small enough to broadcast under the
//! configured threshold.
//!
//! **Graceful fallback:** if EITHER side has never been analyzed (no stats),
//! the cost model returns `false` and the join keeps the default broadcast
//! plan — zero regression for un-analyzed collections.

use nodedb_cluster::distributed_join::{JoinStrategy, select_strategy};

use super::super::convert::ConvertContext;

/// Per-column fallback width (bytes) used when a column has no recorded
/// `avg_value_len`. A small constant keeps the estimate conservative without
/// assuming wide payloads for un-measured columns.
const DEFAULT_COLUMN_WIDTH_BYTES: usize = 16;

/// Decide whether the cost model selects a shuffle join over a broadcast join.
///
/// Looks up BOTH sides' estimated byte sizes from ANALYZE column statistics.
/// Returns:
/// - `false` if either side has no statistics (never analyzed) — graceful
///   broadcast fallback, no regression for un-analyzed collections.
/// - `false` if either estimate is zero (empty collection) — broadcast is
///   trivially correct and cheaper.
/// - `select_strategy(left, right, threshold) == Shuffle` otherwise.
///
/// `left_collection` / `right_collection` are the RAW (non-db-qualified)
/// collection names, matching how `ANALYZE` keys its persisted stats.
pub(super) fn cost_model_picks_shuffle(
    ctx: &ConvertContext,
    left_collection: &str,
    right_collection: &str,
) -> bool {
    let Some(left_bytes) = estimated_collection_bytes(ctx, left_collection) else {
        return false;
    };
    let Some(right_bytes) = estimated_collection_bytes(ctx, right_collection) else {
        return false;
    };
    if left_bytes == 0 || right_bytes == 0 {
        return false;
    }
    matches!(
        select_strategy(left_bytes, right_bytes, ctx.broadcast_threshold_bytes),
        JoinStrategy::Shuffle
    )
}

/// Estimate a collection's on-the-wire size in bytes from ANALYZE statistics.
///
/// Returns `None` when the collection has no statistics at all (never analyzed)
/// — the caller treats this as "broadcast". The estimate is:
///
/// ```text
/// per_row_width = Σ_columns (avg_value_len OR DEFAULT_COLUMN_WIDTH_BYTES)
/// estimated_bytes = row_count * per_row_width   (saturating)
/// ```
///
/// `row_count` is identical across a collection's columns (it is the table's
/// row count at ANALYZE time), so any column's value is representative; we take
/// the maximum to be robust against partially-written stat rows.
fn estimated_collection_bytes(ctx: &ConvertContext, collection: &str) -> Option<usize> {
    let credentials = ctx.credentials.as_ref()?;
    let catalog = credentials.catalog();
    let stats = catalog
        .load_column_stats(ctx.tenant_id.as_u64(), collection)
        .ok()?;
    if stats.is_empty() {
        return None;
    }

    let row_count = stats.iter().map(|s| s.row_count).max().unwrap_or(0);
    let per_row_width: usize = stats
        .iter()
        .map(|s| {
            s.avg_value_len
                .map_or(DEFAULT_COLUMN_WIDTH_BYTES, |w| w as usize)
        })
        .fold(0usize, |acc, w| acc.saturating_add(w));

    let row_count = usize::try_from(row_count).unwrap_or(usize::MAX);
    Some(row_count.saturating_mul(per_row_width))
}
