// SPDX-License-Identifier: BUSL-1.1

//! Cost-based Gather-vs-shuffle aggregate selection.
//!
//! This is the automatic, ANALYZE-driven half of the distributed-GROUP-BY
//! planner. The manual override (`nodedb.force_shuffle_agg`) always wins; when
//! it is off, [`cost_model_picks_aggregate_shuffle`] decides whether a
//! whole-aggregate shuffle is cheaper than the default Gather-merge plan, using
//! the per-column statistics that `ANALYZE` persists into the system catalog.
//!
//! The decision turns on **group cardinality** — the number of distinct groups
//! the GROUP BY produces. The default plan emits a bare per-shard `Aggregate`
//! wrapped (by `convert.rs`) in `Exchange{Gather{as_aggregate}}`: every shard
//! sends its partial-aggregate rows to the single coordinator, which merges all
//! of them. When the group cardinality is high, that coordinator merge of many
//! partial rows is the bottleneck — a single core finalizing millions of
//! groups. Shuffling instead hashes each group to a part-owner so the finalize
//! is parallelized across part-owners. When the group cardinality is low, the
//! Gather merge is cheap (few rows) and shuffling only adds an extra network
//! hop, so we keep Gather.
//!
//! **Graceful fallback:** if the collection has never been analyzed (no stats),
//! or ANY group-by column lacks a stats entry (so the group count cannot be
//! estimated), the cost model returns `false` and the aggregate keeps the
//! default Gather plan — zero regression for un-analyzed collections.

use super::super::convert::ConvertContext;

/// Decide whether the cost model selects a whole-aggregate shuffle over the
/// default Gather-merge plan.
///
/// Estimates the GROUP cardinality (number of distinct groups) from the
/// `distinct_count` of each GROUP BY column in the collection's ANALYZE
/// statistics, then compares it against `ctx.shuffle_agg_threshold`
/// (in distinct-group units). Returns:
/// - `false` if the collection has no statistics (never analyzed) — graceful
///   Gather fallback, no regression for un-analyzed collections.
/// - `false` if ANY group-by column has no stats entry — the group count cannot
///   be estimated, so we do not shuffle (correctness-neutral conservatism).
/// - `false` if the estimated group cardinality is zero (empty collection) —
///   Gather is trivially cheap.
/// - `group_card > ctx.shuffle_agg_threshold` otherwise.
///
/// `collection` is the db-qualified collection name; `group_by` are the raw
/// GROUP BY column names, matching how `ANALYZE` keys its persisted stats.
pub(super) fn cost_model_picks_aggregate_shuffle(
    ctx: &ConvertContext,
    collection: &str,
    group_by: &[String],
) -> bool {
    let Some(group_card) = estimated_group_cardinality(ctx, collection, group_by) else {
        return false;
    };
    if group_card == 0 {
        return false;
    }
    group_card > ctx.shuffle_agg_threshold
}

/// Estimate the number of distinct groups a GROUP BY over `group_by` produces,
/// from ANALYZE column statistics.
///
/// Returns `None` when:
/// - the collection has no statistics at all (never analyzed), or
/// - any column in `group_by` has no stats entry (cannot estimate).
///
/// The estimate is the product of each group-by column's `distinct_count`,
/// computed with `saturating_mul` to avoid overflow, then capped at the
/// collection `row_count` (the number of distinct groups can never exceed the
/// number of rows — independent per-column distinct counts overcount when
/// columns are correlated, and the row count is a hard upper bound):
///
/// ```text
/// group_card = min( Π_columns distinct_count , row_count )
/// ```
///
/// `row_count` is identical across a collection's columns (it is the table's
/// row count at ANALYZE time), so any column's value is representative; we take
/// the maximum to be robust against partially-written stat rows.
fn estimated_group_cardinality(
    ctx: &ConvertContext,
    collection: &str,
    group_by: &[String],
) -> Option<usize> {
    let credentials = ctx.credentials.as_ref()?;
    let catalog = credentials.catalog();
    let stats = catalog
        .load_column_stats(ctx.tenant_id.as_u64(), collection)
        .ok()?;
    if stats.is_empty() {
        return None;
    }

    let row_count = stats.iter().map(|s| s.row_count).max().unwrap_or(0);
    let row_count = usize::try_from(row_count).unwrap_or(usize::MAX);

    let mut group_card: usize = 1;
    for col in group_by {
        let entry = stats.iter().find(|s| s.column == *col)?;
        let distinct = usize::try_from(entry.distinct_count).unwrap_or(usize::MAX);
        group_card = group_card.saturating_mul(distinct);
    }

    Some(group_card.min(row_count))
}
