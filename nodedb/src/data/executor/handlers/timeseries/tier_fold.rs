// SPDX-License-Identifier: BUSL-1.1

//! Fold AUTO_TIER downsample buckets into a grouped timeseries aggregate.
//!
//! A tier is a continuous aggregate with no GROUP BY columns. Each of its
//! buckets holds the row count and, per source column, the exact total and
//! the extremes. Those merge exactly into COUNT, SUM, AVG, MIN and MAX of a
//! query with no GROUP BY columns whose time bucket, if any, is a multiple
//! of the tier's resolution. `tier_route::refuse_unmergeable` refuses every
//! other shape before this runs.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_agg::AggAccum;
use crate::engine::timeseries::continuous_agg::{ColumnLayout, PartialAggregate};
use crate::engine::timeseries::grouped_scan::{GroupKey, GroupedAggResult};
use crate::engine::timeseries::time_bucket::time_bucket;
use crate::types::{DatabaseId, TenantId};

use super::tier_route::TierSpan;

/// Inputs to [`CoreLoop::fold_tier_buckets`].
pub(super) struct TierFold<'a> {
    pub database_id: DatabaseId,
    pub tid: TenantId,
    /// The scanned collection: the root every tier of its policy hangs from.
    pub collection: &'a str,
    pub spans: &'a [TierSpan],
    pub aggregates: &'a [(String, String)],
    pub bucket_interval_ms: i64,
    pub now_ms: i64,
}

impl CoreLoop {
    /// The grouped state of every tier bucket inside `spans`.
    ///
    /// The tiers first fold every flushed partition they lack, so each
    /// bucket counts every flushed row of its range. Every tier, tier 2 and
    /// up included, is registered on every core and rolls down from the
    /// root collection on the core that owns it, which is the core that
    /// runs this scan. An unregistered tier, a stale tier, and a column a
    /// tier does not keep are errors.
    pub(super) fn fold_tier_buckets(&mut self, p: TierFold<'_>) -> crate::Result<GroupedAggResult> {
        let db = p.database_id.as_u64();
        let tenant = p.tid.as_u64();
        self.complete_cagg_backfill(db, tenant, p.collection, p.now_ms)?;

        let mut result = GroupedAggResult::new(p.aggregates.len());
        for span in p.spans {
            let name = span.tier.aggregate.as_str();
            let def = self
                .continuous_agg_mgr
                .get_definition(db, tenant, name)
                .ok_or(crate::Error::BadRequest {
                    detail: format!(
                        "AUTO_TIER on '{}': tier {} aggregate '{name}' is not registered; the \
                         retention policy changed while the query ran",
                        p.collection, span.tier.tier_index
                    ),
                })?;
            if def.stale {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "AUTO_TIER on '{}': tier {} aggregate '{name}' is stale; refresh it \
                         before reading its range",
                        p.collection, span.tier.tier_index
                    ),
                });
            }
            let layout = ColumnLayout::of(def);
            // `[start, end)` holds whole buckets: the range read is inclusive.
            let buckets = self
                .continuous_agg_mgr
                .get_materialized_range(db, tenant, name, span.start, span.end.saturating_sub(1))
                .unwrap_or_default();
            for bucket in buckets {
                let key = GroupKey {
                    bucket: (p.bucket_interval_ms > 0)
                        .then(|| time_bucket(p.bucket_interval_ms, bucket.bucket_ts)),
                    cells: Vec::new(),
                };
                let accums = p
                    .aggregates
                    .iter()
                    .map(|(op, field)| tier_accum(bucket, &layout, op, field, name))
                    .collect::<crate::Result<Vec<AggAccum>>>()?;
                let mut one = GroupedAggResult::new(p.aggregates.len());
                one.groups.insert(key, accums);
                result.merge(&one);
            }
        }
        Ok(result)
    }
}

/// One query aggregate's state over one tier bucket.
fn tier_accum(
    bucket: &PartialAggregate,
    layout: &ColumnLayout,
    op: &str,
    field: &str,
    tier: &str,
) -> crate::Result<AggAccum> {
    if op == "count" && field == "*" {
        return AggAccum::from_totals(bucket.count, Default::default(), None, None)
            .map_err(crate::Error::from);
    }
    let column = layout
        .slot(field)
        .and_then(|slot| bucket.columns.get(slot))
        .ok_or(crate::Error::BadRequest {
            detail: format!(
                "AUTO_TIER tier aggregate '{tier}' keeps no state for column '{field}', so \
                 {op}({field}) cannot be read from it"
            ),
        })?;
    AggAccum::from_totals(
        column.sum.count(),
        column.sum.clone(),
        column.min.clone(),
        column.max.clone(),
    )
    .map_err(crate::Error::from)
}
