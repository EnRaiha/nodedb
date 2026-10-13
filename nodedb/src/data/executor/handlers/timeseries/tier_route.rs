// SPDX-License-Identifier: BUSL-1.1

//! AUTO_TIER routing of one timeseries scan on the core that owns the
//! collection: which part of the range raw rows serve, and which parts each
//! downsample tier serves.

use nodedb_physical::physical_plan::{TierRead, TierSource, TieringPlan};

use crate::bridge::scan_filter::ScanFilter;

/// One part of the range a downsample tier serves: buckets in `[start, end)`.
#[derive(Debug, Clone)]
pub(super) struct TierSpan {
    pub tier: TierRead,
    pub start: i64,
    pub end: i64,
}

/// A scan's range split across raw rows and downsample tiers.
#[derive(Debug, Clone)]
pub(super) struct TierSplit {
    /// The inclusive range raw rows serve. `None` when tiers serve all of it.
    pub raw_range: Option<(i64, i64)>,
    /// The ranges downsample tiers serve, newest first.
    pub tiers: Vec<TierSpan>,
}

/// Split the inclusive `time_range` by `tiering` at `now_ms`. Without a
/// tier layout, raw rows serve the whole range.
pub(super) fn split_tiers(
    tiering: Option<&TieringPlan>,
    time_range: (i64, i64),
    now_ms: i64,
) -> crate::Result<TierSplit> {
    let Some(plan) = tiering else {
        return Ok(TierSplit {
            raw_range: Some(time_range),
            tiers: Vec::new(),
        });
    };
    let segments = plan
        .segments(time_range, now_ms)
        .map_err(|e| crate::Error::BadRequest {
            detail: format!("AUTO_TIER: {e}"),
        })?;
    let mut split = TierSplit {
        raw_range: None,
        tiers: Vec::new(),
    };
    for segment in segments {
        match segment.source {
            TierSource::Raw => {
                split.raw_range = Some((segment.start, segment.end.saturating_sub(1)));
            }
            TierSource::Tier(index) => {
                let tier =
                    plan.tiers
                        .get(index)
                        .cloned()
                        .ok_or_else(|| crate::Error::Internal {
                            detail: format!(
                                "AUTO_TIER segment names tier {index}, which the plan lacks"
                            ),
                        })?;
                split.tiers.push(TierSpan {
                    tier,
                    start: segment.start,
                    end: segment.end,
                });
            }
        }
    }
    Ok(split)
}

/// Refuse a read that needs raw rows over a range only downsampled tiers
/// hold: a raw scan, or the generic aggregate's input scan.
pub(super) fn refuse_raw_read_of_tiers(collection: &str, split: &TierSplit) -> crate::Result<()> {
    match split.tiers.first() {
        None => Ok(()),
        Some(span) => Err(crate::Error::BadRequest {
            detail: format!(
                "rows of '{collection}' in [{}, {}) are held only by AUTO_TIER tier {} \
                 ('{}') as downsampled buckets; this query needs raw rows, so narrow the time \
                 range to the raw retention window or use an aggregate the tiers can merge \
                 (COUNT, SUM, AVG, MIN, MAX without GROUP BY columns)",
                span.start, span.end, span.tier.tier_index, span.tier.aggregate
            ),
        }),
    }
}

/// The reasons a grouped scan cannot merge tier buckets exactly, checked
/// before any tier is read.
///
/// Tier buckets hold per-bucket totals with no group columns, no per-row
/// predicates, and no variance state. `time_key` names the column whose
/// bounds already narrowed the range.
pub(super) fn refuse_unmergeable(
    collection: &str,
    span: &TierSpan,
    shape: TierMergeShape<'_>,
) -> crate::Result<()> {
    let TierMergeShape {
        group_by,
        aggregates,
        bucket_interval_ms,
        filter_predicates,
        time_key,
        temporal,
    } = shape;
    let resolution = span.tier.resolution_ms;
    let reason = if !group_by.is_empty() {
        Some("tier buckets carry no GROUP BY columns".to_string())
    } else if let Some(filter) = filter_predicates
        .iter()
        .find(|f| !narrows_time_range(f, time_key))
    {
        Some(format!(
            "the predicate on '{}' cannot be applied to downsampled buckets",
            filter.field
        ))
    } else if !edge_on_bucket(span.start, resolution, i64::MIN)
        || !edge_on_bucket(span.end, resolution, i64::MAX)
    {
        Some(format!(
            "the query's time bounds fall inside a {resolution}ms tier bucket; align them to \
             the tier resolution"
        ))
    } else if bucket_interval_ms > 0 && bucket_interval_ms % span.tier.resolution_ms != 0 {
        Some(format!(
            "time_bucket width {bucket_interval_ms}ms is not a multiple of the tier's {}ms \
             resolution",
            span.tier.resolution_ms
        ))
    } else if temporal {
        Some("FOR SYSTEM_TIME / FOR VALID_TIME reads raw versions only".to_string())
    } else {
        aggregates
            .iter()
            .find(|(op, _)| !matches!(op.as_str(), "count" | "sum" | "avg" | "min" | "max"))
            .map(|(op, field)| {
                format!("{op}({field}) cannot be merged exactly from downsampled buckets")
            })
    };
    match reason {
        None => Ok(()),
        Some(reason) => Err(crate::Error::BadRequest {
            detail: format!(
                "AUTO_TIER on '{collection}': [{}, {}) is held by tier {} ('{}'); {reason}",
                span.start, span.end, span.tier.tier_index, span.tier.aggregate
            ),
        }),
    }
}

/// Whether `filter` only bounds the time key, so the scan's time range
/// already applies it.
fn narrows_time_range(filter: &ScanFilter, time_key: &str) -> bool {
    use crate::bridge::scan_filter::FilterOp;
    filter.op == FilterOp::MatchAll
        || (filter.field == time_key
            && filter.clauses.is_empty()
            && matches!(
                filter.op,
                FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte
            ))
}

/// Whether a span edge falls on a tier bucket boundary, so the buckets the
/// span reads hold no row outside it. The open end of the range (`open`)
/// has no row beyond it.
fn edge_on_bucket(edge: i64, resolution_ms: i64, open: i64) -> bool {
    edge == open || edge.rem_euclid(resolution_ms) == 0
}

/// The parts of a grouped scan [`refuse_unmergeable`] checks.
pub(super) struct TierMergeShape<'a> {
    pub group_by: &'a [String],
    pub aggregates: &'a [(String, String)],
    pub bucket_interval_ms: i64,
    pub filter_predicates: &'a [ScanFilter],
    pub time_key: &'a str,
    pub temporal: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000;

    fn plan() -> TieringPlan {
        TieringPlan {
            raw_retain_ms: 7 * DAY as u64,
            tiers: vec![TierRead {
                tier_index: 1,
                aggregate: "_policy_p_tier1".into(),
                resolution_ms: 60_000,
                retain_ms: 0,
            }],
        }
    }

    #[test]
    fn no_layout_is_raw_only() {
        let split = split_tiers(None, (0, 10), 100).expect("split");
        assert_eq!(split.raw_range, Some((0, 10)));
        assert!(split.tiers.is_empty());
    }

    #[test]
    fn old_range_is_refused_for_raw_reads() {
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (0, now), now).expect("split");
        assert!(split.raw_range.is_some());
        assert_eq!(split.tiers.len(), 1);
        assert!(refuse_raw_read_of_tiers("m", &split).is_err());
    }

    #[test]
    fn grouped_tier_read_is_refused() {
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (0, now), now).expect("split");
        let shape = TierMergeShape {
            group_by: &["host".to_string()],
            aggregates: &[("count".to_string(), "*".to_string())],
            bucket_interval_ms: 0,
            filter_predicates: &[],
            time_key: "ts",
            temporal: false,
        };
        assert!(refuse_unmergeable("m", &split.tiers[0], shape).is_err());
    }

    #[test]
    fn stddev_tier_read_is_refused() {
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (0, now), now).expect("split");
        let shape = TierMergeShape {
            group_by: &[],
            aggregates: &[("ts_stddev".to_string(), "v".to_string())],
            bucket_interval_ms: 0,
            filter_predicates: &[],
            time_key: "ts",
            temporal: false,
        };
        assert!(refuse_unmergeable("m", &split.tiers[0], shape).is_err());
    }

    fn count_shape<'a>(filters: &'a [ScanFilter]) -> TierMergeShape<'a> {
        TierMergeShape {
            group_by: &[],
            aggregates: &[],
            bucket_interval_ms: 0,
            filter_predicates: filters,
            time_key: "ts",
            temporal: false,
        }
    }

    #[test]
    fn unbounded_count_merges() {
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (i64::MIN, i64::MAX), now).expect("split");
        assert!(refuse_unmergeable("m", &split.tiers[0], count_shape(&[])).is_ok());
    }

    #[test]
    fn bound_inside_a_tier_bucket_is_refused() {
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (1_000, i64::MAX), now).expect("split");
        assert!(refuse_unmergeable("m", &split.tiers[0], count_shape(&[])).is_err());
        let split = split_tiers(Some(&plan()), (60_000, i64::MAX), now).expect("split");
        assert!(refuse_unmergeable("m", &split.tiers[0], count_shape(&[])).is_ok());
    }

    #[test]
    fn non_range_time_predicate_is_refused() {
        use crate::bridge::scan_filter::FilterOp;
        let now = 1_000 * DAY;
        let split = split_tiers(Some(&plan()), (i64::MIN, i64::MAX), now).expect("split");
        let filter = |op| ScanFilter {
            field: "ts".into(),
            op,
            value: nodedb_types::Value::Integer(5),
            clauses: Vec::new(),
            expr: None,
        };
        let ne = [filter(FilterOp::Ne)];
        assert!(refuse_unmergeable("m", &split.tiers[0], count_shape(&ne)).is_err());
        let gte = [filter(FilterOp::Gte)];
        assert!(refuse_unmergeable("m", &split.tiers[0], count_shape(&gte)).is_ok());
    }
}
