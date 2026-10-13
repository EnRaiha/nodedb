// SPDX-License-Identifier: BUSL-1.1

//! AUTO_TIER query routing for tiered retention policies.
//!
//! When a timeseries collection has an enabled retention policy with
//! `auto_tier = true`, a scan carries the policy's tier layout as a
//! [`TieringPlan`]. The core that owns the collection splits the query range
//! into segments with [`TieringPlan::segments`], reads recent segments from
//! raw rows and older ones from each tier's continuous aggregate, and merges
//! the groups before HAVING, sort and limit. EXPLAIN TIERS shows the same
//! split through the same function.

use nodedb_physical::physical_plan::{TierRead, TierSource, TieringError, TieringPlan};

use crate::engine::timeseries::retention_policy::RetentionPolicyDef;

/// The tier layout a scan carries for `policy`.
///
/// Tier 0 is the raw tier; its retention bounds the raw segment. Every
/// later tier reads the continuous aggregate the policy created for it.
pub(crate) fn tiering_plan(policy: &RetentionPolicyDef) -> TieringPlan {
    let raw_retain_ms = policy.raw_tier().map_or(0, |raw| raw.retain_ms);
    let tiers = policy
        .downsample_tiers()
        .iter()
        .map(|tier| TierRead {
            tier_index: tier.tier_index,
            aggregate: policy.aggregate_name(tier.tier_index),
            resolution_ms: i64::try_from(tier.resolution_ms).unwrap_or(i64::MAX),
            retain_ms: tier.retain_ms,
        })
        .collect();
    TieringPlan {
        raw_retain_ms,
        tiers,
    }
}

/// Explain the tier selection for a query: one line per segment.
///
/// Used by EXPLAIN TIERS. It splits the range with the same
/// [`TieringPlan::segments`] the scan executes.
pub(crate) fn explain_tier_selection(
    policy: &RetentionPolicyDef,
    query_time_range: (i64, i64),
) -> Result<String, TieringError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let plan = tiering_plan(policy);
    let segments = plan.segments(query_time_range, now_ms)?;

    if segments.is_empty() {
        return Ok(format!(
            "AUTO_TIER on '{}': no tier covers the requested range",
            policy.collection
        ));
    }

    let mut lines = vec![format!(
        "AUTO_TIER plan for '{}' ({} tiers, {} segments):",
        policy.collection,
        policy.tiers.len(),
        segments.len()
    )];
    // Oldest first, as a reader expects a timeline.
    for seg in segments.iter().rev() {
        let (label, source) = match seg.source {
            TierSource::Raw => ("RAW".to_string(), policy.collection.clone()),
            TierSource::Tier(index) => match plan.tiers.get(index) {
                Some(tier) => (
                    format!("tier{} ({}ms buckets)", tier.tier_index, tier.resolution_ms),
                    tier.aggregate.clone(),
                ),
                None => continue,
            },
        };
        lines.push(format!(
            "  [{} .. {}) → {} ({})",
            seg.start, seg.end, label, source,
        ));
    }

    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::TierDef;

    fn make_policy() -> RetentionPolicyDef {
        RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name: "sensor_policy".into(),
            collection: "sensor_data".into(),
            tiers: vec![
                TierDef {
                    tier_index: 0,
                    resolution_ms: 0,
                    aggregates: Vec::new(),
                    retain_ms: 604_800_000, // 7 days
                    archive: None,
                },
                TierDef {
                    tier_index: 1,
                    resolution_ms: 60_000, // 1 minute
                    aggregates: Vec::new(),
                    retain_ms: 7_776_000_000, // 90 days
                    archive: None,
                },
                TierDef {
                    tier_index: 2,
                    resolution_ms: 3_600_000, // 1 hour
                    aggregates: Vec::new(),
                    retain_ms: 63_072_000_000, // 2 years
                    archive: None,
                },
            ],
            auto_tier: true,
            enabled: true,
            eval_interval_ms: 3_600_000,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn plan_names_each_tier_aggregate() {
        let plan = tiering_plan(&make_policy());
        assert_eq!(plan.raw_retain_ms, 604_800_000);
        assert_eq!(plan.tiers.len(), 2);
        assert_eq!(plan.tiers[0].aggregate, "_policy_sensor_policy_tier1");
        assert_eq!(plan.tiers[1].resolution_ms, 3_600_000);
    }

    #[test]
    fn range_spanning_raw_and_tier1_gives_two_segments() {
        let plan = tiering_plan(&make_policy());
        let now = 100_000_000_000i64;
        let segments = plan
            .segments((now - 30 * 86_400_000, now), now)
            .expect("split");
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].source, TierSource::Raw);
        assert_eq!(segments[1].source, TierSource::Tier(0));
    }

    #[test]
    fn old_range_reads_the_coarsest_tier_only() {
        let plan = tiering_plan(&make_policy());
        let now = 100_000_000_000i64;
        let segments = plan
            .segments((now - 365 * 86_400_000, now - 180 * 86_400_000), now)
            .expect("split");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].source, TierSource::Tier(1));
    }

    #[test]
    fn explain_lists_segments_oldest_first() {
        let text = explain_tier_selection(&make_policy(), (0, i64::MAX)).expect("explain");
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].starts_with("AUTO_TIER plan for 'sensor_data'"));
        assert!(lines.last().is_some_and(|l| l.contains("RAW")));
    }
}
