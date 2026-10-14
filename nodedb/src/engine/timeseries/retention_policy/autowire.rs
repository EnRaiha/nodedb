// SPDX-License-Identifier: BUSL-1.1

//! Auto-wiring: create/remove continuous aggregates from retention policy tiers.
//!
//! Each downsample tier of a retention policy is a continuous aggregate.
//! Tiers are chained: tier N aggregates from tier N-1 (cascading). A tier
//! registers on every core of every node, exactly as a user aggregate does:
//! at the policy's post-apply on every node, at boot from the stored
//! policies, and on the proposer before CREATE answers.

use crate::control::catalog_entry::post_apply::{
    register_continuous_aggregate_def_on_every_core, unregister_continuous_aggregate_on_every_core,
};
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{ContinuousAggregateDef, RefreshPolicy};
use crate::engine::timeseries::retention_policy::types::RetentionPolicyDef;

/// The continuous aggregate of every downsample tier of `def`, tier 1
/// first.
///
/// For each non-RAW tier:
/// - Name: `_policy_{policy_name}_tier{N}`
/// - Source: collection (tier 1) or previous tier's aggregate (tier 2+)
/// - Refresh: OnFlush for tier 1, OnSeal for higher tiers
pub fn tier_definitions(def: &RetentionPolicyDef) -> Vec<ContinuousAggregateDef> {
    def.downsample_tiers()
        .iter()
        .map(|tier| {
            let source = if tier.tier_index == 1 {
                def.collection.clone()
            } else {
                def.aggregate_name(tier.tier_index - 1)
            };
            // OnFlush for tier 1 (lowest latency), OnSeal for higher tiers
            // (lower CPU cost: cascaded data is less urgent).
            let refresh_policy = if tier.tier_index == 1 {
                RefreshPolicy::OnFlush
            } else {
                RefreshPolicy::OnSeal
            };
            ContinuousAggregateDef {
                database_id: def.database_id,
                name: def.aggregate_name(tier.tier_index),
                source,
                bucket_interval: format_interval_ms(tier.resolution_ms),
                bucket_interval_ms: i64::try_from(tier.resolution_ms).unwrap_or(i64::MAX),
                group_by: Vec::new(),
                aggregates: tier.aggregates.clone(),
                refresh_policy,
                retention_period_ms: tier.retain_ms,
                stale: false,
                // The policy's creation names this tier's incarnation, so
                // every registration of the same policy keeps the tier's
                // image.
                incarnation: nodedb_types::Hlc::new(def.created_at, 0),
            }
        })
        .collect()
}

/// Register the continuous aggregate of every downsample tier of `def` on
/// every core of this node.
pub async fn register_tiers(
    state: &SharedState,
    def: &RetentionPolicyDef,
) -> Result<(), crate::Error> {
    for agg_def in tier_definitions(def) {
        let name = agg_def.name.clone();
        let source = agg_def.source.clone();
        register_continuous_aggregate_def_on_every_core(state, def.tenant_id, agg_def).await?;
        tracing::info!(
            aggregate = name,
            source,
            policy = %def.name,
            "auto-wired continuous aggregate for retention policy"
        );
    }
    Ok(())
}

/// Unregister every continuous aggregate created by a retention policy
/// from every core of this node, highest tier first.
pub async fn unregister_tiers(
    state: &SharedState,
    def: &RetentionPolicyDef,
) -> Result<(), crate::Error> {
    for tier in def.downsample_tiers().iter().rev() {
        let name = def.aggregate_name(tier.tier_index);
        unregister_continuous_aggregate_on_every_core(state, def.database_id, def.tenant_id, &name)
            .await?;
        tracing::info!(
            aggregate = name,
            tier = tier.tier_index,
            "removed continuous aggregate for retention policy"
        );
    }
    Ok(())
}

/// Format milliseconds as a compact interval string for ContinuousAggregateDef.
fn format_interval_ms(ms: u64) -> String {
    const MINUTE: u64 = 60_000;
    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 86_400_000;

    if ms.is_multiple_of(DAY) {
        format!("{}d", ms / DAY)
    } else if ms.is_multiple_of(HOUR) {
        format!("{}h", ms / HOUR)
    } else if ms.is_multiple_of(MINUTE) {
        format!("{}m", ms / MINUTE)
    } else if ms.is_multiple_of(1_000) {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_interval() {
        assert_eq!(format_interval_ms(60_000), "1m");
        assert_eq!(format_interval_ms(3_600_000), "1h");
        assert_eq!(format_interval_ms(86_400_000), "1d");
        assert_eq!(format_interval_ms(30_000), "30s");
        assert_eq!(format_interval_ms(1_500), "1500ms");
    }
}
