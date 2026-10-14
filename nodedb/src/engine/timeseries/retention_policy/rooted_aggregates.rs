// SPDX-License-Identifier: BUSL-1.1

//! The continuous aggregates a collection's partition removal must wait for.

use nodedb_physical::physical_plan::AggregateIncarnation;

use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;

use super::RetentionPolicyRegistry;

/// Every continuous aggregate rooted at `collection`: the user aggregates
/// the catalog chains down to it, and the downsample tiers of every enabled
/// retention policy in `registry` on it. A pass that removes partitions of `collection`
/// folds the expiring ones into each of them first, so the Data Plane
/// refuses the pass until each one is registered.
///
/// Raw retention and the bitemporal audit purge both send this list.
pub fn aggregates_rooted_at(
    state: &SharedState,
    registry: &RetentionPolicyRegistry,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<AggregateIncarnation>> {
    let catalog = state.credentials.catalog();
    let mut out = Vec::new();
    for stored in catalog.list_continuous_aggregates(database_id, tenant_id)? {
        let root =
            crate::control::server::shared::ddl::neutral::continuous_agg::refresh::resolve_root(
                &stored.name,
                stored.source.clone(),
                |agg| {
                    catalog
                        .get_continuous_aggregate(database_id, tenant_id, agg)
                        .map(|s| s.map(|s| s.source))
                },
            )?;
        if root != collection {
            continue;
        }
        let def: ContinuousAggregateDef =
            zerompk::from_msgpack(&stored.def_bytes).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "continuous aggregate '{}': definition decode: {e}",
                    stored.name
                ),
            })?;
        out.push(AggregateIncarnation {
            name: stored.name,
            incarnation: def.incarnation,
        });
    }
    for policy in registry.list_all_enabled() {
        if policy.database_id != database_id
            || policy.tenant_id != tenant_id
            || policy.collection != collection
        {
            continue;
        }
        for tier in super::autowire::tier_definitions(&policy) {
            out.push(AggregateIncarnation {
                name: tier.name,
                incarnation: tier.incarnation,
            });
        }
    }
    Ok(out)
}
