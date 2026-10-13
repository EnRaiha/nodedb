// SPDX-License-Identifier: BUSL-1.1

//! Boot re-registration of catalog-persisted continuous aggregates.

use crate::control::catalog_entry::post_apply::{
    ContinuousAggregateRegisterFailure, finish_continuous_aggregate_boot_registration,
    register_continuous_aggregate_on_every_core,
};
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::autowire::register_tiers;

/// Re-register every catalog-persisted continuous aggregate on every local
/// Data Plane core: the user aggregates, then the downsample tiers of every
/// stored retention policy. Then every core drops the persisted image
/// entries of aggregates that are not registered, which belong to dropped
/// ones.
///
/// The per-core `continuous_agg_mgr` is in-memory, and the metadata applier
/// skips entries it already applied. Boot therefore rebuilds the registry
/// from redb in single-node and cluster mode. Any aggregate that does not
/// reach every core fails the boot: serving with it inactive silently stops
/// its bucket aggregation.
pub async fn register_persisted_continuous_aggregates(state: &SharedState) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    for s in catalog.load_all_continuous_aggregates()? {
        register_continuous_aggregate_on_every_core(state, s.tenant_id, &s.name, &s.def_bytes)
            .await
            .map_err(
                |failure: ContinuousAggregateRegisterFailure| match failure.error {
                    crate::Error::Codec { .. } => failure.error,
                    error => crate::Error::Internal {
                        detail: format!(
                            "continuous aggregate '{}' (tenant {}, database {}): {}: {error}",
                            s.name,
                            s.tenant_id,
                            failure.database_id,
                            failure.stage.label()
                        ),
                    },
                },
            )?;
    }
    for policy in catalog.load_all_retention_policies()? {
        register_tiers(state, &policy)
            .await
            .map_err(|error| crate::Error::Internal {
                detail: format!(
                    "tiers of retention policy '{}' (tenant {}, database {}): {error}",
                    policy.name, policy.tenant_id, policy.database_id
                ),
            })?;
    }
    finish_continuous_aggregate_boot_registration(state).await
}
