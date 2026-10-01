// SPDX-License-Identifier: BUSL-1.1

//! Boot re-registration of catalog-persisted continuous aggregates.

use crate::control::catalog_entry::post_apply::{
    ContinuousAggregateRegisterFailure, register_continuous_aggregate_on_every_core,
};
use crate::control::state::SharedState;

/// Re-register every catalog-persisted continuous aggregate on every local
/// Data Plane core.
///
/// The per-core `continuous_agg_mgr` is in-memory, and the metadata applier
/// skips entries it already applied. Boot therefore rebuilds the registry
/// from redb in single-node and cluster mode. Any aggregate that does not
/// reach every core fails the boot: serving with it inactive silently stops
/// its bucket aggregation.
pub async fn register_persisted_continuous_aggregates(state: &SharedState) -> crate::Result<()> {
    let stored = state
        .credentials
        .catalog()
        .load_all_continuous_aggregates()?;
    for s in stored {
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
    Ok(())
}
