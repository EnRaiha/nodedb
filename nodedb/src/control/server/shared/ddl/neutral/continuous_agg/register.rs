// SPDX-License-Identifier: BUSL-1.1

//! Startup replay: re-register catalog-persisted continuous aggregates.
//!
//! Moved verbatim from the pgwire `ddl::continuous_agg::register` module. This is
//! a boot-time Data-Plane replay helper, not a DDL statement handler, so it
//! carries no pgwire response types and is unchanged by the neutral migration.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_physical::physical_plan::MetaOp;

/// Re-register every catalog-persisted continuous aggregate on the
/// local Data Plane. Called at startup on paths that don't trigger
/// the raft post-apply chain (single-node restart, the
/// `nodedb-test-support` harness): the `continuous_agg_mgr` is a
/// per-core in-memory registry, so without explicit replay every
/// aggregate becomes silently inactive after restart and the runtime
/// bucket-aggregation pipeline forgets the definitions.
pub async fn register_persisted_continuous_aggregates(state: &SharedState) {
    let catalog = state.credentials.catalog();
    let stored = match catalog.load_all_continuous_aggregates() {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "boot: failed to load continuous aggregates from catalog");
            return;
        }
    };
    for s in stored {
        let def: ContinuousAggregateDef = match zerompk::from_msgpack(&s.def_bytes) {
            Ok(def) => def,
            Err(e) => {
                tracing::warn!(
                    cagg = %s.name,
                    tenant = s.tenant_id,
                    error = %e,
                    "boot: failed to decode continuous aggregate def — skipping replay"
                );
                continue;
            }
        };
        let plan = PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate { def: def.clone() });
        let tenant_id = crate::types::TenantId::new(s.tenant_id);
        if let Err(e) = sync_dispatch::dispatch_system(
            state,
            sync_dispatch::SystemTask::new(
                sync_dispatch::SystemReason::CatalogMaintenance,
                tenant_id,
                crate::types::DatabaseId::new(def.database_id),
                &def.source,
                plan,
            ),
            Duration::from_secs(5),
        )
        .await
        {
            tracing::warn!(
                cagg = %s.name,
                tenant = s.tenant_id,
                error = %e,
                "boot: failed to re-register continuous aggregate on Data Plane"
            );
        }
    }
}
