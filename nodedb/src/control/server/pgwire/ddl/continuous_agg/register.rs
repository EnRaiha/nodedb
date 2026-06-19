// SPDX-License-Identifier: BUSL-1.1

//! Startup replay: re-register catalog-persisted continuous aggregates.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::pgwire::ddl::sync_dispatch;
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
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };
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
        // TODO(A8-followup): StoredContinuousAggregate is not yet keyed by
        // database; boot-time replay has no session database to thread.
        if let Err(e) = sync_dispatch::dispatch_async(
            state,
            tenant_id,
            crate::types::DatabaseId::DEFAULT,
            &def.source,
            plan,
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
