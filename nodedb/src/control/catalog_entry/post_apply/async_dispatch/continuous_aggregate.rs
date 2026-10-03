// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for continuous-aggregate catalog entries.
//!
//! `PutContinuousAggregate` dispatches `MetaOp::RegisterContinuousAggregate`
//! to every core on this node so the local `continuous_agg_mgr` picks up
//! the new definition without re-issuing the DDL — this is what makes the
//! registration consistent across leader and followers after the raft
//! commit. `DeleteContinuousAggregate` dispatches the matching
//! `MetaOp::UnregisterContinuousAggregate`.
//!
//! Every core must apply the op. The catalog row is already committed and
//! the post-apply lane cannot propagate, so every failure files a report.
//! Boot re-registration installs every stored aggregate again from redb, so a
//! lost post-apply dispatch is repaired at the next boot.

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::ContinuousAggregateDef;
use nodedb_physical::physical_plan::MetaOp;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// Name the fan-out reports a continuous-aggregate dispatch under. The
/// fan-out addresses each core directly, so this name only labels the ack
/// line and the unreached-core error.
const CAGG_SENTINEL_COLLECTION: &str = "_continuous_aggregates";

/// Dispatch `MetaOp::RegisterContinuousAggregate` to every core on
/// this node. `def_bytes` is the MessagePack-encoded
/// `ContinuousAggregateDef` from the catalog row.
pub async fn put_async(tenant_id: u64, name: String, def_bytes: Vec<u8>, shared: Arc<SharedState>) {
    if let Err(failure) = register_on_every_core(&shared, tenant_id, &name, &def_bytes).await {
        crate::diag::continuous_aggregate_not_applied(
            &failure.error,
            failure.stage.label(),
            failure.database_id,
            tenant_id,
            &name,
        );
    }
}

/// Why a register did not reach every core, and at which stage.
pub struct RegisterFailure {
    pub error: crate::Error,
    pub stage: RegisterStage,
    /// Zero when the definition did not decode, since the definition names it.
    pub database_id: u64,
}

/// The stage of a register that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterStage {
    /// The stored definition did not decode.
    Decode,
    /// The register did not reach every core.
    Dispatch,
}

impl RegisterStage {
    /// The stage name the diagnostic report carries.
    pub fn label(self) -> &'static str {
        match self {
            Self::Decode => "put_decode",
            Self::Dispatch => "put_dispatch",
        }
    }
}

/// Decode one stored definition and register it on every core on this node.
///
/// Shared by the post-apply lane and boot re-registration, so both install
/// the aggregate on the same set of cores.
pub async fn register_on_every_core(
    shared: &SharedState,
    tenant_id: u64,
    name: &str,
    def_bytes: &[u8],
) -> Result<(), RegisterFailure> {
    let def: ContinuousAggregateDef =
        zerompk::from_msgpack(def_bytes).map_err(|e| RegisterFailure {
            error: crate::Error::Codec {
                detail: format!("continuous aggregate '{name}': definition decode: {e}"),
            },
            stage: RegisterStage::Decode,
            database_id: 0,
        })?;
    let database_id = def.database_id;
    let plan = PhysicalPlan::Meta(MetaOp::RegisterContinuousAggregate { def });
    let fanout = fanout_for(database_id, tenant_id, name);
    dispatch_to_every_core(shared, &fanout, &plan)
        .await
        .map_err(|error| RegisterFailure {
            error,
            stage: RegisterStage::Dispatch,
            database_id,
        })
}

/// Dispatch `MetaOp::UnregisterContinuousAggregate` to every core
/// on this node. `database_id` scopes the unregister to the right
/// per-database manager map.
pub async fn delete_async(
    database_id: u64,
    tenant_id: u64,
    name: String,
    shared: Arc<SharedState>,
) {
    let plan = PhysicalPlan::Meta(MetaOp::UnregisterContinuousAggregate { name: name.clone() });
    let fanout = fanout_for(database_id, tenant_id, &name);
    if let Err(error) = dispatch_to_every_core(&shared, &fanout, &plan).await {
        crate::diag::continuous_aggregate_not_applied(
            &error,
            "delete_dispatch",
            database_id,
            tenant_id,
            &name,
        );
    }
}

fn fanout_for(database_id: u64, tenant_id: u64, name: &str) -> CoreFanout<'_> {
    CoreFanout {
        database_id,
        tenant_id,
        collection: CAGG_SENTINEL_COLLECTION,
        what: "continuous aggregate change",
        detail: name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fan-out carries the aggregate's own database, so an aggregate in
    /// one database never registers in another's per-core manager map.
    #[test]
    fn the_fanout_carries_the_aggregates_own_database() {
        let fanout = fanout_for(7, 3, "hourly");
        assert_eq!(fanout.database_id, 7);
        assert_eq!(fanout.tenant_id, 3);
        assert_eq!(fanout.detail, "hourly");
        assert_eq!(fanout.collection, CAGG_SENTINEL_COLLECTION);
    }
}
