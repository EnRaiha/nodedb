// SPDX-License-Identifier: BUSL-1.1

//! Dispatch `MetaOp::UnregisterCollection` to the local Data Plane.
//!
//! Called from `catalog_entry::post_apply::async_dispatch::collection::purge_async`
//! on **every node** (leader and followers) so each node's Data
//! Plane reclaims its own local L1/L2 storage for the purged
//! collection symmetrically with the metadata row removal.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::MetaOp;

/// Dispatch `MetaOp::UnregisterCollection { tenant_id, name, purge_lsn }`
/// to this node's Data Plane. Best-effort: failures log at warn but
/// do not fail the catalog apply (the redb row is already gone; a
/// follow-up retry by the retention sweeper or operator manual PURGE
/// can complete the reclaim). The operation is idempotent by design.
pub async fn dispatch_unregister_collection(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
) {
    let tenant = TenantId::new(tenant_id);
    let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, name);
    let plan = PhysicalPlan::Meta(MetaOp::UnregisterCollection {
        tenant_id,
        name: name.to_string(),
        purge_lsn,
    });

    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        tracing::warn!(
            %name,
            tenant = tenant_id,
            error = %e,
            "failed to dispatch UnregisterCollection to Data Plane (non-fatal — retry on next GC pass)"
        );
    }
}
