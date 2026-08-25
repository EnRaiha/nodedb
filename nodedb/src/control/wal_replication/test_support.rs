// SPDX-License-Identifier: BUSL-1.1

//! Shared test helper for `wal_replication`'s test modules.

use super::*;
use crate::bridge::envelope::PhysicalPlan;
use crate::types::{DatabaseId, TenantId, VShardId};

/// Decide + encode in one call, so each test names only the plan it encodes.
/// Shadows the crate-level `to_replicated_entry`, which now takes a decided
/// [`ReplicableWrite`].
pub(super) fn to_replicated_entry(
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: &PhysicalPlan,
) -> crate::Result<Option<ReplicatedEntry>> {
    let write = ReplicableWrite::decide_for_replication(plan)?;
    encode::to_replicated_entry(tenant_id, database_id, vshard_id, &write)
}
