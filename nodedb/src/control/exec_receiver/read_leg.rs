// SPDX-License-Identifier: BUSL-1.1

//! The serving half of a linearizable read leg sent by another node.

use std::time::Duration;

use nodedb_cluster::rpc_codec::TypedClusterError;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::read_leg::confirm_local_read;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::support::execution_error_to_typed;

/// Confirm a linearizable read leg on this node before it reads.
///
/// `read_groups` comes from the coordinator. It is empty unless the leg is a
/// linearizable read, and a write ignores it: Raft orders writes.
pub(super) async fn confirm_read_leg(
    state: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    read_groups: &[u64],
    budget: Duration,
) -> Result<(), TypedClusterError> {
    if read_groups.is_empty() || plan_is_write(plan) {
        return Ok(());
    }
    let budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
    confirm_local_read(state, database_id, plan, read_groups, budget_ms)
        .await
        .map_err(execution_error_to_typed)
}
