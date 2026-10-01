// SPDX-License-Identifier: BUSL-1.1

//! The linearizable half of dispatching one gateway route.
//!
//! A route that is a leg of a linearizable read confirms its group on the node
//! that serves it: here before a local read, on the remote node before a
//! forwarded one (the route's groups ride on the `ExecuteRequest`, and the
//! receiver calls [`confirm_local_read`]).

use std::time::Duration;

use crate::Error;
use nodedb_physical::physical_plan::{PhysicalPlan, plan_contains_cluster_partitioned_leaf};

use crate::control::cluster::linearizable_read::{
    confirm_linearizable_read, groups_of_vshards, linearizable_read_deadline,
};
use crate::control::server::graph_dispatch::graph_read_groups;
use crate::control::server::shared::write_admission::plan_is_write;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::route::TaskRoute;

/// The groups a route's read confirms where it is served: the group of the
/// route's vShard for a linearizable read, none for a write or a weaker read.
pub(super) fn linearizable_read_groups(
    shared: &SharedState,
    route: &TaskRoute,
    linearizable: bool,
) -> Result<Vec<u64>, Error> {
    if !linearizable || plan_is_write(&route.plan) {
        return Ok(Vec::new());
    }
    groups_of_vshards(shared, [route.vshard_id])
}

/// Confirm a linearizable read on this node before `plan` reads locally,
/// within the statement's remaining `deadline_ms`. Nothing to confirm when
/// `read_groups` is empty.
///
/// A graph or array plan fans across every local core, so it confirms the
/// groups the plan reads there (`graph_dispatch::read_groups`) rather than the
/// route's one group.
pub(crate) async fn confirm_local_read(
    shared: &SharedState,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    read_groups: &[u64],
    deadline_ms: u64,
) -> Result<(), Error> {
    if read_groups.is_empty() {
        return Ok(());
    }
    let deadline = linearizable_read_deadline(Duration::from_millis(deadline_ms));
    if plan_contains_cluster_partitioned_leaf(plan) {
        let groups = graph_read_groups(shared, database_id, plan)?;
        return confirm_linearizable_read(shared, &groups, deadline).await;
    }
    confirm_linearizable_read(shared, read_groups, deadline).await
}
