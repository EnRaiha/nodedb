// SPDX-License-Identifier: BUSL-1.1

//! Making a linearizable read safe on the node that serves it.
//!
//! A read observes every write committed before it began only when the node
//! that runs it has applied its group through a read index taken after the
//! read began. The read index comes from the group's leader: this node when it
//! leads (under its lease or a quorum round), the leader over the transport
//! otherwise. Every path that serves a strong read calls
//! [`confirm_linearizable_read`] on the serving node before it reads.

use std::time::{Duration, Instant};

use nodedb_cluster::WaitOutcome;

use crate::control::cluster::read_index::ReadIndexRefusal;
use crate::control::state::SharedState;

/// Budget for one linearizable read to get each group's read index and apply
/// through it. Several production election timeouts (150-300ms), so an
/// ordinary round trip fits and a partition is refused rather than hung on.
pub const LINEARIZABLE_READ_TIMEOUT: Duration = Duration::from_millis(750);

/// The Raft groups that hold `vshard_ids`, deduplicated. Empty on a node with
/// no routing table: without a cluster there is one copy and nothing to prove.
pub fn groups_of_vshards(
    state: &SharedState,
    vshard_ids: impl IntoIterator<Item = u32>,
) -> crate::Result<Vec<u64>> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(Vec::new());
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut groups: Vec<u64> = Vec::new();
    for vshard_id in vshard_ids {
        let group_id = routing
            .group_for_vshard(vshard_id)
            .map_err(|_| crate::Error::NoLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
            })?;
        if !groups.contains(&group_id) {
            groups.push(group_id);
        }
    }
    Ok(groups)
}

/// Every data group this node replicates, as a voter or a learner. A read that
/// fans across all local cores observes each of them. The metadata group holds
/// no user rows, so it is left out.
pub fn groups_hosted_here(state: &SharedState) -> Vec<u64> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Vec::new();
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    routing
        .group_ids()
        .into_iter()
        .filter(|&group_id| group_id != nodedb_cluster::METADATA_GROUP_ID)
        .filter(|&group_id| hosts_group(state, &routing, group_id))
        .collect()
}

/// The groups that hold `vshard_ids` and that this node replicates.
///
/// A read on this node's cores observes only groups this node replicates. A
/// group held elsewhere has no rows here and never applies here, so it is
/// left out.
pub fn hosted_groups_of_vshards(
    state: &SharedState,
    vshard_ids: impl IntoIterator<Item = u32>,
) -> crate::Result<Vec<u64>> {
    let groups = groups_of_vshards(state, vshard_ids)?;
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(groups);
    };
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    Ok(groups
        .into_iter()
        .filter(|&group_id| hosts_group(state, &routing, group_id))
        .collect())
}

fn hosts_group(state: &SharedState, routing: &nodedb_cluster::RoutingTable, group_id: u64) -> bool {
    routing.group_info(group_id).is_some_and(|info| {
        info.members.contains(&state.node_id) || info.learners.contains(&state.node_id)
    })
}

/// The deadline a linearizable read in the running statement gets: never past
/// what is left of the statement's budget.
pub fn statement_read_deadline(state: &SharedState) -> Instant {
    let remaining_ms = crate::control::server::shared::session::statement_deadline_ms(
        state.tuning.network.default_deadline_secs,
    );
    linearizable_read_deadline(Duration::from_millis(remaining_ms))
}

/// The deadline a linearizable read gets, never past `statement_budget`.
pub fn linearizable_read_deadline(statement_budget: Duration) -> Instant {
    Instant::now() + LINEARIZABLE_READ_TIMEOUT.min(statement_budget)
}

/// Make a linearizable read of `groups` safe to serve on this node.
///
/// Each group gets a read index taken after this call, then this node waits
/// until it has applied the group through that index. The groups run
/// concurrently under one `deadline`.
pub async fn confirm_linearizable_read(
    state: &SharedState,
    groups: &[u64],
    deadline: Instant,
) -> crate::Result<()> {
    if groups.is_empty() {
        return Ok(());
    }
    let Some(gate) = state.raft_read_gate.get() else {
        // A routing table without a gate: `start_raft` has not published it.
        return Err(refused(groups[0], "raft is not serving on this node yet"));
    };
    let reads = groups.iter().map(|&group_id| async move {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let read_index =
            gate.read_index(group_id, remaining)
                .await
                .map_err(|refusal| match refusal {
                    ReadIndexRefusal::NotLeader => {
                        refused(group_id, "no leader confirmed a read index")
                    }
                    ReadIndexRefusal::Timeout { waited_ms } => refused(
                        group_id,
                        &format!("no quorum confirmed a read index within {waited_ms}ms"),
                    ),
                })?;
        wait_applied_through(state, group_id, read_index, deadline).await
    });
    futures::future::try_join_all(reads).await.map(|_| ())
}

/// Wait until this node has applied `group_id` through `read_index`, or
/// refuse at `deadline`.
pub(crate) async fn wait_applied_through(
    state: &SharedState,
    group_id: u64,
    read_index: u64,
    deadline: Instant,
) -> crate::Result<()> {
    let watcher = state.applied_index_watcher(group_id);
    if watcher.current() >= read_index {
        return Ok(());
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    // The watcher parks its caller on a condition variable, so the wait runs
    // on the blocking pool.
    let outcome = tokio::task::spawn_blocking(move || watcher.wait_for(read_index, remaining))
        .await
        .map_err(|e| refused(group_id, &format!("the apply wait did not finish: {e}")))?;
    match outcome {
        WaitOutcome::Reached => Ok(()),
        WaitOutcome::TimedOut => Err(refused(
            group_id,
            &format!("this node did not apply through read index {read_index} in time"),
        )),
        WaitOutcome::GroupGone => Err(refused(group_id, "the group left this node")),
    }
}

fn refused(group_id: u64, detail: &str) -> crate::Error {
    crate::Error::LinearizableReadRefused {
        group_id,
        detail: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{LINEARIZABLE_READ_TIMEOUT, linearizable_read_deadline};

    #[test]
    fn the_deadline_never_exceeds_the_statement_budget() {
        let short = Duration::from_millis(10);
        let before = std::time::Instant::now();
        let deadline = linearizable_read_deadline(short);
        assert!(deadline <= std::time::Instant::now() + short);
        assert!(deadline >= before + short);

        let long = LINEARIZABLE_READ_TIMEOUT * 10;
        let deadline = linearizable_read_deadline(long);
        assert!(deadline <= std::time::Instant::now() + LINEARIZABLE_READ_TIMEOUT);
    }
}
