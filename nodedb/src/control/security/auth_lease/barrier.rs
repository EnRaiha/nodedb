// SPDX-License-Identifier: BUSL-1.1

//! The writer's side: hold an authorization change's acknowledgement until
//! no node can plan against the state before it.
//!
//! The metadata leader holds the barrier until every node with an unexpired
//! lease covered the targets, or its lease expired. The writing node is a
//! lease holder too, so its own Event Plane lag closes the same way. A
//! single-node cluster runs the same barrier against its one lease.

use std::time::{Duration, Instant};

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::{AuthBarrierOutcome, AuthBarrierRequest, GroupCoverage, RaftRpc};

use crate::control::security::auth_fence::cluster::behind;
use crate::control::state::SharedState;

use super::leadership::{metadata_leader, send_to_leader};

/// Hold the acknowledgement of a change until it binds every node.
///
/// `targets` name the change: per group, the index it committed at. The
/// change itself is committed; an error means only that the barrier did not
/// release within the request deadline.
///
/// Latency: in a cluster, every authorization-bearing write waits about one
/// renewal interval (the Raft heartbeat) before it is acknowledged. Each
/// holder reports its coverage only with its next renewal. This covers every
/// DDL that bears authorization, `CREATE COLLECTION` included. A holder that
/// cannot renew adds up to one lease duration (the election timeout), until
/// its lease expires. A pinned holder, the leader that is the only voter of
/// the metadata group, has no expiry: the barrier waits for its next renewal
/// however late it runs.
pub async fn authorization_barrier(
    state: &SharedState,
    targets: Vec<GroupCoverage>,
) -> crate::Result<()> {
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    let timing = lease_timing(state)?;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(committed_but_pending(format!(
                "the barrier did not release within {deadline_secs}s"
            )));
        }
        let request = AuthBarrierRequest {
            targets: targets.clone(),
            timeout_ms: u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX),
        };
        let outcome = match metadata_leader(state).filter(|(leader, _)| *leader != 0) {
            None => None,
            Some((leader_id, _)) if leader_id == state.node_id => {
                match state.authorization_fence.leader() {
                    Some(service) => Some(service.hold_barrier(request).await.outcome),
                    None => None,
                }
            }
            Some((leader_id, _)) => match send_to_leader(
                state,
                leader_id,
                RaftRpc::AuthBarrierRequest(request),
                remaining + timing.lease,
            )
            .await
            {
                Ok(RaftRpc::AuthBarrierResponse(response)) => Some(response.outcome),
                Ok(other) => {
                    tracing::warn!(
                        leader_id,
                        "authorization barrier: unexpected reply {other:?}"
                    );
                    None
                }
                Err(error) => {
                    tracing::debug!(%error, "authorization barrier: not delivered");
                    None
                }
            },
        };
        match outcome {
            Some(AuthBarrierOutcome::Released) => return Ok(()),
            // No leader known, a leader change, or a lost message: ask the
            // leader again. A new leader holds the barrier to its own floors.
            Some(AuthBarrierOutcome::NotLeader { .. })
            | Some(AuthBarrierOutcome::Timeout { .. })
            | None => tokio::time::sleep(timing.renew_every).await,
        }
    }
}

/// Hold the acknowledgement of a Calvin transaction that wrote a
/// permission-tree source.
///
/// Its completion acks sit in the sequencer log, at or below the sequencer
/// group's commit index now. A node covers that index only once its own
/// replicas applied every acknowledged transaction below it.
pub async fn calvin_write_barrier(state: &SharedState) -> crate::Result<()> {
    lease_timing(state)?;
    let commit_index = state
        .raft_status_fn
        .get()
        .and_then(|status| {
            status()
                .into_iter()
                .find(|group| group.group_id == SEQUENCER_GROUP_ID)
                .map(|group| group.commit_index)
        })
        .ok_or_else(|| committed_but_pending("this node does not replicate the sequencer group"))?;
    authorization_barrier(
        state,
        vec![GroupCoverage {
            group_id: SEQUENCER_GROUP_ID,
            through: commit_index,
        }],
    )
    .await
}

/// The lease timing `start_raft` installs.
pub(crate) fn lease_timing(state: &SharedState) -> crate::Result<super::LeaseTiming> {
    state
        .authorization_fence
        .timing()
        .ok_or(crate::Error::Internal {
            detail: "the authorization lease is not installed: start_raft has not run on this \
                     node"
                .to_owned(),
        })
}

fn committed_but_pending(detail: impl std::fmt::Display) -> crate::Error {
    behind(format!(
        "the authorization change is committed, but {detail}; nodes still planning against \
         the previous state refuse until they catch up"
    ))
}
