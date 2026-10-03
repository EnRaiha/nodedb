// SPDX-License-Identifier: BUSL-1.1

//! Server-side `JoinRequest` orchestration.
//!
//! This is the async flow invoked by the `RaftRpc::JoinRequest` arm in
//! [`super::handle_rpc`]. It turns a remote node's desire to join the
//! cluster into a series of durable Raft conf-changes and returns a
//! `JoinResponse` containing everything the joining node needs to
//! reconstruct its local `MultiRaft` in the `Learner` role.
//!
//! ## Flow
//!
//! 1. **Leader check.** Snapshot the group-0 leader id and clone the
//!    routing table under a single `MultiRaft` lock. If another node is
//!    the leader, return a redirect response with that node's address.
//! 2. **Validate address.** Parse `req.listen_addr`. On failure, return
//!    an error response.
//! 3. **Idempotency / collision check.** If the node id is already in
//!    topology with the same address, continue and reconcile any groups
//!    missing that node (an earlier join may have failed part-way through).
//!    If the node id exists with a different address, reject.
//! 4. **Register transport peer.** Add the new peer address to the
//!    local transport so the leader can immediately send AppendEntries
//!    to the learner-to-be.
//! 5. **Admit into topology.** Under a short `topology.write()` guard,
//!    call `bootstrap::handle_join_request` — the only side effect is
//!    inserting the new `NodeInfo`. The routing-table clone we took in
//!    step 1 is intentionally *not* reused for the final response; a
//!    fresh clone is taken after step 6 so the response reflects the
//!    post-AddLearner routing state.
//! 6. **Propose AddLearner on the metadata and sequencer groups** where
//!    they do not contain the node yet. Metadata-group admission is
//!    mandatory. The sequencer group defers only once the metadata group
//!    has three voters. Data groups follow placement: reconcile, kicked in
//!    step 9, names the joiner in the placements that take it, and each
//!    group's leader adds it there.
//! 7. **Wait for each conf-change to apply.** Poll actual group membership
//!    every 20 ms with a 5-second deadline. A
//!    single-voter group (the bootstrap seed before any voters have
//!    been added) commits instantly. Multi-voter groups wait for
//!    quorum. On timeout, return an error response — the joining node
//!    will retry the whole flow.
//! 8. **Persist topology + routing to catalog** (when a catalog is
//!    attached). Order matters: Raft log → catalog → response.
//! 9. **Broadcast TopologyUpdate** to every currently-active peer so
//!    followers learn the new node's address. Fire-and-forget.
//! 10. **Build and return JoinResponse** with every group of the routing
//!     view. A group this node hosts carries its Raft membership.
//!
//! The Raft-level promotion from learner to voter happens asynchronously
//! in the tick loop (`super::tick::promote_ready_learners`) once the
//! learner's `match_index` catches up. That avoids blocking the join
//! handler on replication progress while still completing the
//! two-phase single-server add.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::bootstrap::handle_join_request;
use crate::conf_change::{ConfChange, ConfChangeType};
use crate::error::{ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::health;
use crate::multi_raft::{GroupStatus, MultiRaft};
use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinGroupInfo, JoinRequest, JoinResponse, LEADER_REDIRECT_PREFIX};

use super::handle_rpc::{JoinDecision, TOPOLOGY_GROUP_ID, decide_join};
use super::loop_core::{CommitApplier, RaftLoop};

/// Maximum time we wait for any one `AddLearner` conf-change to commit
/// before giving up and returning a failure response to the joining
/// node.
const CONF_CHANGE_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Polling interval for the commit-wait loop.
const CONF_CHANGE_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// The groups the join admits `node_id` to: the metadata group and the
/// sequencer group, where they do not contain it yet.
///
/// A data group's members follow its placement alone. Reconcile authors the
/// placement with the joiner, each group's leader adds the joiner where the
/// placement names it, and the joiner mounts those groups. A join-time
/// learner in every data group would outrun that placement: the leader drops
/// each learner the placement it holds does not name, then adds it back
/// once the new placement applies.
fn groups_requiring_admission(multi_raft: &MultiRaft, node_id: u64) -> Vec<u64> {
    multi_raft
        .group_ids()
        .into_iter()
        .filter(|group_id| {
            (*group_id == TOPOLOGY_GROUP_ID
                || *group_id == crate::calvin::sequencer::SEQUENCER_GROUP_ID)
                && !multi_raft
                    .group_contains_node(*group_id, node_id)
                    .unwrap_or(false)
        })
        .collect()
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Full server-side `JoinRequest` handler. See module docs for the
    /// phase-by-phase description.
    pub(super) async fn join_flow(&self, req: JoinRequest) -> JoinResponse {
        // 1. Snapshot group-0 leader + clone routing under one lock.
        let (group0_leader, routing): (u64, RoutingTable) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let routing = mr
                .routing()
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let leader_id = mr
                .group_statuses()
                .into_iter()
                .find(|s: &GroupStatus| s.group_id == TOPOLOGY_GROUP_ID)
                .map(|s| s.leader_id)
                .unwrap_or(0);
            (leader_id, routing)
        };

        // Leader check.
        let leader_addr_hint = if group0_leader != 0 && group0_leader != self.node_id {
            self.topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get_node(group0_leader)
                .map(|n| n.addr.clone())
        } else {
            None
        };
        if let JoinDecision::Redirect { leader_addr } =
            decide_join(group0_leader, self.node_id, leader_addr_hint)
        {
            warn!(
                joining_node = req.node_id,
                leader_id = group0_leader,
                leader_addr = %leader_addr,
                "JoinRequest received on non-leader; redirecting"
            );
            return reject(format!("{LEADER_REDIRECT_PREFIX}{leader_addr}"));
        }

        // 2. Validate the address.
        let new_addr: SocketAddr = match req.listen_addr.parse() {
            Ok(a) => a,
            Err(e) => {
                return reject(format!("invalid listen_addr '{}': {e}", req.listen_addr));
            }
        };

        // 3. Idempotency / collision check against topology.
        //    `handle_join_request` in step 5 handles the fine-grained
        //    semantics, but we check the collision case here before doing
        //    any Raft work. Same-address retries continue so a partial join
        //    can reconcile groups that were missed on the first attempt.
        let existing = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get_node(req.node_id)
            .cloned();
        if let Some(existing) = existing {
            if existing.addr != req.listen_addr {
                return reject(format!(
                    "node_id {} already registered with different address {} (request: {})",
                    req.node_id, existing.addr, req.listen_addr
                ));
            }
            // Same id + same addr may be a fully completed rejoin, or it may
            // be a retry after an earlier attempt inserted topology and then
            // failed part-way through group admission. Continue through the
            // per-group reconciliation below instead of returning early.
            debug!(
                joining_node = req.node_id,
                "idempotent re-join; reconciling group membership"
            );
        }

        // 4. Register transport peer so the leader can reach it.
        self.transport.register_peer(req.node_id, new_addr);

        // Every successful `JoinResponse` echoes the catalog's cluster id.
        // The joiner persists it, so its next boot takes `restart()`.
        // - An attached catalog with no cluster id is an invariant
        //   violation: the join is rejected, never answered with a
        //   sentinel id.
        // - With no catalog attached (unit tests), `self.node_id` stands
        //   in: test node ids are unique by construction.
        let cluster_id = match self.catalog.as_ref() {
            Some(catalog) => match catalog.load_cluster_id() {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return reject(
                        "server catalog is attached but has no cluster_id — refusing to \
                         issue a JoinResponse without a real cluster identity"
                            .to_string(),
                    );
                }
                Err(e) => {
                    return reject(format!("failed to read cluster_id from catalog: {e}"));
                }
            },
            None => self.node_id,
        };

        // 5. Admit into topology.
        {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            let initial_resp = handle_join_request(&req, &mut topo, &routing, cluster_id);
            if !initial_resp.success {
                // Reject bubbled up from the shared function (e.g., the
                // collision check we just did, repeated under the write
                // guard in case something raced).
                return initial_resp;
            }
        }

        // 6. Propose AddLearner on the metadata group and the sequencer
        //    group where they do not contain this node yet, so a retry
        //    resumes a partial join. Data groups are left to placement (see
        //    `groups_requiring_admission`). Metadata-group admission is
        //    mandatory. The sequencer group defers only once the metadata
        //    group has three voters.
        let (group_ids, metadata_voters) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let metadata_voters = mr
                .group_membership(TOPOLOGY_GROUP_ID)
                .map_or(0, |membership| membership.voters.len());
            (
                groups_requiring_admission(&mr, req.node_id),
                metadata_voters,
            )
        };
        let deferrable =
            |gid: u64| gid == crate::calvin::sequencer::SEQUENCER_GROUP_ID && metadata_voters >= 3;

        let mut pending: Vec<(u64, u64)> = Vec::with_capacity(group_ids.len()); // (group_id, log_index)
        for gid in &group_ids {
            let change = ConfChange {
                change_type: ConfChangeType::AddLearner,
                node_id: req.node_id,
            };
            let propose_result = {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.propose_conf_change(*gid, &change)
            };
            match propose_result {
                Ok((_, log_index)) => pending.push((*gid, log_index)),
                Err(ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                    leader_hint, ..
                })) if deferrable(*gid) => {
                    debug!(
                        group_id = *gid,
                        joining_node = req.node_id,
                        ?leader_hint,
                        "deferred learner admission on independently led group"
                    );
                }
                Err(ClusterError::Transport { detail })
                    if deferrable(*gid) && detail.contains("not leader") =>
                {
                    debug!(
                        group_id = *gid,
                        joining_node = req.node_id,
                        error = %detail,
                        "deferred learner admission on independently led group"
                    );
                }
                Err(ClusterError::Transport { detail }) => {
                    return reject(format!(
                        "failed to propose AddLearner on group {gid}: {detail}"
                    ));
                }
                Err(e) => {
                    return reject(format!("failed to propose AddLearner on group {gid}: {e}"));
                }
            }
        }

        // 7. Wait for every conf change to apply: the node appears in the
        //    group's Raft membership as a learner or a voter.
        let deadline = Instant::now() + CONF_CHANGE_COMMIT_TIMEOUT;
        for (gid, log_index) in &pending {
            if let Err(err) = self
                .wait_for_group_admission(*gid, req.node_id, *log_index, deadline)
                .await
            {
                return reject(err.to_string());
            }
        }

        // 8. Persist catalog (topology + post-AddLearner routing), off the
        // async threads. The routing table goes through the one routing
        // writer, so this save never lands after a newer one.
        if let Some(catalog) = self.catalog.as_ref() {
            let topo_snapshot = self
                .topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let catalog = Arc::clone(catalog);
            let saved =
                tokio::task::spawn_blocking(move || catalog.save_topology(&topo_snapshot)).await;
            let error = match saved {
                Ok(Ok(())) => None,
                Ok(Err(e)) => Some(e.to_string()),
                Err(e) => Some(format!("save task: {e}")),
            };
            if let Some(e) = error {
                warn!(error = %e, "failed to persist topology after join");
                return reject(format!("catalog save_topology failed: {e}"));
            }
        }
        if let Some(persister) = self.routing_persister.as_ref()
            && !persister.wait(persister.request()).await
        {
            warn!("failed to persist routing after join");
            return reject(
                "catalog save_routing failed; see the routing persister's warning".into(),
            );
        }

        // 9. Broadcast topology to everyone so peers learn the new addr.
        health::broadcast_topology(self.node_id, &self.topology, &self.transport);

        // Kick placement reconcile immediately once the joining node's
        // membership has applied and topology is persisted, instead of waiting
        // for the next throttled tick.
        self.reconcile_notify.notify_one();

        // 10. Build the final response from the post-AddLearner state.
        info!(
            joining_node = req.node_id,
            groups = pending.len(),
            "join accepted; learner AddLearner commits complete"
        );

        self.build_current_response(&req)
    }

    /// Wait for the semantic goal of "node is now a voter or learner in
    /// this Raft group", polling every
    /// [`CONF_CHANGE_POLL_INTERVAL`] up to `deadline`.
    ///
    /// Polling actual Raft membership rather than routing or raw commit index
    /// covers both vShard groups and internal groups such as the sequencer.
    ///
    /// `log_index` is carried into the error enum for debugging
    /// only; the condition is not gated on it.
    ///
    /// Surfaces failure through [`ClusterError::JoinCommitTimeout`]
    /// and [`ClusterError::JoinGroupDisappeared`] so the join
    /// flow can match the cause and so the crate's central
    /// error enum owns the human-readable rendering.
    async fn wait_for_group_admission(
        &self,
        group_id: u64,
        learner_id: u64,
        log_index: u64,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let applied = {
                let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.group_contains_node(group_id, learner_id)
            };
            match applied {
                Some(true) => return Ok(()),
                Some(false) => {}
                None => return Err(ClusterError::JoinGroupDisappeared { group_id }),
            }
            if Instant::now() >= deadline {
                return Err(ClusterError::JoinCommitTimeout {
                    group_id,
                    log_index,
                });
            }
            tokio::time::sleep(CONF_CHANGE_POLL_INTERVAL).await;
        }
    }

    /// Build a `JoinResponse` snapshotting the current topology
    /// and routing. Used by both new and idempotent joins after per-group
    /// reconciliation. The strict cluster_id
    /// check is the same as the one at the top of `join_flow` —
    /// a catalog-attached server with no stamped cluster_id is an
    /// invariant violation and we reject the join rather than
    /// synthesise a sentinel identity.
    fn build_current_response(&self, req: &JoinRequest) -> JoinResponse {
        let cluster_id = match self.catalog.as_ref() {
            Some(catalog) => match catalog.load_cluster_id() {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return reject(
                        "server catalog is attached but has no cluster_id — refusing to \
                         issue a JoinResponse without a real cluster identity"
                            .to_string(),
                    );
                }
                Err(e) => {
                    return reject(format!("failed to read cluster_id from catalog: {e}"));
                }
            },
            None => self.node_id,
        };

        let topology_clone = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let (routing_clone, raft_groups) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let groups: Vec<JoinGroupInfo> = mr
                .group_ids()
                .into_iter()
                .filter_map(|group_id| mr.group_membership(group_id))
                .map(|membership| JoinGroupInfo {
                    group_id: membership.group_id,
                    leader: membership.leader_id,
                    members: membership.voters,
                    learners: membership.learners,
                })
                .collect();
            let routing = mr
                .routing()
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            (routing, groups)
        };
        // Re-use the pure builder from `bootstrap/handle_join.rs`.
        // `handle_join_request` is idempotent against the same
        // (id, addr) — at this point the topology already
        // contains the new node, so this call only rebuilds the
        // wire response.
        let mut topo = topology_clone;
        let mut response = handle_join_request(req, &mut topo, &routing_clone, cluster_id);
        overlay_hosted_groups(&mut response.groups, raft_groups);
        response
    }
}

/// Replace each routing-view group in `groups` with the Raft membership this
/// node hosts for it, and add the hosted groups routing does not list.
///
/// `groups` starts as every group of this node's routing view. The joiner
/// builds its whole routing table from the response. A group missing from it
/// has no routing entry on the joiner, and nothing adds one later: the leader
/// probe and the placement apply only move entries that exist. A replication
/// factor below the node count leaves groups this node hosts no replica of,
/// so the response cannot list only the hosted groups. A hosted group's Raft
/// membership holds every conf change this node applied, so it replaces the
/// routing entry.
fn overlay_hosted_groups(groups: &mut Vec<JoinGroupInfo>, hosted: Vec<JoinGroupInfo>) {
    for group in hosted {
        match groups.iter_mut().find(|g| g.group_id == group.group_id) {
            Some(slot) => *slot = group,
            None => groups.push(group),
        }
    }
}

/// Build a failure `JoinResponse` with the given error message.
fn reject(error: String) -> JoinResponse {
    JoinResponse {
        success: false,
        error,
        cluster_id: 0,
        nodes: vec![],
        vshard_to_group: vec![],
        groups: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::SEQUENCER_GROUP_ID;
    use crate::routing::RoutingTable;

    #[test]
    fn partial_rejoin_only_reconciles_missing_group_membership() {
        let dir = tempfile::tempdir().unwrap();
        let routing = RoutingTable::uniform(1, &[1], 1);
        let mut multi_raft = MultiRaft::new(1, routing, dir.path().to_path_buf());
        multi_raft.add_group(0, vec![]).unwrap();
        multi_raft.add_group(1, vec![]).unwrap();
        multi_raft.add_group(SEQUENCER_GROUP_ID, vec![]).unwrap();

        for group_id in [0, 1] {
            multi_raft
                .apply_conf_change(
                    group_id,
                    &ConfChange {
                        change_type: ConfChangeType::AddLearner,
                        node_id: 2,
                    },
                )
                .unwrap();
        }

        assert_eq!(
            groups_requiring_admission(&multi_raft, 2),
            vec![SEQUENCER_GROUP_ID]
        );
        // A new node is admitted to the metadata and sequencer groups only.
        // Data group 1 takes it as its placement names it.
        let mut fresh = groups_requiring_admission(&multi_raft, 3);
        fresh.sort_unstable();
        assert_eq!(fresh, vec![0, SEQUENCER_GROUP_ID]);
    }

    fn info(group_id: u64, leader: u64, members: &[u64], learners: &[u64]) -> JoinGroupInfo {
        JoinGroupInfo {
            group_id,
            leader,
            members: members.to_vec(),
            learners: learners.to_vec(),
        }
    }

    #[test]
    fn the_response_keeps_the_groups_this_node_hosts_no_replica_of() {
        // Routing lists groups 0, 1 and 2. This node hosts 0, 1 and the
        // sequencer; group 2 lives on node 2 only.
        let mut groups = vec![
            info(0, 1, &[1, 2], &[]),
            info(1, 1, &[1], &[]),
            info(2, 2, &[2], &[]),
        ];
        let hosted = vec![
            info(0, 1, &[1, 2], &[3]),
            info(1, 1, &[1], &[3]),
            info(SEQUENCER_GROUP_ID, 1, &[1, 2], &[3]),
        ];
        overlay_hosted_groups(&mut groups, hosted);
        groups.sort_by_key(|g| g.group_id);
        assert_eq!(
            groups,
            vec![
                info(0, 1, &[1, 2], &[3]),
                info(1, 1, &[1], &[3]),
                info(2, 2, &[2], &[]),
                info(SEQUENCER_GROUP_ID, 1, &[1, 2], &[3]),
            ]
        );
    }
}
