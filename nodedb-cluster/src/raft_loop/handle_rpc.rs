// SPDX-License-Identifier: BUSL-1.1

//! Inbound Raft RPC dispatch — `impl RaftRpcHandler for RaftLoop`.
//!
//! Each RPC variant is either handled inline (Raft consensus RPCs that
//! just lock `MultiRaft`) or delegated to a helper module — health,
//! forwarding, VShard envelopes, or (for `JoinRequest`) the async
//! orchestration in [`super::join`].

use crate::error::{ClusterError, Result};
use crate::forward::{ChunkSink, PlanExecutor};
use crate::health;
use crate::rpc_codec::{
    AssignSurrogateRequest, AssignSurrogateResponse, ExecuteRequest, RaftRpc,
    ShuffleAggregateConsumeRequest, ShuffleAggregateConsumeResponse, ShuffleConsumeRequest,
    ShuffleConsumeResponse, ShuffleProduceRequest, ShufflePushRequest, TypedClusterError,
};
use crate::transport::RaftRpcHandler;

use super::loop_core::{CommitApplier, RaftLoop};

/// The Raft group that owns cluster topology / membership.
///
/// Group 0 is the "metadata" group and is the authoritative source of
/// truth for who is in the cluster. Joins must be processed by its
/// leader; this constant is also used by the join orchestration in
/// [`super::join`].
pub(super) const TOPOLOGY_GROUP_ID: u64 = 0;

/// Outcome of the leader-check phase of the join flow.
///
/// Extracted as a pure enum so the decision logic can be unit-tested
/// without spinning up a real `MultiRaft` just to observe its leader id.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum JoinDecision {
    /// This node is the group-0 leader (or the founding seed with no leader
    /// elected yet). Admit the join locally.
    Admit,
    /// Another node is the group-0 leader. The client should retry at
    /// `leader_addr`.
    Redirect { leader_addr: String },
}

/// Pure decision: given the observed group-0 leader, this node's id, and
/// the leader's address (as known to the local topology), should we
/// admit the join or redirect?
///
/// - `group0_leader == 0` means "no elected leader yet". On a freshly
///   bootstrapped single-seed cluster this is normal — the founding node
///   is the only possible leader, so we accept.
/// - `group0_leader == self_node_id` means we are the leader — accept.
/// - Otherwise redirect. If the leader's address is unknown to topology
///   (an operator error that shouldn't happen in practice), we still
///   redirect with an empty string so the client at least sees the
///   `"not leader"` prefix and can decide to try the next seed.
pub(super) fn decide_join(
    group0_leader: u64,
    self_node_id: u64,
    leader_addr: Option<String>,
) -> JoinDecision {
    if group0_leader == 0 || group0_leader == self_node_id {
        JoinDecision::Admit
    } else {
        JoinDecision::Redirect {
            leader_addr: leader_addr.unwrap_or_default(),
        }
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftRpcHandler for RaftLoop<A, P> {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            // Raft consensus RPCs — lock MultiRaft (sync, never across await).
            RaftRpc::AppendEntriesRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_append_entries(&req)?;
                Ok(RaftRpc::AppendEntriesResponse(resp))
            }
            RaftRpc::RequestVoteRequest(req) => {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                let resp = mr.handle_request_vote(&req)?;
                Ok(RaftRpc::RequestVoteResponse(resp))
            }
            RaftRpc::InstallSnapshotRequest(req) => {
                // Validate snapshot framing for any non-empty chunk.
                // Empty data is the bootstrap stub (no engine data yet); skip
                // framing in that case. When a real engine ships data it calls
                // `encode_snapshot_chunk` on the sender side and enforcement
                // happens here automatically.
                if !req.data.is_empty() {
                    // Short-circuit immediately if this chunk has already been
                    // quarantined after two consecutive CRC failures. Without
                    // this check a quarantined chunk would re-attempt the
                    // (always-failing) decode on every incoming RPC and never
                    // surface a stable, operator-visible error.
                    if let Some(ref hook) = self.snapshot_quarantine_hook
                        && hook.is_quarantined(req.group_id, req.last_included_index)
                    {
                        return Err(crate::error::ClusterError::Codec {
                            detail: format!(
                                "InstallSnapshot chunk quarantined: group={} index={}",
                                req.group_id, req.last_included_index
                            ),
                        });
                    }

                    match nodedb_raft::decode_snapshot_chunk(&req.data) {
                        Ok(_) => {
                            // Successful decode — reset any prior strike so a
                            // single transient CRC error does not permanently
                            // count against a healthy peer.
                            if let Some(ref hook) = self.snapshot_quarantine_hook {
                                hook.record_success(req.group_id, req.last_included_index);
                            }
                        }
                        Err(e) => {
                            let is_crc_class = matches!(
                                e,
                                nodedb_raft::snapshot_framing::SnapshotFramingError::CrcMismatch {
                                    ..
                                }
                                    | nodedb_raft::snapshot_framing::SnapshotFramingError::Truncated(
                                        _
                                    )
                            );
                            if is_crc_class && let Some(ref hook) = self.snapshot_quarantine_hook {
                                hook.record_failure(
                                    req.group_id,
                                    req.last_included_index,
                                    &e.to_string(),
                                );
                            }
                            return Err(crate::error::ClusterError::Codec {
                                detail: format!("InstallSnapshot framing: {e}"),
                            });
                        }
                    }
                }

                let last_included_index = req.last_included_index;
                let group_id = req.group_id;

                // Route through the chunk accumulator when a data directory is
                // configured. The accumulator writes chunks to a `.partial` file,
                // validates the full CRC on the final chunk, and then calls
                // `mr.handle_install_snapshot` after atomic rename.
                //
                // When `data_dir` is `None` (unit tests that don't set a data
                // directory) fall through to the original direct call so test
                // coverage for Raft state-machine transitions is unaffected.
                //
                // Quarantine accounting for offset regression and CRC errors is
                // preserved: the `SnapshotOffsetRegression` and
                // `SnapshotCrcMismatch` error paths in the receiver both surface
                // as `ClusterError` variants that are propagated here.
                if let Some(ref data_dir) = self.data_dir {
                    match crate::install_snapshot::receiver::handle_chunk(
                        &req,
                        &self.partial_snapshots,
                        data_dir,
                        &self.multi_raft,
                    )
                    .await
                    {
                        Ok(crate::install_snapshot::ChunkOutcome::Committed(snap_resp)) => {
                            // Final chunk committed — bump watcher for metadata group.
                            if group_id == TOPOLOGY_GROUP_ID {
                                self.group_watchers.bump(group_id, last_included_index);
                            }
                            return Ok(RaftRpc::InstallSnapshotResponse(snap_resp));
                        }
                        Ok(crate::install_snapshot::ChunkOutcome::Pending) => {
                            // Non-final chunk — pass a done=false stub to MultiRaft so
                            // it resets its election timeout and returns the current term.
                            let pending_req = nodedb_raft::InstallSnapshotRequest {
                                term: req.term,
                                leader_id: req.leader_id,
                                last_included_index: req.last_included_index,
                                last_included_term: req.last_included_term,
                                offset: req.offset,
                                data: vec![],
                                done: false,
                                group_id,
                                total_size: 0,
                            };
                            let resp = {
                                let mut mr =
                                    self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                                mr.handle_install_snapshot(&pending_req)?
                            };
                            return Ok(RaftRpc::InstallSnapshotResponse(resp));
                        }
                        Err(e @ crate::error::ClusterError::SnapshotOffsetRegression { .. }) => {
                            // Record the regression as a quarantine strike so the
                            // sender knows to retransmit from offset 0.
                            if let Some(ref hook) = self.snapshot_quarantine_hook {
                                hook.record_failure(group_id, last_included_index, &e.to_string());
                            }
                            // Reset partial state so the next offset-0 chunk starts fresh.
                            self.partial_snapshots
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .remove(&group_id);
                            return Err(e);
                        }
                        Err(e @ crate::error::ClusterError::SnapshotCrcMismatch { .. }) => {
                            if let Some(ref hook) = self.snapshot_quarantine_hook {
                                hook.record_failure(group_id, last_included_index, &e.to_string());
                            }
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }

                // Fallback: no data_dir — direct call (unit test path).
                let resp = {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    mr.handle_install_snapshot(&req)?
                };
                // Watcher contract: `applied_index` means "state visible
                // on this node up to index N", NOT "raft has advanced to
                // N". Bumping the watcher must therefore mirror actual
                // state-machine progress.
                //
                // - Metadata group: `mr.handle_install_snapshot` restores
                //   the metadata state machine synchronously before
                //   returning, so the watcher can be bumped here — state
                //   IS visible at `last_included_index`.
                //
                // - Data groups: snapshot install fast-forwards raft's
                //   `last_applied` but does NOT restore the data-plane
                //   state machine (no committed entries are produced for
                //   `run_apply_loop`, and there is currently no
                //   data-group state-machine snapshot restore path).
                //   Bumping the watcher here would wake waiters that
                //   then read missing state — silent data-loss-shaped
                //   bug. The data-group watcher is bumped only by the
                //   host crate's apply loop after the SPSC round-trip
                //   completes; that path is the single source of truth
                //   for "state visible".
                //
                // When data-group state-machine snapshots are
                // implemented, the restore path must bump the watcher
                // itself — not this handler.
                if group_id == TOPOLOGY_GROUP_ID {
                    self.group_watchers.bump(group_id, last_included_index);
                }
                Ok(RaftRpc::InstallSnapshotResponse(resp))
            }
            // Cluster join — full orchestration in `super::join`.
            RaftRpc::JoinRequest(req) => Ok(RaftRpc::JoinResponse(self.join_flow(req).await)),
            // Health check.
            RaftRpc::Ping(req) => {
                let topo_version = {
                    let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
                    topo.version()
                };
                Ok(health::handle_ping(self.node_id, topo_version, &req))
            }
            // Topology broadcast.
            RaftRpc::TopologyUpdate(update) => {
                let (updated, ack) =
                    health::handle_topology_update(self.node_id, &self.topology, &update);
                if updated {
                    // Register every member's address with the transport
                    // so raft RPCs to newly-learned peers actually have
                    // a destination. Without this, a node that joined
                    // early and then learned about a later joiner via
                    // broadcast would hold a stale peer set in its
                    // transport and AppendEntries to the new peer would
                    // fail until the circuit breaker opened permanently.
                    for node in &update.nodes {
                        if node.node_id == self.node_id {
                            continue;
                        }
                        match node.addr.parse::<std::net::SocketAddr>() {
                            Ok(addr) => self.transport.register_peer(node.node_id, addr),
                            Err(e) => tracing::warn!(
                                node_id = node.node_id,
                                addr = %node.addr,
                                error = %e,
                                "topology update contains unparseable peer address; skipping register_peer"
                            ),
                        }
                    }
                    // Persist the adopted topology so a subsequent
                    // restart reads the latest member set from catalog
                    // rather than the stale snapshot taken at join
                    // time. Persist only when a catalog is attached;
                    // failures are logged but never propagate — the
                    // next TopologyUpdate will retry.
                    if let Some(catalog) = self.catalog.as_ref() {
                        let snap = self
                            .topology
                            .read()
                            .unwrap_or_else(|p| p.into_inner())
                            .clone();
                        if let Err(e) = catalog.save_topology(&snap) {
                            tracing::warn!(error = %e, "failed to persist topology update to catalog");
                        }
                    }
                }
                Ok(ack)
            }
            // Physical-plan execution (C-β) — execute locally via the PlanExecutor,
            // skipping SQL re-planning entirely.
            RaftRpc::ExecuteRequest(req) => {
                let resp = self.plan_executor.execute_plan(req).await;
                Ok(RaftRpc::ExecuteResponse(resp))
            }
            // Metadata-group proposal forwarding — apply locally if
            // we're the metadata leader, otherwise return a
            // NotLeader response with a leader hint so the
            // forwarder can chase the redirect.
            RaftRpc::MetadataProposeRequest(req) => {
                let resp = match self.propose_to_metadata_group(req.bytes) {
                    Ok(log_index) => crate::rpc_codec::MetadataProposeResponse::ok(log_index),
                    Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                        leader_hint,
                    })) => {
                        crate::rpc_codec::MetadataProposeResponse::err("not leader", leader_hint)
                    }
                    Err(e) => crate::rpc_codec::MetadataProposeResponse::err(e.to_string(), None),
                };
                Ok(RaftRpc::MetadataProposeResponse(resp))
            }
            // Data-group proposal forwarding — apply locally if we are the
            // data-group leader for the given vshard, otherwise return
            // NotLeader with a hint so the forwarder can chase the redirect.
            RaftRpc::DataProposeRequest(req) => {
                let resp = {
                    let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                    match mr.propose(req.vshard_id, req.bytes) {
                        Ok((group_id, log_index)) => {
                            crate::rpc_codec::DataProposeResponse::ok(group_id, log_index)
                        }
                        Err(crate::error::ClusterError::Raft(
                            nodedb_raft::RaftError::NotLeader { leader_hint },
                        )) => crate::rpc_codec::DataProposeResponse::err("not leader", leader_hint),
                        Err(e) => crate::rpc_codec::DataProposeResponse::err(e.to_string(), None),
                    }
                };
                Ok(RaftRpc::DataProposeResponse(resp))
            }
            // VShardEnvelope — dispatch to registered handler (Event Plane, etc.).
            RaftRpc::VShardEnvelope(bytes) => {
                if let Some(ref handler) = self.vshard_handler {
                    let response_bytes = handler(bytes).await?;
                    Ok(RaftRpc::VShardEnvelope(response_bytes))
                } else {
                    Err(ClusterError::Transport {
                        detail: "VShardEnvelope handler not configured".into(),
                    })
                }
            }
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request type in RPC handler: {other:?}"),
            }),
        }
    }

    // Streaming physical-plan execution (L4) — delegate to the PlanExecutor's
    // streaming path. The transport drives the multi-frame chunk/end envelope
    // writes; this just runs the plan and feeds `sink`.
    async fn handle_rpc_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        self.plan_executor.execute_plan_streaming(req, sink).await
    }

    // Cross-node streaming shuffle (E1) — delegate to the host-crate
    // `ShuffleReceiver` (backed by `nodedb`'s `ShuffleReceiverRegistry`). When
    // no receiver is installed (cluster-only tests / single-node), the request
    // is a no-op and a chunk surfaces a typed "not configured" error.
    async fn on_shuffle_request(&self, req: ShufflePushRequest) {
        if let Some(recv) = &self.shuffle_receiver {
            recv.on_shuffle_request(req.shuffle_id, req.part, req.side, req.producer_count)
                .await;
        }
    }

    async fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> Result<()> {
        match &self.shuffle_receiver {
            Some(recv) => recv.on_shuffle_chunk(shuffle_id, part, side, payload).await,
            None => Err(ClusterError::Transport {
                detail: "shuffle receiver not configured (no ShuffleReceiver installed)".into(),
            }),
        }
    }

    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        if let Some(recv) = &self.shuffle_receiver {
            recv.on_shuffle_end(shuffle_id, part, side, error).await;
        }
    }

    // Cross-node shuffle PRODUCER trigger (E4a) — delegate to the host-crate
    // `ShuffleProducer`. When no producer is installed (cluster-only tests /
    // single-node), return a typed "not configured" error so the coordinator
    // learns the trigger could not run rather than silently succeeding.
    async fn on_shuffle_produce(&self, req: ShuffleProduceRequest) -> Option<TypedClusterError> {
        match &self.shuffle_producer {
            Some(producer) => producer.on_shuffle_produce(req).await,
            None => Some(TypedClusterError::Internal {
                code: 0,
                message: "shuffle producer not configured (no ShuffleProducer installed)".into(),
            }),
        }
    }

    // Cross-node shuffle CONSUMER trigger (E4b) — delegate to the host-crate
    // `ShuffleConsumer`. When no consumer is installed (cluster-only tests /
    // single-node), return a typed "not configured" error (empty rows) so the
    // coordinator learns the trigger could not run rather than silently
    // receiving zero join rows.
    async fn on_shuffle_consume(&self, req: ShuffleConsumeRequest) -> ShuffleConsumeResponse {
        match &self.shuffle_consumer {
            Some(consumer) => consumer.on_shuffle_consume(req).await,
            None => ShuffleConsumeResponse {
                rows: Vec::new(),
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "shuffle consumer not configured (no ShuffleConsumer installed)"
                        .into(),
                }),
            },
        }
    }

    // Cross-node distributed GROUP BY shuffle CONSUMER trigger (E5b) — delegate
    // to the host-crate `ShuffleAggregator`. The single-sided aggregate sibling
    // of `on_shuffle_consume`. When no aggregator is installed (cluster-only
    // tests / single-node), return a typed "not configured" error (empty rows)
    // so the coordinator learns the trigger could not run rather than silently
    // receiving zero aggregate rows.
    async fn on_shuffle_aggregate(
        &self,
        req: ShuffleAggregateConsumeRequest,
    ) -> ShuffleAggregateConsumeResponse {
        match &self.shuffle_aggregator {
            Some(aggregator) => aggregator.on_shuffle_aggregate(req).await,
            None => ShuffleAggregateConsumeResponse {
                rows: Vec::new(),
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "shuffle aggregator not configured (no ShuffleAggregator installed)"
                        .into(),
                }),
            },
        }
    }

    // Routed-surrogate-exchange (F1b) — delegate to the host-crate
    // `AssignRemoteSurrogate`. This node is the home vShard's leader; a LOCAL
    // assign through it yields the authoritative surrogate. When no assigner is
    // installed (cluster-only tests / single-node), return a typed "not
    // configured" error (surrogate 0) so the coordinator learns the request could
    // not run rather than silently receiving a bogus zero surrogate.
    async fn on_assign_surrogate(&self, req: AssignSurrogateRequest) -> AssignSurrogateResponse {
        match &self.assign_remote_surrogate {
            Some(assigner) => assigner.on_assign_surrogate(req).await,
            None => AssignSurrogateResponse {
                surrogate: 0,
                error: Some(TypedClusterError::Internal {
                    code: 0,
                    message: "assign-remote-surrogate not configured \
                              (no AssignRemoteSurrogate installed)"
                        .into(),
                }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_raft::MultiRaft;
    use crate::routing::RoutingTable;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};
    use crate::transport::NexarTransport;
    use nodedb_raft::message::LogEntry;
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    /// No-op applier for tests that don't care about state machine output.
    struct NoopApplier;
    impl CommitApplier for NoopApplier {
        fn apply_committed(&self, _group_id: u64, entries: &[LogEntry]) -> u64 {
            entries.last().map(|e| e.index).unwrap_or(0)
        }
    }

    fn make_transport(node_id: u64) -> Arc<NexarTransport> {
        Arc::new(
            NexarTransport::new(
                node_id,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn rpc_handler_routes_append_entries() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::AppendEntriesRequest(nodedb_raft::AppendEntriesRequest {
            term: 99,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::AppendEntriesResponse(r) => {
                assert!(r.success);
                assert_eq!(r.term, 99);
            }
            other => panic!("expected AppendEntriesResponse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rpc_handler_routes_request_vote() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        let rt = RoutingTable::uniform(1, &[1, 2, 3], 3);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![2, 3]).unwrap();

        let topo = Arc::new(RwLock::new(ClusterTopology::new()));
        let raft_loop = RaftLoop::new(mr, transport, topo, NoopApplier);

        let req = RaftRpc::RequestVoteRequest(nodedb_raft::RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 0,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::RequestVoteResponse(r) => {
                assert!(r.vote_granted);
                assert_eq!(r.term, 1);
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }
    }

    /// JoinRequest on a freshly-bootstrapped single-seed RaftLoop is
    /// admitted locally: this node is leader of every group, so
    /// `AddLearner` conf-changes are proposed and (because the groups
    /// are single-voter) commit instantly.
    #[tokio::test]
    async fn rpc_handler_accepts_join_on_bootstrap_seed() {
        let dir = tempfile::tempdir().unwrap();
        let transport = make_transport(1);
        // uniform(2, ...) creates metadata group 0 + data groups 1 and 2.
        let rt = RoutingTable::uniform(2, &[1], 1);
        let mut mr = MultiRaft::new(1, rt, dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();
        mr.add_group(1, vec![]).unwrap();
        mr.add_group(2, vec![]).unwrap();
        // Force immediate election so both groups reach Leader before
        // the join flow proposes AddLearner.
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }

        let mut topology = ClusterTopology::new();
        topology.add_node(NodeInfo::new(
            1,
            "127.0.0.1:9400".parse().unwrap(),
            NodeState::Active,
        ));
        let topo = Arc::new(RwLock::new(topology));

        let raft_loop = RaftLoop::new(mr, transport, topo.clone(), NoopApplier);
        raft_loop.do_tick();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let req = RaftRpc::JoinRequest(crate::rpc_codec::JoinRequest {
            node_id: 2,
            listen_addr: "127.0.0.1:9401".into(),
            wire_version: crate::topology::CLUSTER_WIRE_FORMAT_VERSION,
            spiffe_id: None,
            spki_pin: None,
        });

        let resp = raft_loop.handle_rpc(req).await.unwrap();
        match resp {
            RaftRpc::JoinResponse(r) => {
                assert!(
                    r.success,
                    "join should succeed on bootstrap seed: {}",
                    r.error
                );
                assert_eq!(r.nodes.len(), 2);
                // uniform(2, ...) creates 3 groups (metadata + 2 data).
                assert_eq!(r.groups.len(), 3);
                assert_eq!(r.vshard_to_group.len(), 1024);
                // The new node should appear as a learner on every group,
                // not as a voter — voter promotion happens asynchronously
                // via the tick loop's promotion phase.
                for g in &r.groups {
                    assert!(
                        g.learners.contains(&2),
                        "expected node 2 as learner in group {}, got learners={:?} members={:?}",
                        g.group_id,
                        g.learners,
                        g.members
                    );
                }
            }
            other => panic!("expected JoinResponse, got {other:?}"),
        }

        let topo_guard = topo.read().unwrap();
        assert_eq!(topo_guard.node_count(), 2);
        assert!(topo_guard.contains(2));
    }

    #[test]
    fn decide_join_self_leader_admits() {
        assert_eq!(
            decide_join(7, 7, Some("10.0.0.7:9400".into())),
            JoinDecision::Admit
        );
    }

    #[test]
    fn decide_join_no_leader_yet_admits() {
        assert_eq!(decide_join(0, 7, None), JoinDecision::Admit);
    }

    #[test]
    fn decide_join_other_leader_redirects() {
        assert_eq!(
            decide_join(1, 7, Some("10.0.0.1:9400".into())),
            JoinDecision::Redirect {
                leader_addr: "10.0.0.1:9400".into()
            }
        );
    }

    #[test]
    fn decide_join_other_leader_unknown_addr_still_redirects() {
        assert_eq!(
            decide_join(1, 7, None),
            JoinDecision::Redirect {
                leader_addr: String::new()
            }
        );
    }
}
