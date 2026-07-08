// SPDX-License-Identifier: BUSL-1.1

use crate::multi_raft::MultiRaft;
use crate::routing::RoutingTable;
use crate::rpc_codec::RaftRpc;
use crate::topology::{ClusterTopology, NodeInfo, NodeState};
use crate::transport::{NexarTransport, RaftRpcHandler};
use nodedb_raft::message::LogEntry;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::super::loop_core::{CommitApplier, RaftLoop};
use super::membership::{JoinDecision, decide_join};

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
