// SPDX-License-Identifier: BUSL-1.1

//! Serving a read on a group's leader under its leader lease.
//!
//! A read answered by a node that lost leadership can miss writes a newer
//! leader committed. A node answers a leased read of a group only while it
//! holds the group's leader lease, and only once it has applied the group
//! through the lease read index. The lease lapses before any other node can
//! win an election, so no newer leader has committed anything the read
//! misses. A group whose lease this node does not hold is refused, with the
//! leader and term its routing table names.

use std::time::Instant;

use crate::control::state::SharedState;

use super::linearizable_read::wait_applied_through;

/// A group this node does not serve a leased read of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRefusal {
    pub group_id: u64,
    /// The leader this node's routing table names, `0` when it names none or
    /// names this node.
    pub leader_node: u64,
    /// The term `leader_node` is known at, `0` when unknown.
    pub leader_term: u64,
}

/// Make a read of `groups` safe to serve on this node under its leader
/// leases.
///
/// Returns the groups whose lease this node does not hold. Every other group
/// is applied here through its lease read index before this returns. A node
/// with no routing table serves every group: without a cluster there is one
/// copy and nothing to prove.
pub async fn confirm_leased_read(
    state: &SharedState,
    groups: &[u64],
    deadline: Instant,
) -> crate::Result<Vec<LeaseRefusal>> {
    let Some(routing) = state.cluster_routing.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(gate) = state.raft_read_gate.get() else {
        // A routing table without a gate: `start_raft` has not published it.
        return Ok(groups
            .iter()
            .map(|&group_id| LeaseRefusal {
                group_id,
                leader_node: 0,
                leader_term: 0,
            })
            .collect());
    };
    // The gate locks `MultiRaft`, so every lease is read before the routing
    // guard is taken. Holding the guard across the gate inverts the lock order.
    let leases: Vec<(u64, Option<u64>)> = groups
        .iter()
        .map(|&group_id| (group_id, gate.lease_read_index(group_id)))
        .collect();
    let mut refusals = Vec::new();
    let mut leased: Vec<(u64, u64)> = Vec::new();
    {
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        for (group_id, lease) in leases {
            match lease {
                Some(read_index) => leased.push((group_id, read_index)),
                None => {
                    let (leader, term) = routing
                        .group_info(group_id)
                        .map(|info| (info.leader, info.leader_term))
                        .unwrap_or((0, 0));
                    refusals.push(LeaseRefusal {
                        group_id,
                        leader_node: if leader == state.node_id { 0 } else { leader },
                        leader_term: term,
                    });
                }
            }
        }
    }
    let waits = leased
        .into_iter()
        .map(|(group_id, read_index)| wait_applied_through(state, group_id, read_index, deadline));
    futures::future::try_join_all(waits).await?;
    Ok(refusals)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use async_trait::async_trait;
    use nodedb_cluster::RoutingTable;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::cluster::read_index::{RaftReadGate, ReadIndexRefusal};
    use crate::wal::WalManager;

    /// A gate that holds no lease. Each lease question probes the routing lock.
    ///
    /// The production gate locks `MultiRaft`, which re-reads routing under that lock.
    /// `try_write` fails while any routing read guard is held, so a failed probe
    /// proves the caller asked the gate under a routing guard.
    struct ProbingGate {
        routing: Arc<RwLock<RoutingTable>>,
        held_guard: AtomicBool,
    }

    impl ProbingGate {
        fn probe(&self) {
            if self.routing.try_write().is_err() {
                self.held_guard.store(true, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl RaftReadGate for ProbingGate {
        async fn confirm_leader(
            &self,
            _group_id: u64,
            _timeout: Duration,
        ) -> Result<u64, ReadIndexRefusal> {
            self.probe();
            Err(ReadIndexRefusal::NotLeader)
        }

        fn within_staleness_bound(&self, _group_id: u64, _max_staleness: Duration) -> bool {
            self.probe();
            false
        }

        fn holds_leader_lease(&self, _group_id: u64) -> bool {
            self.probe();
            false
        }

        fn leader_lease_term(&self, _group_id: u64) -> Option<u64> {
            self.probe();
            None
        }

        fn lease_read_index(&self, _group_id: u64) -> Option<u64> {
            self.probe();
            None
        }
    }

    /// A leased read asks the gate for every lease before it takes the routing guard.
    ///
    /// Holding the guard across the gate inverts the `MultiRaft` then routing order.
    /// A queued routing writer then deadlocks the node.
    #[tokio::test]
    async fn leases_are_read_with_no_routing_guard_held() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("leased.wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        let routing = Arc::new(RwLock::new(RoutingTable::uniform(2, &[1, 2], 2)));
        Arc::get_mut(&mut state)
            .expect("sole owner of fresh state")
            .cluster_routing = Some(Arc::clone(&routing));
        let gate = Arc::new(ProbingGate {
            routing: Arc::clone(&routing),
            held_guard: AtomicBool::new(false),
        });
        assert!(
            state
                .raft_read_gate
                .set(Arc::clone(&gate) as Arc<dyn RaftReadGate>)
                .is_ok()
        );
        let groups = routing.read().expect("routing lock").group_ids();

        let refusals = confirm_leased_read(&state, &groups, Instant::now())
            .await
            .expect("a refused lease is not an error");
        assert!(
            !gate.held_guard.load(Ordering::SeqCst),
            "a lease was read under a routing guard"
        );
        assert_eq!(refusals.len(), groups.len(), "no lease is held here");
    }
}
