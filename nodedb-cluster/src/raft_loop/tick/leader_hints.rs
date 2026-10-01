// SPDX-License-Identifier: BUSL-1.1

//! The routing table's leader hints, kept in step with this node's Raft.
//!
//! Five writers touch a group's leader hint:
//! - this node's Raft, here, with the term it saw the leader at;
//! - a leader redirect from another node, with the term that node knows the
//!   leader at, here and in the gateway;
//! - the leader probe of a group this node does not host (see
//!   [`super::leader_probe`]);
//! - the SWIM liveness hook, which clears a suspected leader and keeps the
//!   term, and fills the clear back when the node answers again;
//! - the metadata log and placement, which name a planned leader and carry
//!   no term.
//!
//! A termed hint applies above the hint's term. It also fills a hint cleared
//! at its own term, because a clear is only a suspicion: this node's Raft
//! reports a leader only while it leads or the leader's contact is fresh,
//! and a redirect names the leader the redirecting node follows. A term-less
//! hint applies only while the hint holds no term. A term-less hint never
//! replaces an observed one, and a new election's higher term always wins.

use tracing::debug;

use crate::forward::PlanExecutor;

use super::super::loop_core::{CommitApplier, RaftLoop};

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Write every hosted group's live Raft leader into the routing table,
    /// where it is newer than the hint or fills a cleared hint at its term.
    ///
    /// The Raft state is read under the `MultiRaft` lock, and the routing
    /// table is written after that lock is released. The write lock is taken
    /// only when a hint changes.
    pub(super) fn sync_leader_hints(&self) {
        let (observed, routing) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            (mr.observed_leaders(), mr.routing())
        };
        let changed: Vec<(u64, u64, u64)> = {
            let table = routing.read().unwrap_or_else(|p| p.into_inner());
            observed
                .into_iter()
                .filter(|&(group_id, leader, term)| {
                    table.leader_confirmation_is_new(group_id, leader, term)
                })
                .collect()
        };
        if changed.is_empty() {
            return;
        }
        let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
        for (group_id, leader, term) in changed {
            if table.confirm_leader(group_id, leader, term) {
                debug!(
                    node = self.node_id,
                    group_id, leader, term, "routing leader hint follows the Raft leader"
                );
            }
        }
    }

    /// Record the leader a redirect names for `group_id`, at the term the
    /// redirecting node knows it at. A redirect without a leader leaves the
    /// hint as it is. A redirect below the hint's term is stale and leaves
    /// the hint as it is. A redirect at the hint's term fills only a cleared
    /// hint. A redirect at term 0 comes from a node that holds no term for
    /// the group, and fills only a hint that holds none.
    pub(in crate::raft_loop) fn observe_redirect(
        &self,
        group_id: u64,
        leader_hint: Option<u64>,
        term: u64,
    ) {
        let Some(leader) = leader_hint else {
            return;
        };
        let routing = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .routing();
        let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
        let changed = if term == 0 {
            table.set_leader(group_id, leader)
        } else {
            table.confirm_leader(group_id, leader, term)
        };
        if changed {
            debug!(
                node = self.node_id,
                group_id, leader, term, "routing leader hint follows a leader redirect"
            );
        }
    }
}
