// SPDX-License-Identifier: BUSL-1.1

//! `InstallSnapshot` request handler.

use tracing::info;

use crate::error::Result;
use crate::message::{InstallSnapshotRequest, InstallSnapshotResponse};
use crate::node::core::RaftNode;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Handle incoming InstallSnapshot RPC (Raft paper Figure 13).
    ///
    /// Called on followers (and learners) that are too far behind for
    /// log-based catch-up. The leader sends its snapshot; the receiver
    /// replaces its log and state.
    ///
    /// The CALLER MUST have restored the snapshot into the state machine
    /// before invoking this: installing the snapshot moves the durable applied
    /// floor to `last_included_index`, which asserts those effects are durable.
    pub fn handle_install_snapshot(
        &mut self,
        req: &InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        if req.term < self.hard_state.current_term {
            return Ok(InstallSnapshotResponse {
                term: self.hard_state.current_term,
            });
        }

        if req.term > self.hard_state.current_term {
            self.become_follower(req.term);
        }

        self.leader_id = req.leader_id;
        self.reset_election_timeout();

        if req.done {
            self.adopt_snapshot_boundary(req.last_included_index, req.last_included_term)?;
        }

        Ok(InstallSnapshotResponse {
            term: self.hard_state.current_term,
        })
    }

    /// Move the log boundary, commit index, applied index, and durable floor
    /// to a snapshot the state machine already holds.
    ///
    /// No term check: a snapshot carries only committed state, so adopting it
    /// is safe whichever term sent it. Boot recovery calls this to complete an
    /// install whose state-machine apply finished before a crash. A no-op when
    /// both the log boundary and `last_applied` are at or past
    /// `last_included_index`.
    ///
    /// A snapshot at or below the log boundary but above `last_applied` still
    /// moves the applied index: it fills part of an apply gap.
    ///
    /// The CALLER MUST have restored the snapshot into the state machine
    /// first, for the same reason as [`Self::handle_install_snapshot`].
    pub fn adopt_snapshot_boundary(
        &mut self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<()> {
        let moves_boundary = last_included_index > self.log.snapshot_index();
        if !moves_boundary && last_included_index <= self.volatile.last_applied {
            return Ok(());
        }
        info!(
            node = self.config.node_id,
            group = self.config.group_id,
            snapshot_index = last_included_index,
            snapshot_term = last_included_term,
            "applying installed snapshot"
        );

        if moves_boundary {
            self.log
                .apply_snapshot(last_included_index, last_included_term)?;
        }

        if self.volatile.commit_index < last_included_index {
            self.volatile.commit_index = last_included_index;
        }
        if self.volatile.last_applied < last_included_index {
            self.volatile.last_applied = last_included_index;
        }
        // The snapshot already holds every effect at or below its index, so
        // queued entries in that range must not reach the state machine again.
        self.ready
            .committed_entries
            .retain(|entry| entry.index > last_included_index);
        if !self.has_apply_gap() {
            self.ready.committed_read_error = None;
        }
        // Move the durable floor with the snapshot boundary. The entries
        // the snapshot subsumes are gone from the log, so a restart that
        // resumed from the pre-snapshot floor would replay from an index
        // the log can no longer serve.
        self.save_durable_applied_index(last_included_index)
    }

    /// Record on the leader that `peer` installed a snapshot through
    /// `last_included_index`, so replication to it resumes after that index.
    ///
    /// Without this the leader keeps `peer`'s next index at or below its own
    /// log boundary and flags it for another snapshot on every heartbeat.
    /// No-op when this node does not lead, or `peer` already matched past
    /// the index.
    pub fn record_snapshot_installed(&mut self, peer: u64, last_included_index: u64) {
        let Some(leader) = self.leader_state.as_mut() else {
            return;
        };
        if last_included_index > leader.match_index_for(peer) {
            leader.set_match_index(peer, last_included_index);
            leader.set_next_index(peer, last_included_index + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use crate::message::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, LogEntry,
        RequestVoteResponse,
    };
    use crate::node::core::RaftNode;
    use crate::node::leader_lease::UNTRACKED_ROUND;
    use crate::state::NodeRole;
    use crate::storage::MemStorage;
    use crate::test_support::{apply_durably, force_election};

    use super::super::test_helpers::test_config;

    const TERM: u64 = 1;

    fn follower() -> RaftNode<MemStorage> {
        RaftNode::new(test_config(2, vec![1, 3]), MemStorage::new())
    }

    /// Node 1 elected leader of voters {1, 2, 3} at `TERM`.
    fn leader() -> RaftNode<MemStorage> {
        let mut node = RaftNode::new(test_config(1, vec![2, 3]), MemStorage::new());
        force_election(&mut node);
        let _ = node.take_ready();
        node.handle_request_vote_response(
            2,
            &RequestVoteResponse {
                term: TERM,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), NodeRole::Leader);
        let _ = node.take_ready();
        node
    }

    /// Deliver leader 1's entries `indices` after `prev_log_index`, committed
    /// through `leader_commit`.
    fn append(
        node: &mut RaftNode<MemStorage>,
        prev_log_index: u64,
        indices: RangeInclusive<u64>,
        leader_commit: u64,
    ) {
        let entries = indices
            .map(|index| LogEntry {
                term: TERM,
                index,
                data: b"write".to_vec(),
            })
            .collect();
        let resp = node.handle_append_entries(&AppendEntriesRequest {
            term: TERM,
            leader_id: 1,
            prev_log_index,
            prev_log_term: if prev_log_index == 0 { 0 } else { TERM },
            entries,
            leader_commit,
            group_id: 1,
            round: 0,
            replicated_floor: 0,
        });
        assert!(resp.success, "append after {prev_log_index} must succeed");
    }

    fn install(node: &mut RaftNode<MemStorage>, last_included_index: u64) {
        node.handle_install_snapshot(&InstallSnapshotRequest {
            term: TERM,
            leader_id: 1,
            last_included_index,
            last_included_term: TERM,
            offset: 0,
            data: Vec::new(),
            done: true,
            group_id: 1,
            total_size: 0,
            voters: Vec::new(),
            learners: Vec::new(),
        })
        .expect("snapshot install succeeds");
    }

    fn committed_indices(node: &mut RaftNode<MemStorage>) -> Vec<u64> {
        let ready = node.take_ready();
        assert!(
            ready.committed_read_error.is_none(),
            "delivery must not stall: {:?}",
            ready.committed_read_error
        );
        ready.committed_entries.iter().map(|e| e.index).collect()
    }

    /// Entries queued before an install that the snapshot covers are dropped.
    #[test]
    fn install_prunes_queued_entries_the_snapshot_covers() {
        let mut node = follower();
        append(&mut node, 0, 1..=5, 5);

        install(&mut node, 3);

        assert_eq!(node.last_applied(), 3);
        assert_eq!(committed_indices(&mut node), vec![4, 5]);
    }

    /// The driver takes a batch, a snapshot installs past it, and the batch
    /// finishes afterwards. Delivery resumes past the snapshot.
    #[test]
    fn interleaved_install_does_not_stall_delivery() {
        let mut node = follower();
        append(&mut node, 0, 1..=5, 5);
        let in_flight = committed_indices(&mut node);
        assert_eq!(in_flight, vec![1, 2, 3, 4, 5]);

        install(&mut node, 8);
        node.advance_applied(5);
        assert_eq!(node.last_applied(), 8);

        append(&mut node, 8, 9..=10, 10);
        assert_eq!(committed_indices(&mut node), vec![9, 10]);
    }

    /// A follower whose log was compacted past its applied index rejects at
    /// that index. The leader then needs a snapshot for it, and after the
    /// install the follower applies new entries again.
    #[test]
    fn follower_with_apply_gap_gets_a_snapshot_and_resumes() {
        let mut leader = leader();
        for _ in 0..11 {
            leader.propose(b"write".to_vec()).expect("leader accepts");
        }
        let tip = leader.last_log_index();
        leader.handle_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: TERM,
                success: true,
                last_log_index: tip,
                round: UNTRACKED_ROUND,
                needs_snapshot: false,
            },
        );
        assert_eq!(leader.commit_index(), tip);
        apply_durably(&mut leader, tip);
        let leader_snapshot = tip - 2;
        assert!(
            leader
                .compact_log_up_to(leader_snapshot)
                .expect("durable prefix compacts")
        );
        let _ = leader.take_ready();

        // Follower log compacted through 8, applied only through 4. Compaction
        // never passes `last_applied`, so the gap is forced directly.
        let mut node = follower();
        append(&mut node, 0, 1..=10, 10);
        let _ = node.take_ready();
        apply_durably(&mut node, 8);
        assert!(node.compact_log_up_to(8).expect("compacts"));
        node.volatile.last_applied = 4;
        assert!(node.has_apply_gap());

        let resp = node.handle_append_entries(&AppendEntriesRequest {
            term: TERM,
            leader_id: 1,
            prev_log_index: tip,
            prev_log_term: TERM,
            entries: Vec::new(),
            leader_commit: tip,
            group_id: 1,
            round: 0,
            replicated_floor: 0,
        });
        assert!(!resp.success);
        assert_eq!(resp.last_log_index, 4);

        leader.handle_append_entries_response(2, &resp);
        assert!(leader.take_ready().snapshots_needed.contains(&2));

        // Once the leader records the install, replication to the peer
        // resumes after the snapshot instead of flagging another one.
        leader.record_snapshot_installed(2, leader_snapshot);
        assert_eq!(leader.match_index_for(2), Some(leader_snapshot));
        leader.propose(b"after".to_vec()).expect("leader accepts");
        assert!(!leader.take_ready().snapshots_needed.contains(&2));

        install(&mut node, leader_snapshot);
        assert!(!node.has_apply_gap());
        assert_eq!(node.last_applied(), leader_snapshot);
        let _ = node.take_ready();

        append(&mut node, leader_snapshot, leader_snapshot + 1..=tip, tip);
        assert_eq!(
            committed_indices(&mut node),
            (leader_snapshot + 1..=tip).collect::<Vec<u64>>()
        );
    }
}
