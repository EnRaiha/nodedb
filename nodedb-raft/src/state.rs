// SPDX-License-Identifier: BUSL-1.1

/// Persistent state that must survive restarts.
///
/// Corresponds to Raft paper Figure 2 "Persistent state on all servers".
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct HardState {
    /// Latest term this server has seen.
    pub current_term: u64,
    /// Candidate that received vote in current term (0 = none).
    pub voted_for: u64,
}

impl HardState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: 0,
        }
    }
}

impl Default for HardState {
    fn default() -> Self {
        Self::new()
    }
}

/// Role of a Raft node within a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
    /// Non-voting member catching up: a new node joins as learner first.
    Learner,
    /// Cross-cluster observer: receives log entries from a source cluster as a
    /// non-voting, non-quorum member. Unlike a `Learner`, an observer never
    /// transitions to `Voter` within this group — it permanently observes.
    /// Acks are advisory and never gate commit on the source.
    Observer,
}

/// Role of a remote peer as seen by the local leader.
///
/// Used to classify tracked peers so the leader can send entries to both
/// voter peers and observer peers without including observers in quorum math.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    /// Full voting member that participates in leader election and the commit
    /// quorum.
    Voter,
    /// Cross-cluster observer that receives entries and acks advisorily.
    /// Never counted in quorum; slow-apply does not stall source commits.
    Observer,
}

/// Volatile state on all servers.
#[derive(Debug, Clone)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed.
    pub commit_index: u64,
    /// Index of highest log entry applied to state machine.
    pub last_applied: u64,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
        }
    }

    /// Volatile state for a node restarting from a durable applied index.
    ///
    /// Seeding `last_applied` stops the restarted node re-delivering entries
    /// whose state-machine effects are already durable. Re-applying an
    /// append-shaped entry (a spatial/columnar row insert, a memtable append)
    /// duplicates it, so the floor must survive the restart even though
    /// `commit_index` legitimately starts at 0 and is re-learned from the
    /// leader.
    pub fn restored(applied_index: u64) -> Self {
        Self {
            commit_index: 0,
            last_applied: applied_index,
        }
    }
}

impl Default for VolatileState {
    fn default() -> Self {
        Self::new()
    }
}

/// Volatile leader-side state for an in-progress leadership transfer.
///
/// Tracks the target voter, whether the `TimeoutNow` trigger has already been
/// emitted (so it is sent at most once), and the deadline after which the
/// transfer is abandoned and the leader resumes normal operation. This state
/// is never persisted: a leader crash mid-transfer simply yields a clean
/// follower on restart.
#[derive(Debug, Clone)]
pub struct LeadershipTransfer {
    /// The voter peer that leadership is being transferred to.
    pub target: u64,
    /// Whether the `TimeoutNow` trigger has already been emitted to `target`.
    pub emitted: bool,
    /// When the transfer is abandoned if leadership has not yet moved.
    pub deadline: std::time::Instant,
}

/// Per-observer send state tracked by the leader.
///
/// Observers have an independent bounded send queue. When the queue is full
/// because the observer is slow, new entries are dropped from the queue and
/// the observer falls into snapshot-recovery mode on reconnect. Source commits
/// are never delayed by observer apply pace.
#[derive(Debug, Clone)]
pub struct ObserverState {
    /// Index of the next log entry to send to this observer.
    pub next_index: u64,
    /// Index of the highest log entry the observer has acked (advisory).
    pub match_index: u64,
    /// Number of entries currently queued for this observer (advisory
    /// backpressure tracking). Does not gate commit.
    pub pending_count: u32,
}

impl ObserverState {
    /// Maximum number of in-flight entries queued for an observer before the
    /// leader stops pushing and waits for the observer to drain. Once the
    /// observer drains below this threshold, replication resumes. Source
    /// commits are never affected.
    pub const MAX_PENDING: u32 = 256;
}

/// Volatile state on leaders (reinitialized after election).
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each voter peer: index of next log entry to send.
    pub next_index: Vec<(u64, u64)>,
    /// For each voter/learner peer: index of highest log entry known to be replicated.
    pub match_index: Vec<(u64, u64)>,
    /// Per-observer send state. Observers are tracked separately from voters
    /// and learners so quorum math never accidentally includes them.
    pub observer_states: Vec<(u64, ObserverState)>,
}

impl LeaderState {
    /// Create leader state for the given voter/learner peers plus observers.
    pub fn new(peers: &[u64], observers: &[u64], last_log_index: u64) -> Self {
        Self {
            next_index: peers.iter().map(|&id| (id, last_log_index + 1)).collect(),
            match_index: peers.iter().map(|&id| (id, 0)).collect(),
            observer_states: observers
                .iter()
                .map(|&id| {
                    (
                        id,
                        ObserverState {
                            next_index: last_log_index + 1,
                            match_index: 0,
                            pending_count: 0,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn next_index_for(&self, peer: u64) -> u64 {
        self.next_index
            .iter()
            .find(|&&(id, _)| id == peer)
            .map(|&(_, idx)| idx)
            .unwrap_or(1)
    }

    pub fn set_next_index(&mut self, peer: u64, index: u64) {
        if let Some(entry) = self.next_index.iter_mut().find(|e| e.0 == peer) {
            entry.1 = index;
        }
    }

    pub fn match_index_for(&self, peer: u64) -> u64 {
        self.match_index
            .iter()
            .find(|&&(id, _)| id == peer)
            .map(|&(_, idx)| idx)
            .unwrap_or(0)
    }

    pub fn set_match_index(&mut self, peer: u64, index: u64) {
        if let Some(entry) = self.match_index.iter_mut().find(|e| e.0 == peer) {
            entry.1 = index;
        }
    }

    /// Add a new voter/learner peer to leader tracking.
    pub fn add_peer(&mut self, peer: u64, last_log_index: u64) {
        if !self.next_index.iter().any(|&(id, _)| id == peer) {
            self.next_index.push((peer, last_log_index + 1));
            self.match_index.push((peer, 0));
        }
    }

    /// Remove a voter/learner peer from leader tracking.
    pub fn remove_peer(&mut self, peer: u64) {
        self.next_index.retain(|&(id, _)| id != peer);
        self.match_index.retain(|&(id, _)| id != peer);
    }

    /// Current voter/learner peer list tracked by this leader state.
    pub fn peers(&self) -> Vec<u64> {
        self.next_index.iter().map(|&(id, _)| id).collect()
    }

    /// Add an observer to leader tracking.
    pub fn add_observer(&mut self, observer: u64, last_log_index: u64) {
        if !self.observer_states.iter().any(|&(id, _)| id == observer) {
            self.observer_states.push((
                observer,
                ObserverState {
                    next_index: last_log_index + 1,
                    match_index: 0,
                    pending_count: 0,
                },
            ));
        }
    }

    /// Remove an observer from leader tracking.
    pub fn remove_observer(&mut self, observer: u64) {
        self.observer_states.retain(|&(id, _)| id != observer);
    }

    /// Get a mutable reference to an observer's state.
    pub fn observer_state_mut(&mut self, observer: u64) -> Option<&mut ObserverState> {
        self.observer_states
            .iter_mut()
            .find(|(id, _)| *id == observer)
            .map(|(_, state)| state)
    }

    /// Whether an observer's send queue is below the backpressure threshold.
    /// Returns `false` if the observer is unknown.
    pub fn observer_can_receive(&self, observer: u64) -> bool {
        self.observer_states
            .iter()
            .find(|(id, _)| *id == observer)
            .map(|(_, state)| state.pending_count < ObserverState::MAX_PENDING)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_default() {
        let hs = HardState::new();
        assert_eq!(hs.current_term, 0);
        assert_eq!(hs.voted_for, 0);
    }

    #[test]
    fn leader_state_initialization() {
        let peers = vec![2, 3, 4];
        let ls = LeaderState::new(&peers, &[], 10);
        assert_eq!(ls.next_index_for(2), 11);
        assert_eq!(ls.next_index_for(3), 11);
        assert_eq!(ls.match_index_for(2), 0);
    }

    #[test]
    fn leader_state_update() {
        let peers = vec![2, 3];
        let mut ls = LeaderState::new(&peers, &[], 5);
        ls.set_next_index(2, 8);
        ls.set_match_index(2, 7);
        assert_eq!(ls.next_index_for(2), 8);
        assert_eq!(ls.match_index_for(2), 7);
        // Peer 3 unchanged.
        assert_eq!(ls.next_index_for(3), 6);
    }

    #[test]
    fn node_role_equality() {
        assert_eq!(NodeRole::Follower, NodeRole::Follower);
        assert_ne!(NodeRole::Follower, NodeRole::Leader);
    }
}
