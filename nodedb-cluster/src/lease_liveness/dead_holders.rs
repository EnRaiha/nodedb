// SPDX-License-Identifier: BUSL-1.1

//! SWIM Dead records and the lease-liveness rule built on them.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use nodedb_types::{MAX_CLOCK_SKEW_NS, NodeId};

use crate::multi_raft::PeerAckSample;
use crate::swim::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

use super::raft_contact::RaftContactClock;

/// Longest a holder uses a lease without metadata-leader contact. It is twice
/// the default 5 s maximum election timeout, so a healthy failover never trips it.
pub const LEASE_SELF_FENCE_WINDOW: Duration = Duration::from_secs(10);

/// Largest clock offset tolerated between the node stamping a lease expiry and
/// the node reading it.
pub const LEASE_CLOCK_SKEW: Duration = Duration::from_nanos(MAX_CLOCK_SKEW_NS);

/// Wait after SWIM marks a holder Dead before its leases can count as expired:
/// the holder's self-fence window plus the clock-skew margin.
pub const DEAD_HOLDER_LEASE_GRACE: Duration =
    LEASE_SELF_FENCE_WINDOW.saturating_add(LEASE_CLOCK_SKEW);

/// Raft silence the metadata leader must see from a Dead holder before its
/// leases count as expired. Past it, the holder has either lost leader contact
/// for its self-fence window or still hears the leader and applies the release.
pub const DEAD_HOLDER_RAFT_SILENCE: Duration =
    LEASE_SELF_FENCE_WINDOW.saturating_add(LEASE_CLOCK_SKEW);

/// Cap on tracked Dead holders. A holder past the cap keeps its full lease
/// expiry, which is the safe fallback.
const MAX_TRACKED_DEAD_HOLDERS: usize = 4096;

/// The instant a lease-liveness check is evaluated at.
#[derive(Debug, Clone, Copy)]
pub struct LeaseNow {
    /// Local wall time, the frame `expires_at` is stamped in.
    pub wall_ns: u64,
    /// Monotonic time, the frame the dead grace is measured in.
    pub instant: Instant,
    /// This node's metadata-group term while it leads the group, else `None`.
    /// Only the leader can release a Dead holder's lease early.
    pub metadata_leader_term: Option<u64>,
}

/// When each lease holder went SWIM-Dead, plus the metadata leader's view of
/// each holder's Raft contact.
#[derive(Debug, Default)]
pub struct LeaseHolderLiveness {
    dead_since: Mutex<HashMap<u64, Instant>>,
    raft_contact: RaftContactClock,
}

impl LeaseHolderLiveness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `node_id` as Dead since `at`. An earlier record is kept.
    pub fn record_dead_at(&self, node_id: u64, at: Instant) {
        let mut map = self.dead_since.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = map.get_mut(&node_id) {
            if at < *existing {
                *existing = at;
            }
            return;
        }
        if map.len() >= MAX_TRACKED_DEAD_HOLDERS {
            tracing::warn!(
                node_id,
                cap = MAX_TRACKED_DEAD_HOLDERS,
                "lease holder liveness: dead-holder map full; holder keeps full lease expiry"
            );
            return;
        }
        map.insert(node_id, at);
    }

    /// Clear the Dead record of `node_id` after SWIM sees it Alive again.
    pub fn record_alive(&self, node_id: u64) {
        self.dead_since
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&node_id);
    }

    /// When `node_id` was declared Dead, if it still is.
    pub fn dead_since(&self, node_id: u64) -> Option<Instant> {
        self.dead_since
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&node_id)
            .copied()
    }

    /// Whether `node_id` has been Dead for at least [`DEAD_HOLDER_LEASE_GRACE`] at `now`.
    pub fn dead_grace_elapsed(&self, node_id: u64, now: Instant) -> bool {
        self.dead_since(node_id)
            .is_some_and(|since| now.saturating_duration_since(since) >= DEAD_HOLDER_LEASE_GRACE)
    }

    /// Whether any holder's dead grace has elapsed at `now`.
    pub fn any_dead_grace_elapsed(&self, now: Instant) -> bool {
        self.dead_since
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .any(|since| now.saturating_duration_since(*since) >= DEAD_HOLDER_LEASE_GRACE)
    }

    /// Drop the Dead records of every node `keep` rejects.
    pub fn retain(&self, keep: impl Fn(u64) -> bool) {
        self.dead_since
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|id, _| keep(*id));
    }

    /// Fold in the metadata leader's per-peer response counts, sampled at `now`.
    pub fn observe_raft_contact(&self, sample: &PeerAckSample, now: Instant) {
        self.raft_contact.observe(sample, now);
    }

    /// Forget Raft contact samples. Called when this node stops leading the
    /// metadata group.
    pub fn clear_raft_contact(&self) {
        self.raft_contact.clear();
    }

    /// Whether the leases of `holder` count as released: SWIM has held it Dead
    /// past the grace, and the metadata leader, in `leader_term`, has seen no
    /// Raft response from it for [`DEAD_HOLDER_RAFT_SILENCE`].
    pub fn dead_holder_released(
        &self,
        holder: u64,
        leader_term: Option<u64>,
        now: Instant,
    ) -> bool {
        let Some(term) = leader_term else {
            return false;
        };
        self.dead_grace_elapsed(holder, now)
            && self
                .raft_contact
                .silent_for(holder, term, DEAD_HOLDER_RAFT_SILENCE)
    }

    /// Whether a lease held by `holder` still blocks a drain.
    ///
    /// This node's own lease expires at `expires_at`. Another node's lease
    /// stays live until [`MAX_CLOCK_SKEW_NS`] past it, unless
    /// [`Self::dead_holder_released`] holds first.
    pub fn lease_is_live(
        &self,
        holder: u64,
        local_node: u64,
        expires_at_wall_ns: u64,
        now: &LeaseNow,
    ) -> bool {
        if holder == local_node {
            return expires_at_wall_ns > now.wall_ns;
        }
        if self.dead_holder_released(holder, now.metadata_leader_term, now.instant) {
            return false;
        }
        expires_at_wall_ns.saturating_add(MAX_CLOCK_SKEW_NS) > now.wall_ns
    }
}

impl MembershipSubscriber for LeaseHolderLiveness {
    fn on_state_change(&self, node_id: &NodeId, _old: Option<MemberState>, new: MemberState) {
        // Seed placeholders (`seed:<addr>`) carry no numeric id and hold no lease.
        let Ok(numeric_id) = node_id.as_str().parse::<u64>() else {
            return;
        };
        match new {
            MemberState::Dead | MemberState::Left => {
                self.record_dead_at(numeric_id, Instant::now())
            }
            MemberState::Alive => self.record_alive(numeric_id),
            MemberState::Suspect => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOCAL: u64 = 1;
    const REMOTE: u64 = 2;
    const TERM: u64 = 3;
    const SECOND_NS: u64 = 1_000_000_000;
    const NOW_WALL: u64 = 100 * SECOND_NS;

    fn node(id: u64) -> NodeId {
        NodeId::try_new(id.to_string()).expect("numeric node id")
    }

    fn past(ago: Duration) -> Instant {
        Instant::now()
            .checked_sub(ago)
            .expect("monotonic clock far enough from its origin")
    }

    fn leader_now(instant: Instant) -> LeaseNow {
        LeaseNow {
            wall_ns: NOW_WALL,
            instant,
            metadata_leader_term: Some(TERM),
        }
    }

    /// Leader samples showing `REMOTE` silent since `from` until `to`.
    fn silent_between(liveness: &LeaseHolderLiveness, from: Instant, to: Instant) {
        let sample = PeerAckSample {
            term: TERM,
            acks: vec![(REMOTE, 9)],
        };
        liveness.observe_raft_contact(&sample, from);
        liveness.observe_raft_contact(&sample, to);
    }

    #[test]
    fn self_fence_window_exceeds_the_default_election_timeout() {
        let max_election = Duration::from_millis(
            nodedb_types::config::tuning::ClusterTransportTuning::default()
                .effective_election_timeout_max_ms(),
        );
        assert!(LEASE_SELF_FENCE_WINDOW > max_election);
    }

    #[test]
    fn remote_lease_is_live_inside_the_skew_margin() {
        let liveness = LeaseHolderLiveness::new();
        let now = leader_now(Instant::now());
        let expires = NOW_WALL - SECOND_NS;
        assert!(liveness.lease_is_live(REMOTE, LOCAL, expires, &now));
        assert!(!liveness.lease_is_live(LOCAL, LOCAL, expires, &now));
        let far_past = NOW_WALL - MAX_CLOCK_SKEW_NS - SECOND_NS;
        assert!(!liveness.lease_is_live(REMOTE, LOCAL, far_past, &now));
    }

    #[test]
    fn dead_and_raft_silent_holder_is_released_only_after_the_grace() {
        let liveness = LeaseHolderLiveness::new();
        let expires = NOW_WALL + 60 * SECOND_NS;
        let now = Instant::now();
        silent_between(
            &liveness,
            past(DEAD_HOLDER_RAFT_SILENCE + Duration::from_secs(1)),
            now,
        );

        liveness.record_dead_at(REMOTE, now);
        assert!(liveness.lease_is_live(REMOTE, LOCAL, expires, &leader_now(now)));

        liveness.record_dead_at(
            REMOTE,
            past(DEAD_HOLDER_LEASE_GRACE + Duration::from_secs(1)),
        );
        assert!(!liveness.lease_is_live(REMOTE, LOCAL, expires, &leader_now(now)));
    }

    /// SWIM says Dead, but the leader heard from the holder over Raft
    /// recently: the holder has not fenced, so its lease must stay live.
    #[test]
    fn dead_by_swim_but_recently_acked_by_raft_keeps_the_lease() {
        let liveness = LeaseHolderLiveness::new();
        let expires = NOW_WALL + 60 * SECOND_NS;
        let now = Instant::now();
        liveness.record_dead_at(
            REMOTE,
            past(DEAD_HOLDER_LEASE_GRACE + Duration::from_secs(1)),
        );
        let earlier = past(DEAD_HOLDER_RAFT_SILENCE + Duration::from_secs(1));
        liveness.observe_raft_contact(
            &PeerAckSample {
                term: TERM,
                acks: vec![(REMOTE, 9)],
            },
            earlier,
        );
        liveness.observe_raft_contact(
            &PeerAckSample {
                term: TERM,
                acks: vec![(REMOTE, 10)],
            },
            now,
        );

        assert!(liveness.lease_is_live(REMOTE, LOCAL, expires, &leader_now(now)));
    }

    /// A drainer that does not lead the metadata group never releases early.
    #[test]
    fn a_non_leader_keeps_a_dead_holders_lease_live() {
        let liveness = LeaseHolderLiveness::new();
        let expires = NOW_WALL + 60 * SECOND_NS;
        let now = Instant::now();
        silent_between(
            &liveness,
            past(DEAD_HOLDER_RAFT_SILENCE + Duration::from_secs(1)),
            now,
        );
        liveness.record_dead_at(
            REMOTE,
            past(DEAD_HOLDER_LEASE_GRACE + Duration::from_secs(1)),
        );
        let follower_now = LeaseNow {
            metadata_leader_term: None,
            ..leader_now(now)
        };
        assert!(liveness.lease_is_live(REMOTE, LOCAL, expires, &follower_now));
    }

    #[test]
    fn a_later_dead_report_keeps_the_earlier_record() {
        let liveness = LeaseHolderLiveness::new();
        let earlier = past(Duration::from_secs(30));
        liveness.record_dead_at(REMOTE, earlier);
        liveness.record_dead_at(REMOTE, Instant::now());
        assert_eq!(liveness.dead_since(REMOTE), Some(earlier));
    }

    #[test]
    fn subscriber_records_dead_and_alive_refutation_clears() {
        let liveness = LeaseHolderLiveness::new();
        liveness.on_state_change(&node(REMOTE), Some(MemberState::Suspect), MemberState::Dead);
        assert!(liveness.dead_since(REMOTE).is_some());

        liveness.on_state_change(&node(REMOTE), Some(MemberState::Dead), MemberState::Suspect);
        assert!(liveness.dead_since(REMOTE).is_some());

        liveness.on_state_change(&node(REMOTE), Some(MemberState::Dead), MemberState::Alive);
        assert!(liveness.dead_since(REMOTE).is_none());
    }

    #[test]
    fn subscriber_ignores_seed_placeholders() {
        let liveness = LeaseHolderLiveness::new();
        let seed = NodeId::try_new("seed:127.0.0.1:9000").expect("seed placeholder id");
        liveness.on_state_change(&seed, Some(MemberState::Alive), MemberState::Dead);
        assert!(liveness.dead_since.lock().expect("unpoisoned").is_empty());
    }

    #[test]
    fn map_is_bounded() {
        let liveness = LeaseHolderLiveness::new();
        for id in 0..(MAX_TRACKED_DEAD_HOLDERS as u64 + 10) {
            liveness.record_dead_at(id, Instant::now());
        }
        assert_eq!(
            liveness.dead_since.lock().expect("unpoisoned").len(),
            MAX_TRACKED_DEAD_HOLDERS
        );
    }

    #[test]
    fn retain_drops_rejected_holders() {
        let liveness = LeaseHolderLiveness::new();
        liveness.record_dead_at(REMOTE, Instant::now());
        liveness.record_dead_at(3, Instant::now());
        liveness.retain(|id| id == REMOTE);
        assert!(liveness.dead_since(REMOTE).is_some());
        assert!(liveness.dead_since(3).is_none());
    }
}
