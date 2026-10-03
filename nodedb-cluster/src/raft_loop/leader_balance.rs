// SPDX-License-Identifier: BUSL-1.1

//! Leader balance: each data group's leader hands leadership to the group's
//! preferred leader (see [`crate::rebalancer::leader_preference`]).
//!
//! The bootstrap node leads every group, and nothing in Raft moves a leader
//! that stays healthy. This phase moves every group's leadership to its
//! preferred leader, so a formed cluster spreads its leaders over the voters.
//!
//! A leader transfers when all of these hold:
//! - this node is not leaving the group (the step-aside phase moves that
//!   leader to a placement voter);
//! - the preferred leader is another current voter of the group;
//! - it was healthy for [`PREFERRED_HEALTHY_PASSES`] balance passes in a
//!   row, this pass included. Healthy means it answered a heartbeat since
//!   the previous pass and holds every committed entry.
//!
//! The streak damps failover. A preferred leader that just came back, or
//! that missed one heartbeat round, earns its groups back only after it
//! stayed healthy for the whole streak. How this node won the group plays
//! no part. A check-quorum step-down that this node wins back keeps the
//! streak: the pass after the new term counts again (see [`PeerHealth`]).
//! Every pass rechecks every group, so the balance converges whenever the
//! rules hold, however the voter set changed.

use std::collections::HashSet;

use tracing::debug;

use crate::forward::PlanExecutor;
use crate::rebalancer::preferred_leaders;

use super::loop_core::{CommitApplier, RaftLoop};

/// Ticks between leader-balance passes (about 0.5 s at the 10 ms tick). A
/// pass apart spans several heartbeats, so a live voter always answers one.
pub(super) const LEADER_BALANCE_TICK_INTERVAL: u64 = 50;

/// Consecutive healthy passes a preferred leader needs before a transfer to
/// it (about 2 s). A node that failed one pass starts the streak over.
pub(super) const PREFERRED_HEALTHY_PASSES: u32 = 4;

/// Passes a peer's record outlives its last sample. One unsampled pass
/// covers a brief loss of leadership. A longer gap starts the streak over.
pub(super) const PEER_HEALTH_GAP_PASSES: u64 = 2;

/// One pass's view of a peer from the group's leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerSample {
    /// The leader's term. Response counts restart at zero in every term.
    pub term: u64,
    /// `AppendEntries` responses from the peer in `term`.
    pub acks: u64,
    /// Whether the peer holds every committed entry.
    pub caught_up: bool,
}

/// What the balance knows of one peer of one group across passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PeerHealth {
    /// Term of the last sample.
    term: u64,
    /// Response count of the last sample.
    acks: u64,
    /// Healthy passes in a row up to the last sample.
    streak: u32,
    /// Whether the last sample itself showed the peer healthy.
    healthy_now: bool,
    /// Pass of the last sample.
    seen_pass: u64,
}

impl PeerHealth {
    /// The record after `sample`, taken at balance pass `pass`.
    ///
    /// - A sample in the same term counts the pass healthy when the response
    ///   count rose and the peer is caught up. Any other sample ends the
    ///   streak.
    /// - A sample in a new term cannot compare counts. The pass sets a new
    ///   baseline, keeps the streak and does not count as healthy.
    /// - A record older than [`PEER_HEALTH_GAP_PASSES`] starts over.
    pub(super) fn next(previous: Option<Self>, sample: PeerSample, pass: u64) -> Self {
        let recent = previous.filter(|prev| prev.is_current(pass));
        let (streak, healthy_now) = match recent {
            Some(prev) if prev.term == sample.term => {
                if sample.acks > prev.acks && sample.caught_up {
                    (prev.streak.saturating_add(1), true)
                } else {
                    (0, false)
                }
            }
            Some(prev) => (prev.streak, false),
            None => (0, false),
        };
        Self {
            term: sample.term,
            acks: sample.acks,
            streak,
            healthy_now,
            seen_pass: pass,
        }
    }

    /// Whether the record still counts at balance pass `pass`.
    pub(super) fn is_current(&self, pass: u64) -> bool {
        pass.saturating_sub(self.seen_pass) <= PEER_HEALTH_GAP_PASSES
    }

    /// Whether the peer earned leadership: healthy at the last sample, and
    /// for [`PREFERRED_HEALTHY_PASSES`] passes in a row.
    pub(super) fn earned_leadership(&self) -> bool {
        self.healthy_now && self.streak >= PREFERRED_HEALTHY_PASSES
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Transfer each data group this node leads to its preferred leader,
    /// when the rules above allow it. `pass` numbers the balance passes and
    /// rises by one each pass.
    pub(super) fn balance_leadership(&self, pass: u64) {
        self.tick_state.forget_stale_peer_health(pass);
        let transfers: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let leading: Vec<u64> = mr
                .group_ids()
                .into_iter()
                .filter(|gid| is_data_group(*gid) && mr.group_role_is_leader(*gid))
                .collect();
            let (preferred, leaving): (_, HashSet<u64>) = {
                let routing = mr.routing();
                let routing = routing.read().unwrap_or_else(|p| p.into_inner());
                let leaving = leading
                    .iter()
                    .copied()
                    .filter(|gid| {
                        routing
                            .group_info(*gid)
                            .and_then(|info| info.placement.as_ref())
                            .is_some_and(|placement| !placement.contains(&self.node_id))
                    })
                    .collect();
                (preferred_leaders(&routing), leaving)
            };
            let mut out = Vec::new();
            for gid in leading {
                let Some(&target) = preferred.get(&gid) else {
                    continue;
                };
                if target == self.node_id || leaving.contains(&gid) {
                    continue;
                }
                let is_voter = mr
                    .group_membership(gid)
                    .is_some_and(|m| m.voters.contains(&target));
                let (Some(term), Some(acks)) =
                    (mr.leader_term(gid), mr.peer_ack_count(gid, target))
                else {
                    continue;
                };
                let sample = PeerSample {
                    term,
                    acks,
                    caught_up: mr.peer_caught_up(gid, target),
                };
                // Sampled on every pass, so the streak is known once it is long enough.
                let health = self
                    .tick_state
                    .observe_peer_health(gid, target, sample, pass);
                if is_voter && health.earned_leadership() {
                    out.push((gid, target));
                }
            }
            out
        };

        for (group_id, target) in transfers {
            let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.transfer_leadership(group_id, target) {
                Ok(()) => debug!(
                    group_id,
                    target, "leader balance: transferring to the preferred leader"
                ),
                Err(e) => debug!(
                    group_id,
                    target,
                    error = %e,
                    "leader balance: transfer refused"
                ),
            }
        }
    }
}

/// Whether `group_id` is a data group: neither the metadata group nor the
/// sequencer group.
fn is_data_group(group_id: u64) -> bool {
    group_id != crate::metadata_group::METADATA_GROUP_ID
        && group_id != crate::calvin::sequencer::SEQUENCER_GROUP_ID
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(term: u64, acks: u64, caught_up: bool) -> PeerSample {
        PeerSample {
            term,
            acks,
            caught_up,
        }
    }

    /// Feed `samples` to a fresh record, one per pass from `first_pass`.
    fn run(first_pass: u64, samples: &[Option<PeerSample>]) -> Vec<Option<PeerHealth>> {
        let mut record: Option<PeerHealth> = None;
        let mut out = Vec::new();
        for (offset, sample) in samples.iter().enumerate() {
            let pass = first_pass + offset as u64;
            if let Some(sample) = sample {
                record = Some(PeerHealth::next(record, *sample, pass));
            }
            out.push(record.filter(|r| r.is_current(pass)));
        }
        out
    }

    #[test]
    fn a_steadily_healthy_peer_earns_leadership_after_the_streak() {
        let samples: Vec<_> = (0..=u64::from(PREFERRED_HEALTHY_PASSES))
            .map(|i| Some(sample(1, 10 + i, true)))
            .collect();
        let records = run(0, &samples);
        let earned: Vec<bool> = records
            .iter()
            .map(|r| r.is_some_and(|r| r.earned_leadership()))
            .collect();
        let last = earned.len() - 1;
        assert!(earned[..last].iter().all(|e| !e), "{earned:?}");
        assert!(earned[last], "{earned:?}");
    }

    /// The leader steps down on check-quorum and wins the group back every
    /// few passes. The preferred voter answers throughout, so the streak
    /// survives each new term and the transfer still happens.
    #[test]
    fn a_check_quorum_flap_among_live_voters_does_not_block_the_transfer() {
        let mut samples = Vec::new();
        let mut term = 1;
        let mut acks = 0;
        let mut earned_at = None;
        for pass in 0..40u64 {
            // Every third pass this node is a follower, then wins a new term.
            if pass % 3 == 2 {
                samples.push(None);
                term += 1;
                acks = 0;
                continue;
            }
            acks += 5;
            samples.push(Some(sample(term, acks, true)));
            let records = run(0, &samples);
            if records
                .last()
                .copied()
                .flatten()
                .is_some_and(|r| r.earned_leadership())
            {
                earned_at = Some(pass);
                break;
            }
        }
        let earned_at = earned_at.expect("flapping leadership blocked the transfer");
        assert!(earned_at < 12, "earned at pass {earned_at}");
    }

    /// A preferred leader that stopped answering for one pass is not handed
    /// leadership until it answered a full streak again.
    #[test]
    fn a_recently_failed_peer_waits_for_a_full_streak() {
        let streak = u64::from(PREFERRED_HEALTHY_PASSES);
        let mut samples: Vec<_> = (0..=streak)
            .map(|i| Some(sample(1, 10 + i, true)))
            .collect();
        // The peer misses a heartbeat round: its count does not rise.
        let stalled = 10 + streak;
        samples.push(Some(sample(1, stalled, true)));
        for i in 1..=streak {
            samples.push(Some(sample(1, stalled + i, true)));
        }
        let records = run(0, &samples);
        let failed_at = streak as usize + 1;
        assert!(records[failed_at - 1].is_some_and(|r| r.earned_leadership()));
        for (offset, record) in records[failed_at..].iter().enumerate() {
            let earned = record.is_some_and(|r| r.earned_leadership());
            assert_eq!(
                earned,
                offset as u64 == streak,
                "pass {} after the failure",
                offset
            );
        }
    }

    /// A peer that is not caught up breaks the streak like a missed answer.
    #[test]
    fn a_lagging_peer_starts_the_streak_over() {
        let first = PeerHealth::next(None, sample(1, 1, true), 0);
        let healthy = PeerHealth::next(Some(first), sample(1, 2, true), 1);
        assert_eq!(healthy.streak, 1);
        let lagging = PeerHealth::next(Some(healthy), sample(1, 3, false), 2);
        assert_eq!(lagging.streak, 0);
        assert!(!lagging.healthy_now);
    }

    /// A record unsampled for longer than the gap starts over, so a node
    /// that left and came back earns its streak again.
    #[test]
    fn a_stale_record_starts_over() {
        let mut record = PeerHealth::next(None, sample(1, 0, true), 0);
        for pass in 1..=u64::from(PREFERRED_HEALTHY_PASSES) {
            record = PeerHealth::next(Some(record), sample(1, pass, true), pass);
        }
        assert!(record.earned_leadership());
        let late = u64::from(PREFERRED_HEALTHY_PASSES) + PEER_HEALTH_GAP_PASSES + 1;
        assert!(!record.is_current(late));
        let fresh = PeerHealth::next(Some(record), sample(2, 50, true), late);
        assert_eq!(fresh.streak, 0);
        assert!(!fresh.earned_leadership());
    }

    /// A new term sets a baseline: it keeps the streak but never transfers
    /// on a sample whose count it cannot compare.
    #[test]
    fn a_new_term_keeps_the_streak_without_counting_the_pass() {
        let mut record = PeerHealth::next(None, sample(1, 0, true), 0);
        for pass in 1..=u64::from(PREFERRED_HEALTHY_PASSES) {
            record = PeerHealth::next(Some(record), sample(1, pass, true), pass);
        }
        let pass = u64::from(PREFERRED_HEALTHY_PASSES) + 2;
        let baseline = PeerHealth::next(Some(record), sample(2, 1, true), pass);
        assert_eq!(baseline.streak, record.streak);
        assert!(!baseline.earned_leadership());
        let next = PeerHealth::next(Some(baseline), sample(2, 3, true), pass + 1);
        assert!(next.earned_leadership());
    }
}
