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
//! - it answered a heartbeat since the previous pass, so it is live;
//! - it holds every committed entry, so it can win at once;
//! - this node did not win the group by a contested election less than
//!   [`FAILOVER_HOLD_TIMEOUTS`] election timeouts ago.
//!
//! The last rule damps failover. A leader that won an election against
//! other voters, after a crash or a partition, keeps the group for a while,
//! so a node that just came back does not take its groups back at once. A
//! bootstrap self-election and a transfer target's win are not contested,
//! so formation converges without a wait. Every pass rechecks every group,
//! so the balance converges whenever the rules hold, however the voter set
//! changed.

use std::collections::HashSet;
use std::time::Instant;

use tracing::debug;

use crate::forward::PlanExecutor;
use crate::rebalancer::preferred_leaders;

use super::loop_core::{CommitApplier, RaftLoop};

/// Ticks between leader-balance passes (about 0.5 s at the 10 ms tick). A
/// pass apart spans several heartbeats, so a live voter always answers one.
pub(super) const LEADER_BALANCE_TICK_INTERVAL: u64 = 50;

/// Election timeouts a contested win holds its group before the balance
/// may move it.
pub(super) const FAILOVER_HOLD_TIMEOUTS: u32 = 10;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Record when this node started to lead each data group it leads, and
    /// forget the groups it no longer leads. Runs every tick.
    pub(super) fn note_led_groups(&self) {
        let now = Instant::now();
        let leading: HashSet<u64> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.group_ids()
                .into_iter()
                .filter(|gid| is_data_group(*gid) && mr.group_role_is_leader(*gid))
                .collect()
        };
        self.tick_state.retain_led(&leading);
        for gid in leading {
            self.tick_state.note_led(gid, now);
        }
    }

    /// Transfer the leadership of every data group this node leads, and
    /// should not, to the group's preferred leader.
    pub(super) fn balance_leadership(&self) {
        let now = Instant::now();
        let transfers: Vec<(u64, u64)> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let hold = mr.election_timeout_max() * FAILOVER_HOLD_TIMEOUTS;
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
                let Some(acks) = mr.peer_ack_count(gid, target) else {
                    continue;
                };
                // Sampled on every pass, so liveness is known when the hold ends.
                let live = self
                    .tick_state
                    .peer_answered_since_last_pass(gid, target, acks);
                let held =
                    mr.leads_by_contested_election(gid) && self.tick_state.led_for(gid, now) < hold;
                if is_voter && live && !held && mr.peer_caught_up(gid, target) {
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
