// SPDX-License-Identifier: BUSL-1.1

//! The tickets of admitted writes, and the start fence a scheduler waits on.
//!
//! A scheduler that starts must not stage before every write the gate
//! admitted for its vShard earlier reached its core. A transaction that
//! stages first reads the state before the write, and its redo then
//! overwrites it. Two kinds of write lack a key on the new lock table:
//!
//! - A write admitted while no lock table existed took no key.
//! - A write that took keys on a table a retired scheduler owned holds
//!   nothing on its successor's table.
//!
//! So every admitted write holds a ticket:
//!
//! - The gate opens an [`AdmissionTicket`] before it first looks up the
//!   vShard's lock table. A ticket ends once its write reached its core, or
//!   landed in its data group's log at a known index.
//! - A scheduler takes a [`StartFence`] after its lock table is registered.
//!   A write that found no table, or a table that retired since, opened its
//!   ticket before that registration. Its ticket is earlier than the fence.
//! - The scheduler stages nothing until every ticket earlier than its fence
//!   ended, and its data group applied every index those writes landed at.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

/// How many independently locked stripes the vShards spread over.
const STRIPES: u32 = 64;

/// One vShard's admitted writes.
///
/// Open tickets are counted, not stored, so a ticket allocates nothing. Each
/// start fence begins a new generation. A ticket of an earlier generation
/// counts in `open_before`, one of the current generation in `open_now`.
#[derive(Default)]
struct VShardTickets {
    /// The generation a ticket opened now belongs to.
    generation: u64,
    /// Open tickets of the current generation.
    open_now: u64,
    /// Open tickets of every earlier generation.
    open_before: u64,
    /// Per data group, the highest log index an admitted write landed at.
    landed: BTreeMap<u64, u64>,
}

/// Every vShard's admitted writes on this node.
pub struct AdmissionTickets {
    stripes: Box<[Mutex<HashMap<u32, VShardTickets>>]>,
}

impl Default for AdmissionTickets {
    fn default() -> Self {
        Self::new()
    }
}

/// The point a starting scheduler of `vshard` waits past before it stages.
///
/// Every ticket of an earlier generation than the fence's opened before the
/// scheduler's table was registered. A later fence of the vShard counts the
/// tickets between the two as earlier too. That only waits longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartFence {
    vshard: u32,
}

/// Where a [`StartFence`] stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceState {
    /// A write earlier than the fence has not reached its core or its log.
    Open,
    /// Every write earlier than the fence reached its core or its log. The fence
    /// passes once the group applied this index. `None` needs no index.
    Through(Option<u64>),
}

/// The vShards one ticket covers, each with the generation it opened in. A
/// write on one vShard allocates nothing.
enum Homes {
    One { vshard: u32, generation: u64 },
    Many(Vec<(u32, u64)>),
    Closed,
}

impl AdmissionTickets {
    pub fn new() -> Self {
        Self {
            stripes: (0..STRIPES).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn stripe(&self, vshard: u32) -> MutexGuard<'_, HashMap<u32, VShardTickets>> {
        // `vshard % STRIPES` is below `STRIPES`, the slice's length.
        let index = (vshard % STRIPES) as usize;
        self.stripes[index]
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Count one open ticket on `vshard`. Returns its generation.
    fn open_one(&self, vshard: u32) -> u64 {
        let mut stripe = self.stripe(vshard);
        let tickets = stripe.entry(vshard).or_default();
        tickets.open_now += 1;
        tickets.generation
    }

    /// Open a ticket for a write to `vshard`.
    pub fn open(self: &Arc<Self>, vshard: u32) -> AdmissionTicket {
        let generation = self.open_one(vshard);
        AdmissionTicket {
            tickets: Arc::clone(self),
            homes: Homes::One { vshard, generation },
        }
    }

    /// Open a ticket for a write to each of `vshards`.
    pub fn open_many(self: &Arc<Self>, vshards: &[u32]) -> AdmissionTicket {
        let homes = vshards
            .iter()
            .map(|&vshard| (vshard, self.open_one(vshard)))
            .collect();
        AdmissionTicket {
            tickets: Arc::clone(self),
            homes: Homes::Many(homes),
        }
    }

    /// The fence of a scheduler of `vshard` whose lock table is registered.
    /// It starts a new generation: every ticket open now counts as earlier.
    pub fn start_fence(&self, vshard: u32) -> StartFence {
        let mut stripe = self.stripe(vshard);
        let tickets = stripe.entry(vshard).or_default();
        tickets.open_before += tickets.open_now;
        tickets.open_now = 0;
        tickets.generation += 1;
        StartFence { vshard }
    }

    /// Where `fence` stands for data group `group_id`.
    pub fn fence_state(&self, fence: StartFence, group_id: u64) -> FenceState {
        let stripe = self.stripe(fence.vshard);
        let Some(tickets) = stripe.get(&fence.vshard) else {
            return FenceState::Through(None);
        };
        if tickets.open_before > 0 {
            return FenceState::Open;
        }
        FenceState::Through(tickets.landed.get(&group_id).copied())
    }

    fn close(&self, vshard: u32, generation: u64, landed_at: Option<(u64, u64)>) {
        let mut stripe = self.stripe(vshard);
        let tickets = stripe.entry(vshard).or_default();
        // The landing records before the ticket closes: no fence reads the
        // ticket gone and its index missing.
        if let Some((group_id, log_index)) = landed_at {
            let landed = tickets.landed.entry(group_id).or_default();
            *landed = (*landed).max(log_index);
        }
        // Every open ticket is counted in one of the two, so neither drops
        // below zero.
        if generation == tickets.generation {
            tickets.open_now -= 1;
        } else {
            tickets.open_before -= 1;
        }
    }
}

/// The ticket of one admitted write. Dropping it ends it: the write reached
/// its core, or never will.
pub struct AdmissionTicket {
    tickets: Arc<AdmissionTickets>,
    homes: Homes,
}

impl AdmissionTicket {
    /// The write landed at `log_index` of `group_id`. Its ticket ends, and
    /// each starting scheduler of its vShards waits for that index to apply.
    pub fn landed(mut self, group_id: u64, log_index: u64) {
        self.close(Some((group_id, log_index)));
    }

    fn close(&mut self, landed_at: Option<(u64, u64)>) {
        match std::mem::replace(&mut self.homes, Homes::Closed) {
            Homes::One { vshard, generation } => {
                self.tickets.close(vshard, generation, landed_at);
            }
            Homes::Many(homes) => {
                for (vshard, generation) in homes {
                    self.tickets.close(vshard, generation, landed_at);
                }
            }
            Homes::Closed => {}
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        self.close(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GROUP: u64 = 4;

    /// A write admitted before the scheduler's table holds the fence open
    /// until it lands, then until its index applies.
    #[test]
    fn a_write_before_the_fence_holds_it_until_its_index() {
        let tickets = Arc::new(AdmissionTickets::new());
        let write = tickets.open(7);
        let fence = tickets.start_fence(7);
        assert_eq!(tickets.fence_state(fence, GROUP), FenceState::Open);
        write.landed(GROUP, 12);
        assert_eq!(
            tickets.fence_state(fence, GROUP),
            FenceState::Through(Some(12))
        );
    }

    /// A write that reached its core with no log index ends at its drop.
    #[test]
    fn a_dropped_write_needs_no_index() {
        let tickets = Arc::new(AdmissionTickets::new());
        let write = tickets.open(7);
        let fence = tickets.start_fence(7);
        drop(write);
        assert_eq!(tickets.fence_state(fence, GROUP), FenceState::Through(None));
    }

    /// A write that opens its ticket after the fence does not hold it.
    #[test]
    fn a_write_after_the_fence_does_not_hold_it() {
        let tickets = Arc::new(AdmissionTickets::new());
        let fence = tickets.start_fence(7);
        let _later = tickets.open(7);
        assert_eq!(tickets.fence_state(fence, GROUP), FenceState::Through(None));
    }

    /// A write to several vShards holds the fence of each of them.
    #[test]
    fn a_multi_home_write_holds_every_home() {
        let tickets = Arc::new(AdmissionTickets::new());
        let write = tickets.open_many(&[3, 70]);
        let fences = [tickets.start_fence(3), tickets.start_fence(70)];
        for fence in fences {
            assert_eq!(tickets.fence_state(fence, GROUP), FenceState::Open);
        }
        write.landed(GROUP, 9);
        for fence in fences {
            assert_eq!(
                tickets.fence_state(fence, GROUP),
                FenceState::Through(Some(9))
            );
        }
    }

    /// The highest landing of a group fences the scheduler; another group's
    /// landing does not.
    #[test]
    fn the_fence_waits_for_its_groups_highest_landing() {
        let tickets = Arc::new(AdmissionTickets::new());
        tickets.open(7).landed(GROUP, 30);
        tickets.open(7).landed(GROUP, 20);
        let fence = tickets.start_fence(7);
        assert_eq!(
            tickets.fence_state(fence, GROUP),
            FenceState::Through(Some(30))
        );
        assert_eq!(
            tickets.fence_state(fence, GROUP + 1),
            FenceState::Through(None)
        );
    }
}
