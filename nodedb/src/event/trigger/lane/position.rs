// SPDX-License-Identifier: BUSL-1.1

//! The replica-independent position of an event whose actions fire.
//!
//! The lane names an event as the CDC router does: by the change-feed
//! partition of its write and a position in that partition. The position is
//! the write's replicated position (a Raft entry, or a Calvin transaction)
//! with the event's ordinal among the write's firing events. One partition's
//! events come from one Data-Plane core in apply order, so every replica
//! numbers them alike.
//!
//! Every firing event takes an ordinal, whether or not this node's catalog
//! holds an action for its collection. The numbering then never depends on
//! when a replica applied a trigger's DDL.

use crate::event::cdc::position::{CALVIN_PARTITION_BASE, PartitionTail, PositionSequencer};
use crate::event::cdc::{CdcOffset, CdcRouter};
use crate::event::types::WriteEvent;

/// The bit of an action identity's source LSN that marks a Calvin
/// partition. A Raft log index and a Calvin sequencer epoch share one number
/// space, so the bit keeps their events apart. Neither reaches `2^63`.
const CALVIN_IDENTITY_BIT: u64 = 1 << 63;

/// The position allocator of the lane's firing events.
#[derive(Debug, Default)]
pub struct ActionPositions {
    sequencer: PositionSequencer,
}

impl ActionPositions {
    pub fn new() -> Self {
        Self::default()
    }

    /// The partition and position of `event`, a firing event. `tail` yields
    /// the highest position this node holds for a partition. It runs once
    /// per partition, on the partition's first firing event since this
    /// process started.
    pub fn next(
        &self,
        event: &WriteEvent,
        router: &CdcRouter,
        tail: impl FnOnce(u32) -> Option<CdcOffset>,
    ) -> (u32, CdcOffset) {
        let record_lsn = event
            .record
            .map_or(event.lsn.as_u64(), |record| record.lsn.as_u64());
        let (partition, source) = router.index_source(event.vshard_id.as_u32(), record_lsn);
        let position = self.sequencer.next(partition, source, record_lsn, || {
            tail(partition).map(|position| PartitionTail {
                position,
                record_lsn: 0,
            })
        });
        (partition, position)
    }
}

/// The `(source_lsn, source_sequence)` a fired action names its event by:
/// the position's index and sequence, with the Calvin bit set for a Calvin
/// partition. Every replica derives the same pair.
///
/// Every position has epoch `0` (see `cdc::offset`), so the pair names the
/// event within its vShard.
pub fn action_identity(partition: u32, position: CdcOffset) -> (u64, u64) {
    let kind = if partition >= CALVIN_PARTITION_BASE {
        CALVIN_IDENTITY_BIT
    } else {
        0
    };
    (position.index | kind, position.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::position::calvin_partition;

    #[test]
    fn a_raft_and_a_calvin_event_at_one_index_have_distinct_identities() {
        let position = CdcOffset::data_event(0, 12, 1);
        let raft = action_identity(3, position);
        let calvin = action_identity(calvin_partition(3), position);
        assert_eq!(raft, (12, position.sequence));
        assert_ne!(raft, calvin);
    }
}
