// SPDX-License-Identifier: BUSL-1.1

//! Where each partition's change events begin on this node.
//!
//! A replica caught up by snapshot install never applies the entries the
//! snapshot covers, so it never routes their change events. It records the
//! first position it can serve for each partition of the installed group. A
//! consumer whose cursor lies below that position silently skips the
//! missing events here, so the consume is refused instead.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::event::cdc::offset::CdcOffset;

/// Per-partition first servable position. A partition with no entry serves
/// its whole history.
#[derive(Debug, Default)]
pub struct AvailabilityFloors {
    floors: RwLock<HashMap<u32, CdcOffset>>,
}

impl AvailabilityFloors {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that events of `partition` below `from` are not available on
    /// this node. A floor only rises.
    pub fn raise(&self, partition: u32, from: CdcOffset) {
        let mut floors = self.floors.write().unwrap_or_else(|p| p.into_inner());
        let floor = floors.entry(partition).or_insert(from);
        if from > *floor {
            *floor = from;
        }
    }

    /// The first position this node serves for `partition`, if a snapshot
    /// install left a gap before it.
    pub fn floor(&self, partition: u32) -> Option<CdcOffset> {
        self.floors
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&partition)
            .copied()
    }

    /// Every recorded floor.
    pub fn all(&self) -> Vec<(u32, CdcOffset)> {
        self.floors
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(partition, floor)| (*partition, *floor))
            .collect()
    }

    /// Whether a consumer at `cursor` on `partition` can miss events here:
    /// the events between the cursor and the floor were never routed on this
    /// node.
    pub fn misses(&self, partition: u32, cursor: CdcOffset) -> Option<CdcOffset> {
        let floor = self.floor(partition)?;
        (cursor < floor && !covers_gap(cursor, floor)).then_some(floor)
    }
}

/// Whether a cursor already acknowledges everything below `floor`: it
/// acknowledges the whole write before the floor's write.
fn covers_gap(cursor: CdcOffset, floor: CdcOffset) -> bool {
    floor.index > 0 && cursor >= CdcOffset::whole_write(floor.epoch, floor.index - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_below_the_floor_misses_events() {
        let floors = AvailabilityFloors::new();
        let floor = CdcOffset::at(0, 101, 0);
        floors.raise(3, floor);
        assert_eq!(floors.misses(3, CdcOffset::ZERO), Some(floor));
        assert_eq!(
            floors.misses(3, CdcOffset::data_event(0, 50, 1)),
            Some(floor)
        );
        // A cursor through entry 100 misses nothing.
        assert_eq!(floors.misses(3, CdcOffset::whole_write(0, 100)), None);
        assert_eq!(floors.misses(3, CdcOffset::data_event(0, 101, 1)), None);
        assert_eq!(floors.misses(4, CdcOffset::ZERO), None);
    }

    #[test]
    fn a_floor_only_rises() {
        let floors = AvailabilityFloors::new();
        floors.raise(1, CdcOffset::at(0, 50, 0));
        floors.raise(1, CdcOffset::at(0, 20, 0));
        assert_eq!(floors.floor(1), Some(CdcOffset::at(0, 50, 0)));
    }
}
