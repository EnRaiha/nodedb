// SPDX-License-Identifier: BUSL-1.1

//! Per-group indexes a data-group snapshot is cut at.
//!
//! - `started`: the highest entry the apply loop took off its backlog. Entries
//!   start in log order, so every entry at or below it started. A snapshot
//!   builder fences the group's apply and captures at this index once the
//!   group settled through it.
//! - `covered`: the highest entry a snapshot this node installed holds. The
//!   snapshot is cut at or above its Raft index, so the entries between the
//!   two reach the apply loop from the log. Their effects are in the
//!   installed state already, so the loop concludes them without applying.
//!
//! One pair of indexes per group, bounded by the groups of the cluster.

use std::collections::HashMap;

/// Both indexes of one group.
#[derive(Debug, Default, Clone, Copy)]
struct GroupCut {
    started: u64,
    covered: u64,
}

/// Every group's cut indexes.
#[derive(Debug, Default)]
pub(super) struct GroupCuts {
    groups: HashMap<u64, GroupCut>,
}

impl GroupCuts {
    /// Raise `group_id`'s started index to `log_index`.
    pub fn note_started(&mut self, group_id: u64, log_index: u64) {
        let cut = self.groups.entry(group_id).or_default();
        cut.started = cut.started.max(log_index);
    }

    /// Highest entry of `group_id` the apply loop started.
    pub fn started_through(&self, group_id: u64) -> u64 {
        self.groups.get(&group_id).map_or(0, |cut| cut.started)
    }

    /// Raise `group_id`'s covered index to `log_index`.
    pub fn cover_through(&mut self, group_id: u64, log_index: u64) {
        let cut = self.groups.entry(group_id).or_default();
        cut.covered = cut.covered.max(log_index);
    }

    /// Highest entry of `group_id` an installed snapshot holds.
    pub fn covered_through(&self, group_id: u64) -> u64 {
        self.groups.get(&group_id).map_or(0, |cut| cut.covered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexes_only_rise_and_stay_per_group() {
        let mut cuts = GroupCuts::default();
        cuts.note_started(1, 9);
        cuts.note_started(1, 4);
        cuts.cover_through(1, 12);
        cuts.cover_through(1, 7);
        assert_eq!(cuts.started_through(1), 9);
        assert_eq!(cuts.covered_through(1), 12);
        assert_eq!(cuts.started_through(2), 0);
        assert_eq!(cuts.covered_through(2), 0);
    }
}
