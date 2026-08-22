// SPDX-License-Identifier: BUSL-1.1

//! Linearizable reads against a group hosted on this node.
//!
//! Both calls are non-blocking. The probe is taken under the coordinator
//! lock and confirmed under a later one, so the caller polls between ticks
//! instead of holding the lock while a quorum answers.

use nodedb_raft::{ReadIndexProbe, ReadIndexStatus};

use crate::multi_raft::core::MultiRaft;

impl MultiRaft {
    /// Begin a linearizable read on `group_id`.
    ///
    /// `None` when the group is not hosted here or this node does not lead
    /// it. Confirm with [`Self::read_index_confirmed`] before serving.
    pub fn start_read_index(&mut self, group_id: u64) -> Option<ReadIndexProbe> {
        self.groups.get_mut(&group_id)?.start_read_index()
    }

    /// How far `probe` has got. A group that is no longer hosted here counts
    /// as lost leadership, not as still waiting.
    pub fn read_index_status(&self, group_id: u64, probe: &ReadIndexProbe) -> ReadIndexStatus {
        match self.groups.get(&group_id) {
            Some(node) => node.read_index_status(probe),
            None => ReadIndexStatus::LeadershipLost,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::multi_raft::core::MultiRaft;
    use crate::routing::RoutingTable;

    fn coordinator(dir: &tempfile::TempDir) -> MultiRaft {
        MultiRaft::new(
            1,
            RoutingTable::uniform(1, &[1, 2, 3], 3),
            PathBuf::from(dir.path()),
        )
    }

    #[test]
    fn an_unhosted_group_starts_no_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = coordinator(&dir);
        assert!(mr.start_read_index(7).is_none());
    }

    #[test]
    fn a_group_this_node_does_not_lead_starts_no_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut mr = coordinator(&dir);
        mr.add_group(7, vec![1, 2, 3]).expect("add group");
        assert!(
            mr.start_read_index(7).is_none(),
            "a fresh group starts as a follower"
        );
    }
}
