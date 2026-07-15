// SPDX-License-Identifier: BUSL-1.1

//! CommitApplier for committed data-group Raft entries.
//!
//! When a Raft entry is committed across the quorum, it is pushed into the
//! `DistributedApplier`'s bounded mpsc channel, which a background task
//! drains — dispatching to the Data Plane via SPSC and notifying proposers
//! via `ProposeTracker`. The Calvin sequencer group is intercepted locally
//! and applied into the in-process `SequencerStateMachine` instead.

use std::sync::{Arc, Mutex};

use crate::control::distributed_applier::DistributedApplier;
use nodedb_cluster::calvin::{SEQUENCER_GROUP_ID, SequencerStateMachine};

/// CommitApplier that delegates committed data-group Raft entries to
/// the `DistributedApplier` apply loop.
///
/// This is the bridge between the synchronous Raft tick loop (which calls
/// `apply_committed`) and the async `run_apply_loop` task (which does the
/// actual Data Plane dispatch and proposer notification).
pub struct SpscCommitApplier {
    applier: Arc<DistributedApplier>,
    sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
}

impl SpscCommitApplier {
    pub fn new(
        _shared: Arc<crate::control::state::SharedState>,
        applier: Arc<DistributedApplier>,
        sequencer_state_machine: Arc<Mutex<SequencerStateMachine>>,
    ) -> Self {
        Self {
            applier,
            sequencer_state_machine,
        }
    }
}

impl nodedb_cluster::CommitApplier for SpscCommitApplier {
    fn apply_committed(&self, group_id: u64, entries: &[nodedb_raft::message::LogEntry]) -> u64 {
        if group_id == SEQUENCER_GROUP_ID {
            let mut sm = self
                .sequencer_state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for entry in entries {
                if !entry.data.is_empty() {
                    sm.apply(entry.index, &entry.data);
                }
            }
            return entries.last().map(|e| e.index).unwrap_or(0);
        }
        self.applier.apply_committed(group_id, entries)
    }
}
