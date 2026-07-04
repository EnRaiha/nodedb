// SPDX-License-Identifier: BUSL-1.1

//! Deleted-node-tracker undo entry application logic.
//!
//! The PointDelete cascade records a deleted document's node id in the
//! in-memory `deleted_nodes` set so a subsequent `EdgePut` to that node is
//! rejected as dangling. This tracker is IN-MEMORY, so an aborted redb write
//! transaction does NOT reverse it — a rolled-back tx DELETE must explicitly
//! un-mark the node (mirroring the vector/spatial/stats undo paths, which
//! reverse in-memory side-effects an aborted redb txn leaves behind).
//!
//! The forward capture only pushes a `MarkNodeDeleted` entry when the mark
//! newly inserted the node, so this un-mark never resurrects a tombstone a
//! prior committed op created.
//!
//! Returns `Err((entry_index, detail))` on fatal failure so the caller can
//! escalate to a typed `RollbackFailed` response.

use crate::data::executor::core_loop::CoreLoop;

use super::UndoEntry;

impl CoreLoop {
    pub(super) fn apply_undo_mark_node(
        &mut self,
        _entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::MarkNodeDeleted {
                database_id,
                tid,
                node_id,
            } => {
                self.unmark_node_deleted(database_id, tid, &node_id);
                Ok(())
            }
            _ => unreachable!("apply_undo_mark_node called with non-mark-node entry"),
        }
    }
}
