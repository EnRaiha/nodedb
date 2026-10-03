// SPDX-License-Identifier: BUSL-1.1

//! `RestorePoint` host-side effect, and the clock step every stamped entry
//! takes.

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_types::Hlc;
use nodedb_wal::record::RestorePointPayload;

use super::types::MetadataCommitApplier;
use crate::control::pitr::restore_point::{record_group_point, spawn_node_cut};
use crate::control::security::catalog::restore_points::StoredRestorePoint;

impl MetadataCommitApplier {
    /// Move this node's clock past the leader's stamp of the entry `data`,
    /// and record the stamp as the applied high-water.
    ///
    /// A write that follows the entry's effect on this node then carries a
    /// later HLC, so a restore that drops the entry drops the write too. A
    /// leader's next stamp lands above it, and the high-water carries that
    /// across a restart and a snapshot install.
    pub(super) fn observe_stamp(&self, data: &[u8]) -> crate::Result<()> {
        let Some(stamp) = nodedb_cluster::entry_stamp(data) else {
            return Ok(());
        };
        self.credentials.catalog().raise_metadata_stamp_hwm(stamp)?;
        if let Ok(shared) = self.shared_state() {
            shared.hlc_clock.update(Hlc::new(stamp, 0));
        }
        Ok(())
    }

    /// Record the restore point created at `raft_index`, record the metadata
    /// group's place at it in this node's WAL, then cut every group this node
    /// hosts at `hlc`.
    ///
    /// The catalog row is the durable effect: a persist error returns `Err`,
    /// so the watermark stays on this entry and Raft re-delivers it.
    pub(super) fn apply_restore_point(
        &self,
        hlc: u64,
        created_at_ms: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        self.credentials
            .catalog()
            .put_restore_point(&StoredRestorePoint {
                id: raft_index,
                hlc,
                created_at_ms,
            })?;
        record_group_point(
            &shared,
            RestorePointPayload {
                id: raft_index,
                hlc,
                group_id: METADATA_GROUP_ID,
                applied_index: raft_index,
                term: 0,
                next_epoch: 0,
                epoch_system_ms: 0,
                vshards: Vec::new(),
            },
        );
        spawn_node_cut(shared, raft_index, hlc);
        Ok(())
    }
}
