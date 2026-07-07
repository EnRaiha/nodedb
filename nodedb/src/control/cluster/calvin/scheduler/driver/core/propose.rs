// SPDX-License-Identifier: BUSL-1.1

//! Propose `ReplicatedWrite::CalvinReadResult` Raft entries from passive
//! participants.

use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

/// The passive read result to replicate: identity of the originating vshard
/// plus the sequencer coordinate, scope, and read values.
pub struct CalvinReadResultProposal {
    pub vshard_id: u32,
    pub epoch: u64,
    pub position: u32,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub passive_vshard: u32,
    pub values_payload: Vec<u8>,
}

/// Propose a `ReplicatedWrite::CalvinReadResult` entry to the per-vshard
/// Raft group after a passive participant responds.
///
/// This is a Control Plane function (Tokio async context). It constructs the
/// Raft entry and hands it to the WAL replication layer.
///
/// `Instant::now()` is NOT used here (this goes to the WAL/Raft path).
pub async fn propose_calvin_read_result(
    raft_proposer: &(dyn Fn(u32, Vec<u8>) -> crate::Result<(u64, u64)> + Send + Sync),
    proposal: CalvinReadResultProposal,
) -> crate::Result<()> {
    let CalvinReadResultProposal {
        vshard_id,
        epoch,
        position,
        tenant_id,
        database_id,
        passive_vshard,
        values_payload,
    } = proposal;
    let entry = ReplicatedEntry::new(
        tenant_id.as_u64(),
        database_id.as_u64(),
        vshard_id,
        ReplicatedWrite::CalvinReadResult {
            epoch,
            position,
            passive_vshard,
            tenant_id: tenant_id.as_u64(),
            values: values_payload,
        },
    );
    let data = entry.to_bytes();
    raft_proposer(vshard_id, data)?;
    Ok(())
}
