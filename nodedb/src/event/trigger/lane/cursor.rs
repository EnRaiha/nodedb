// SPDX-License-Identifier: BUSL-1.1

//! The replicated firing cursor of the trigger action lane.
//!
//! Each change-feed partition has one cursor: the position of the last event
//! whose actions fired. The owner commits the cursor past the events it
//! fired through the metadata group, as a consumer commits its offsets, so
//! every node raises the cursor on apply. A new owner resumes after it, and
//! every replica releases the events it passed.
//!
//! The cursor is a consumer-group offset under a reserved stream and group
//! name. No change stream or topic takes the name: a `:` in a stream name is
//! reserved, and `_trigger` names no topic.

use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::event::cdc::consumer_group::{OffsetCommit, PartitionOffset};
use crate::types::DatabaseId;

/// The stream the cursors are kept under.
pub const CURSOR_STREAM: &str = "_trigger:actions";
/// The group the cursors are kept under.
pub const CURSOR_GROUP: &str = "_trigger_firing";
/// The tenant the cursors are kept under: the system's own.
const CURSOR_TENANT: u64 = 0;

/// Whether an offset commit raises the firing cursors. The commit applies
/// without a registered group: every node keeps the cursors.
pub fn is_action_cursor(commit: &OffsetCommit) -> bool {
    commit.database_id == DatabaseId::DEFAULT
        && commit.tenant_id == CURSOR_TENANT
        && commit.stream_name == CURSOR_STREAM
        && commit.group_name == CURSOR_GROUP
}

/// The position of the last event of `partition` whose actions fired.
pub fn fired_through(state: &SharedState, partition: u32) -> CdcOffset {
    state.offset_store.get_offset(
        DatabaseId::DEFAULT,
        CURSOR_TENANT,
        CURSOR_STREAM,
        CURSOR_GROUP,
        partition,
    )
}

/// Raise `partition`'s cursor to `through` on this node only. A Raft group
/// snapshot install takes the builder's cursor this way: the metadata
/// entries that raised it there can be missing on this node.
pub fn raise_fired_here(
    state: &SharedState,
    partition: u32,
    through: CdcOffset,
) -> crate::Result<()> {
    state.offset_store.advance_offsets(
        DatabaseId::DEFAULT,
        CURSOR_TENANT,
        CURSOR_STREAM,
        CURSOR_GROUP,
        &[PartitionOffset::new(partition, through)],
    )
}

/// Raise `partition`'s cursor to `through` on every node.
pub(super) async fn commit_fired(
    state: &SharedState,
    partition: u32,
    through: CdcOffset,
) -> crate::Result<()> {
    let commit = OffsetCommit {
        database_id: DatabaseId::DEFAULT,
        tenant_id: CURSOR_TENANT,
        stream_name: CURSOR_STREAM.to_owned(),
        group_name: CURSOR_GROUP.to_owned(),
        group_hlc: nodedb_types::Hlc::ZERO,
        offsets: vec![PartitionOffset::new(partition, through)],
    };
    crate::control::metadata_proposer::propose_cursor_commit(state, commit)
        .await
        .map(|_| ())
}
