// SPDX-License-Identifier: BUSL-1.1

//! The WAL barrier of a snapshot install.
//!
//! An install writes no WAL records, but this node's WAL still holds the
//! records it applied to the group's collections before the install. Replay
//! re-applies the redb-backed engines' records (documents, full-text, graph
//! edges) with no floor. Replaying a pre-install record will bring back a
//! row the snapshot deleted, or overwrite an installed row with an older
//! value.
//!
//! The barrier is one `CollectionTombstoned` record per cleared collection,
//! all naming the WAL position before the install. Replay then skips every
//! record of those collections below it and replays every later record on
//! top of the installed state. The barrier is fsynced before any core clears,
//! so a crash inside the install leaves no pre-install record to replay over
//! the staged install that boot recovery re-applies.

use crate::types::{DatabaseId, TenantId};
use crate::wal::WalManager;
use crate::wal::manager::NO_APPLY_KEY;

use super::clear::GroupCollection;
use super::error::SnapshotInstallError;

/// Append and fsync the record that `group_id`'s install completed on this
/// node.
///
/// The installed rows ride no WAL record, so a point-in-time restore cannot
/// replay across the install from a base taken before it. A restore to a
/// target at or after this record starts from a base taken after it, which
/// holds the installed rows. The record is durable before the install
/// settles, so no write the group applies after the install precedes it.
pub fn append_install_marker(wal: &WalManager, group_id: u64) -> Result<(), SnapshotInstallError> {
    wal.appender(NO_APPLY_KEY)
        .append_snapshot_installed(group_id)
        .map_err(|source| SnapshotInstallError::Barrier { group_id, source })?;
    wal.sync()
        .map_err(|source| SnapshotInstallError::Barrier { group_id, source })
}

/// Append and fsync the install barrier for `collections`.
pub fn append_install_barrier(
    wal: &WalManager,
    group_id: u64,
    collections: &[GroupCollection],
) -> Result<(), SnapshotInstallError> {
    if collections.is_empty() {
        return Ok(());
    }
    let barrier = wal.next_lsn().as_u64();
    let appender = wal.appender(NO_APPLY_KEY);
    for coll in collections {
        appender
            .append_collection_tombstone(
                TenantId::new(coll.tenant_id),
                DatabaseId::new(coll.database_id),
                &coll.name,
                barrier,
            )
            .map_err(|source| SnapshotInstallError::Barrier { group_id, source })?;
    }
    wal.sync()
        .map_err(|source| SnapshotInstallError::Barrier { group_id, source })
}
