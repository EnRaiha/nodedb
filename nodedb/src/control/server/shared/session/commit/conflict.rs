// SPDX-License-Identifier: BUSL-1.1

//! Snapshot-isolation write-conflict validation for a single-shard COMMIT.

use crate::control::state::SharedState;

use super::super::connection::SessionId;
use super::super::outcome::{AbortReason, CommitOutcome};
use super::super::read_set::ReadSetEntry;
use super::super::store::SessionStore;

/// Snapshot-isolation write-conflict check for a single-shard interactive
/// COMMIT. If any read key's collection advanced past both the read LSN and the
/// transaction snapshot LSN — and the transaction did not write that collection
/// itself (read-your-own-write is excluded) — the WAL moved under the reader:
/// records the read-set hot-key aborts and returns a serialization abort. The
/// caller owns the session rollback (via `release_and_rollback`) so it can
/// first release the transaction's read reservations while the reservation owner
/// is still set — the rollback clears it. Returns `None` when there is no
/// conflict (or no snapshot, i.e. not in a transaction).
///
/// This is a single-shard validation: it compares against the global WAL
/// `next_lsn`, so it is only sound for a transaction whose participants are one
/// shard, and is run exclusively on the `SingleShard` / read-only paths.
pub(super) fn si_conflict_abort(
    sessions: &SessionStore,
    session_id: SessionId,
    state: &SharedState,
    read_set: &[ReadSetEntry],
    written_collections: &std::collections::HashSet<String>,
) -> Option<CommitOutcome> {
    let snapshot_lsn = sessions.snapshot_lsn(session_id)?;
    let current_lsn = state.wal.next_lsn();
    let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
    for entry in read_set {
        let collection = &entry.collection;
        let read_lsn = entry.read_lsn;
        if written_collections.contains(collection) {
            continue;
        }
        if current > read_lsn && current > snapshot_lsn {
            // WAL advanced past what we read — concurrent write detected. The
            // caller releases reservations and rolls the session back.
            super::super::hot_key::record_read_set_aborts(state, read_set);
            return Some(CommitOutcome::Aborted {
                reason: AbortReason::Serialization,
            });
        }
    }
    None
}
