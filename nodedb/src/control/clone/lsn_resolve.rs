// SPDX-License-Identifier: BUSL-1.1

//! Wall-clock millisecond → LSN resolution for the clone CoW resolver.
//!
//! Resolves a user-supplied `AS OF SYSTEM TIME <ms>` value to the WAL state
//! committed by then, from the WAL's time anchors. LSNs here are exclusive
//! bounds, as `wal.next_lsn()` is.

use nodedb_types::{Lsn, LsnTimeError};

use crate::control::state::SharedState;

/// The exclusive LSN bound of the state committed by millisecond `wall_ms`.
///
/// Fails when `wall_ms` is before the oldest retained time anchor. See
/// [`SharedState::ms_to_lsn`].
pub fn wall_ms_to_lsn(state: &SharedState, wall_ms: i64) -> Result<Lsn, LsnTimeError> {
    state.ms_to_lsn(wall_ms)
}

/// System time, in wall ms, at which a read through a clone cuts its source:
/// the commit time of the source state below `as_of_lsn`.
///
/// `None` for a non-bitemporal source. It keeps only current versions, so a
/// system-time cut hides every row. A clone collection carries its
/// source's `bitemporal` flag.
pub fn source_as_of_ms(
    state: &SharedState,
    source_bitemporal: bool,
    as_of_lsn: Lsn,
) -> Option<i64> {
    if !source_bitemporal {
        return None;
    }
    state.ms_to_lsn_inverse(as_of_lsn)
}
