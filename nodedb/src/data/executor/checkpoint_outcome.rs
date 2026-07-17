// SPDX-License-Identifier: BUSL-1.1

//! What one engine's checkpoint flush achieved.
//!
//! Exists because the two things a flush has to report are not the same thing,
//! and conflating them is what the data-loss bug was: a COUNT of files written
//! says nothing about what is durable, since one unwritten file is enough to
//! make a reported LSN a lie however large the count. The LSN is a deletion
//! authority; the count is a dirty-page statistic. They travel together here so
//! neither call site has to invent the other.

use crate::types::Lsn;

/// The result of a successful engine checkpoint flush.
///
/// Only ever constructed on the success path: a flush that could not publish
/// every file it owns returns `Err` instead, so there is no representable
/// outcome that says "durable through X" about state that was not written.
pub(crate) struct CheckpointOutcome {
    /// The LSN this engine's state is now durable through OUTSIDE the WAL.
    ///
    /// `execute_checkpoint` folds this into the minimum it reports, and that
    /// minimum authorises `WalManager::truncate_before` to unlink every sealed
    /// segment below it. Nothing may be claimed here that the flush did not
    /// actually put on stable storage.
    pub(crate) durable_lsn: Lsn,

    /// Number of checkpoint files published by this flush.
    ///
    /// Feeds `CheckpointCoordinator::record_flush`'s dirty-page accounting
    /// ONLY — it is telemetry that decides when the maintenance tick next
    /// schedules this engine, and it must never be read as evidence of
    /// durability.
    pub(crate) files_written: usize,
}
