// SPDX-License-Identifier: BUSL-1.1

//! The single site where startup WAL replay gives up on a committed record.
//!
//! Every record handed to a `replay_*_wal` arm has already had its CRC
//! verified by the WAL reader, and every record in the replayed suffix was
//! acknowledged to a client as committed. So a record that an engine arm
//! cannot decode, cannot route, or whose handler rejects is not a damaged byte
//! range to step over — it is a committed write that this build cannot apply.
//! Continuing past it opens the database with a hole in the replayed suffix
//! that no later read can distinguish from data that was never written.
//!
//! Recovery therefore stops, exactly as `replay_all_wal` already does for a
//! redo group it cannot reconstitute and for a partially-recovered sync HWM
//! idempotency gate. The forensic
//! report is filed here rather than at each call site so one WAL tail that
//! fails identically on every core produces one report, not one per core.

/// Abort recovery because a committed WAL record cannot be applied.
///
/// `engine` names the replay arm (`kv`, `fts`, `spatial`, ...), `stage` the
/// step inside it that failed (`decode`, `handler`, `geometry`, ...), and
/// `detail` says why in the detecting site's own words.
///
/// Never returns: a partially replayed WAL is not a state the process may
/// serve from, and there is no caller that could do anything but propagate the
/// same decision.
pub(in crate::data::executor) fn abort_replay(
    engine: &str,
    stage: &str,
    core_id: usize,
    record_lsn: u64,
    detail: &str,
) -> ! {
    crate::diag::replay_record_unapplied(engine, stage, core_id, record_lsn, detail);
    tracing::error!(
        core_id,
        engine,
        stage,
        record_lsn,
        detail,
        "StartupError: a committed WAL record could not be applied — refusing to \
         start with a hole in the replayed suffix"
    );
    std::process::exit(1)
}
