// SPDX-License-Identifier: BUSL-1.1

//! Unified WAL-replay orchestration for a data-plane core on startup.
//!
//! Both the production data-plane runtime (`crate::data::runtime`) and the
//! integration-test core-loop runner call this ONE method so the replay
//! sequence never drifts between them.
//!
//! ## Crash injection
//!
//! Recovery is only idempotent if a crash PART WAY THROUGH it is, so the
//! sequence carries fail points a crash harness can arm through
//! `NODEDB_FAILPOINTS` (feature `failpoints`; `abort` kills the process the way
//! a real crash would):
//!
//! * `replay::before_engine_passes` — nothing applied yet.
//! * `replay::between_engine_passes` — one engine's pass complete, the next
//!   not started.
//! * `replay::kv_mid_pass` — part way through a single engine's records.
//! * `replay::between_standalone_and_redo` — every engine arm done, the
//!   redo-only document / graph arms not yet run.
//! * `replay::before_sync_hwm_pass` — every engine arm is done but the sync
//!   idempotency gate has not been rebuilt.

use nodedb_wal::{TombstoneSet, WalRecord};
use tracing::{error, info};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay every WAL record class into this core's engines, in the exact
    /// order restart correctness requires. No-op when `records` is empty.
    pub fn replay_all_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &TombstoneSet,
    ) {
        if records.is_empty() {
            return;
        }
        let core_id = self.core_id;

        crate::fail_point!("replay::before_engine_passes");

        // Every engine-bearing record class — standalone (autocommit) records
        // and the sub-records of committed `TransactionRedo` groups alike —
        // replays here in ONE globally LSN-ordered pass. Splitting the two
        // classes into separate passes inverts LSN order for any key written
        // both inside a transaction and by a later autocommit, and because redo
        // ops are absolute overwrites the older post-image would win.
        //
        // Fatal on error: a redo group that cannot be reconstituted is a
        // committed transaction that cannot be applied, and continuing would
        // open the database with a hole in the replayed suffix.
        if let Err(e) = self.replay_engines_in_lsn_order(records, num_cores, tombstones) {
            error!(
                core_id,
                error = %e,
                "StartupError: committed-transaction redo replay failed — \
                 refusing to start with an incompletely replayed WAL"
            );
            std::process::exit(1);
        }

        crate::fail_point!("replay::before_sync_hwm_pass");

        // Reconstruct sync HWM maps from SyncSeqAdvance records so
        // post-restart deduplication is correct. Fatal on error —
        // a partially-recovered HWM is not safe to operate with.
        match crate::wal::replay::replay_sync_hwm_records(records) {
            Ok((maps, stats)) => {
                if stats.records > 0 {
                    info!(
                        core_id,
                        records = stats.records,
                        "sync HWM WAL replay complete"
                    );
                }
                self.install_sync_hwm_maps(maps);
            }
            Err(e) => {
                error!(
                    core_id,
                    error = %e,
                    "StartupError: sync HWM WAL replay failed — \
                     refusing to start with a partially-recovered idempotency gate"
                );
                std::process::exit(1);
            }
        }
    }
}
