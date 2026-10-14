// SPDX-License-Identifier: BUSL-1.1

//! Timeseries partition compaction.
//!
//! Merges the partitions of each timeseries interval into one, through the
//! same merge the maintenance tick runs (`partition_merge`). Retention is
//! not compaction's work: the retention policy removes expired partitions,
//! after folding them into every continuous aggregate.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Merge timeseries partition groups. `force` (an on-demand `COMPACT`)
    /// merges every group, including intervals that have not ended, without
    /// the per-tick and maintenance budgets.
    ///
    /// Returns `(merged, deferred)`: the groups merged, and the collections
    /// skipped because their database was over its maintenance CPU budget.
    pub(super) fn run_segment_compaction(&mut self, force: bool) -> (usize, usize) {
        let pass = self.merge_ts_partitions(force);
        if pass.merged > 0 || pass.deferred > 0 {
            tracing::info!(
                core = self.core_id,
                merged = pass.merged,
                deferred = pass.deferred,
                "timeseries partition compaction"
            );
        }
        (pass.merged, pass.deferred)
    }
}
