// SPDX-License-Identifier: BUSL-1.1

//! Timeseries memtable admission knobs: `NODEDB_TS_MEMTABLE_BUDGET_BYTES`,
//! `NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES`, `NODEDB_TS_MAX_TAG_CARDINALITY`.
//!
//! These are env-reachable because the record-boundary admission gate they
//! drive is otherwise only observable at 64/80 MiB and 100k distinct tags —
//! a cost no test can pay, which is how a mid-record flush stamping a
//! partition with the WRONG WAL LSN went unnoticed. Defaults are unchanged;
//! this only makes them reachable.

use super::helpers::apply_positive_env;
use crate::config::server::ServerConfig;

pub(super) fn apply_timeseries_overrides(config: &mut ServerConfig) {
    apply_positive_env(
        "NODEDB_TS_MEMTABLE_BUDGET_BYTES",
        &mut config.tuning.timeseries.memtable_budget_bytes,
    );
    apply_positive_env(
        "NODEDB_TS_MEMTABLE_HARD_LIMIT_BYTES",
        &mut config.tuning.timeseries.memtable_hard_limit_bytes,
    );
    apply_positive_env(
        "NODEDB_TS_MAX_TAG_CARDINALITY",
        &mut config.tuning.timeseries.max_tag_cardinality,
    );
}
