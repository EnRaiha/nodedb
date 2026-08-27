// SPDX-License-Identifier: BUSL-1.1

//! Whether a whole planned statement may be handed to the in-transaction
//! staging gate.
//!
//! A dispatch door that runs a statement itself while a session is inside an
//! explicit transaction bypasses the buffer, so ROLLBACK cannot undo it. Each
//! such door asks this first: every write must be one the gate buffers, or the
//! statement is refused rather than silently applied.

use nodedb_physical::physical_task::PhysicalTask;

use super::classify::plan_requires_txn_buffering;
use crate::control::planner::calvin::is_write_plan;

/// True when the staging gate can buffer every write in `tasks`. Reads impose
/// no requirement — the gate stamps the transaction id and dispatches them.
pub fn all_writes_bufferable(tasks: &[PhysicalTask]) -> bool {
    tasks
        .iter()
        .all(|task| !is_write_plan(&task.plan) || plan_requires_txn_buffering(&task.plan))
}
