// SPDX-License-Identifier: BUSL-1.1

//! Staging of a transaction delete's node-delete edge tasks.
//!
//! A delete on an edge-bearing collection inside a transaction block carries
//! the edge tasks `txn_node_delete_tasks` derives. Each `EdgeDelete` stages
//! into the transaction's graph overlay now, so later reads in the
//! transaction see the edge gone. Each `NodeEdgeGuard` is buffered, and
//! COMMIT checks it.

use std::future::Future;

use crate::bridge::envelope::Response;
use crate::control::server::shared::sql::staging_predicates::is_stageable_write;
use crate::control::state::SharedState;
use nodedb_physical::physical_task::PhysicalTask;

use super::connection::SessionId;
use super::staging_gate::{StagingGateError, stage_write};
use super::store::SessionStore;
use super::txn_expand::expand_for_buffering;

/// Stage or buffer each of `tasks` in the session's transaction.
pub(super) async fn stage_node_delete_tasks<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    tasks: Vec<PhysicalTask>,
    dispatch: &F,
) -> Result<(), StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    for task in tasks {
        if is_stageable_write(&task.plan) {
            stage_write(state, sessions, session_id, task, dispatch).await?;
        } else {
            for shard_task in expand_for_buffering(task).map_err(StagingGateError::Dispatch)? {
                sessions.buffer_write(session_id, shard_task);
            }
        }
    }
    Ok(())
}
