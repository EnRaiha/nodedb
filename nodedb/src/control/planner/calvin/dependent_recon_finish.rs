// SPDX-License-Identifier: BUSL-1.1

//! The outcome of a committed edge recon transaction.

use nodedb_physical::physical_task::PhysicalTask;

use super::dependent_recon::DependentReconOutcome;
use super::submit::local::{ReplyFold, with_reported_results};
use crate::Error;
use crate::control::state::{CalvinApplyResult, SharedState};

/// The outcome of a committed recon transaction over `tasks`: waits for the
/// authorization barrier when a task writes a permission-tree source, then
/// drains the applied response the scheduler deposited.
pub(super) async fn finish_committed(
    state: &SharedState,
    tasks: &[PhysicalTask],
    completed_txn: nodedb_cluster::calvin::TxnId,
    ack_results: &[Vec<u8>],
) -> crate::Result<DependentReconOutcome> {
    // A write to a permission-tree source is acknowledged only once it binds
    // every node.
    let sources = state.authorization_fence.sources();
    let binds_authorization = tasks.iter().any(|task| {
        task.plan
            .named_collections()
            .iter()
            .any(|collection| sources.is_source_collection(collection))
    });
    if binds_authorization {
        crate::control::security::auth_lease::calvin_write_barrier(state).await?;
    }

    // Completion fired: the scheduler deposited the applied Response (with any
    // RETURNING rows) into the sidecar before proposing the ack that woke the
    // retry loop, so the entry is present now if this write carried RETURNING.
    // Drain it (removing the entry) for the caller to shape into DATA-ROWs; a
    // `Conflict` (>1 RETURNING participant) fails loudly rather than returning a
    // partial cross-shard union.
    let drained = state.calvin.apply_results.take(&completed_txn);
    let applied = match drained {
        Some(CalvinApplyResult::Single {
            response,
            has_returning,
        }) => {
            // An installed txn whose reply failed to render deposits it as an
            // error for the statement.
            crate::control::local_dispatch::reject_data_plane_error(&response)?;
            Some((response, has_returning))
        }
        Some(CalvinApplyResult::Conflict) => {
            return Err(Error::Internal {
                detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
            });
        }
        None => None,
    };
    // A participant on a node that holds no replica of it reports its
    // counts and RETURNING rows through its completion ack.
    let fold = ReplyFold::of_plans(tasks.iter().map(|task| &task.plan));
    let apply_result = with_reported_results(applied, ack_results, fold)?;

    Ok(DependentReconOutcome {
        tasks_dispatched: tasks.len() as u64,
        apply_result,
    })
}
