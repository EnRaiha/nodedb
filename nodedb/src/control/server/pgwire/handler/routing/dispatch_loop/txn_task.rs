// SPDX-License-Identifier: BUSL-1.1

//! The transaction a pgwire statement's tasks run in, and one task's route
//! through it.
//!
//! Inside a transaction block every task routes through the client's
//! session. Outside one, a statement whose write fires a BEFORE, INSTEAD OF
//! or SYNC AFTER body runs in an implicit transaction under
//! `EventSource::ImplicitClient` ([`NodeDbPgHandler::dispatch_task_loop_implicit`]):
//! its writes and the bodies' writes stage there and commit together once the
//! statement's tasks all succeed, so its rows fire ASYNC triggers as an
//! autocommit write's do and the bodies' rows fire none. Every other
//! autocommit statement dispatches its tasks directly.
//!
//! Each task in a transaction takes the shared `txn_route`, the route the
//! native protocol takes too.

use std::sync::Arc;

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::server::response_shape::types::{replaced_write_outcome, staged_dml_outcome};
use crate::control::server::shared::session::staging_gate::StagingGateError;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::txn_route::{
    StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task,
};
use crate::control::trigger::statement_txn::with_statement_txn_lifted;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::super::types::{error_to_pg, error_to_sqlstate};
use super::super::super::core::NodeDbPgHandler;
use super::super::super::plan::describe_plan;
use super::super::execute_dml_hooks::HandledWrite;
use super::run::DispatchTaskContext;

/// What one task of a statement in a transaction contributes.
pub(super) enum StatementTxnOutcome {
    /// Not a write the transaction holds: dispatch it.
    Dispatch(Box<PhysicalTask>),
    /// A write the transaction holds: its share of the command tag.
    Write(HandledWrite),
    /// A staged write that answers `RETURNING`: the `RowsPayload` of each
    /// write it staged. Its rows answer the statement in place of a count.
    Returning(Vec<Vec<u8>>),
    /// A clone copy-on-write answered the write: shape this response.
    Clone(crate::bridge::envelope::Response),
}

impl NodeDbPgHandler {
    /// Execute the per-task dispatch loop for non-Calvin queries: in the
    /// client's transaction inside a block, directly outside one.
    pub(crate) async fn dispatch_task_loop(
        &self,
        tasks: Vec<PhysicalTask>,
        context: DispatchTaskContext<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let txn = DmlTxnCtx {
            sessions: &self.sessions,
            session_id: context.session_id,
        };
        let in_block =
            self.sessions.transaction_state(context.session_id) == TransactionState::InBlock;
        self.dispatch_task_loop_in(tasks, context, in_block.then_some(&txn))
            .await
    }

    /// Execute the per-task dispatch loop in an implicit transaction: the
    /// statement, outside a transaction block, fires a joined trigger body.
    /// The transaction commits when every task succeeds and rolls back when
    /// one fails.
    pub(in crate::control::server::pgwire::handler::routing) async fn dispatch_task_loop_implicit(
        &self,
        tasks: Vec<PhysicalTask>,
        context: DispatchTaskContext<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let identity = context.identity.clone();
        let client = DmlTxnCtx {
            sessions: &self.sessions,
            session_id: context.session_id,
        };
        with_statement_txn_lifted(
            &self.state,
            &identity,
            &client,
            true,
            |error| error_to_pg(&error),
            async |txn: &DmlTxnCtx<'_>| self.dispatch_task_loop_in(tasks, context, Some(txn)).await,
        )
        .await
    }

    /// Route one task of a statement through its transaction: its triggers,
    /// its clone copy-on-write steps and its staging.
    pub(super) async fn route_statement_txn_task(
        &self,
        route: &TxnTaskContext<'_>,
        task: PhysicalTask,
        events: &mut StatementEvents,
    ) -> PgWireResult<StatementTxnOutcome> {
        let plan_kind = describe_plan(&task.plan);
        let identity = route.identity;
        let user_id: Option<Arc<str>> = Some(Arc::from(identity.username.as_str()));
        let outcome = route_txn_task(route, task, events, |stage_task| {
            self.dispatch_authorized_task(stage_task, user_id.clone(), identity, false)
        })
        .await
        .map_err(|error| staging_error_to_pg(&error))?;
        Ok(match outcome {
            TxnTaskOutcome::Dispatch(task) => StatementTxnOutcome::Dispatch(task),
            TxnTaskOutcome::Buffered => StatementTxnOutcome::Write(HandledWrite::Opaque),
            TxnTaskOutcome::Staged(staged) if !staged.returning_rows.is_empty() => {
                StatementTxnOutcome::Returning(staged.returning_rows)
            }
            TxnTaskOutcome::Staged(staged) => StatementTxnOutcome::Write(HandledWrite::Dml(
                staged_dml_outcome(staged.kind, staged.affected),
            )),
            TxnTaskOutcome::CloneHandled(resp) => StatementTxnOutcome::Clone(resp),
            TxnTaskOutcome::InsteadOf => {
                StatementTxnOutcome::Write(match replaced_write_outcome(plan_kind) {
                    Some(outcome) => HandledWrite::Dml(outcome),
                    None => HandledWrite::Opaque,
                })
            }
        })
    }
}

/// The pgwire error for a task its transaction refused.
fn staging_error_to_pg(error: &StagingGateError) -> PgWireError {
    let (severity, code, message) = match error {
        StagingGateError::Dispatch(error) => {
            let (severity, code, message) = error_to_sqlstate(error);
            (severity, code.to_owned(), message)
        }
        StagingGateError::Rejected { code: Some(code) } => {
            let (severity, sqlstate, message) =
                crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code);
            (severity, sqlstate.to_owned(), message)
        }
        StagingGateError::Rejected { code: None } => (
            "ERROR",
            nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
            "unknown data plane error".to_owned(),
        ),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(severity.to_owned(), code, message)))
}
