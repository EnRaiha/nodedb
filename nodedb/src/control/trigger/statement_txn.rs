// SPDX-License-Identifier: BUSL-1.1

//! The transaction a client statement and its synchronous trigger bodies
//! share.
//!
//! A BEFORE, INSTEAD OF or SYNC AFTER body joins the transaction of the
//! statement that fired it, so the statement's write and the body's writes
//! commit together or not at all. Inside a transaction block that is the
//! client's own transaction. Outside one, a statement whose write fires such
//! a body runs in an implicit transaction: its write stages there with the
//! bodies' writes, and the transaction commits when the statement succeeds.
//!
//! The implicit transaction's source is
//! [`EventSource::ImplicitClient`]: the client's rows fire ASYNC triggers, as
//! an autocommit write's do. The bodies' rows commit under `Trigger`.

use crate::control::security::catalog::trigger_types::{TriggerExecutionMode, TriggerTiming};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::conn_scope::scoped_system_txn;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::state::SharedState;
use crate::control::system_txn::OpenSystemTxn;
use crate::event::EventSource;

use super::registry::DmlEvent;
use super::scope::TriggerScope;

/// Whether a write of `event` to `collection` fires a body that joins its
/// statement's transaction: a BEFORE, INSTEAD OF or SYNC AFTER trigger.
pub fn fires_joined_body(
    state: &SharedState,
    scope: TriggerScope,
    collection: &str,
    event: DmlEvent,
) -> bool {
    state
        .trigger_registry
        .get_matching(
            scope.database_id,
            scope.tenant_id.as_u64(),
            collection,
            event,
        )
        .iter()
        .any(|trigger| match trigger.timing {
            TriggerTiming::Before | TriggerTiming::InsteadOf => true,
            TriggerTiming::After => trigger.execution_mode == TriggerExecutionMode::Sync,
        })
}

/// Whether the session `ctx` names has a transaction block open.
pub fn in_block(ctx: &DmlTxnCtx<'_>) -> bool {
    ctx.sessions.transaction_state(ctx.session_id) == TransactionState::InBlock
}

/// Run `work` on the statement's transaction.
///
/// `implicit` asks for an implicit transaction: the caller found that the
/// statement, outside a transaction block, fires a joined body. `work` then
/// runs on a private session whose transaction commits when `work` succeeds
/// and rolls back when it fails. Otherwise `work` runs on `ctx` itself.
///
/// The implicit transaction runs in its own connection slots, so its DDL
/// buffer never outlives it on the connection.
pub async fn with_statement_txn<T, E>(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    ctx: &DmlTxnCtx<'_>,
    implicit: bool,
    work: impl AsyncFnOnce(&DmlTxnCtx<'_>) -> Result<T, E>,
) -> Result<T, E>
where
    E: From<crate::Error>,
{
    with_statement_txn_lifted(state, identity, ctx, implicit, E::from, work).await
}

/// [`with_statement_txn`] for a caller whose error type takes a
/// `crate::Error` through `lift`.
pub async fn with_statement_txn_lifted<T, E>(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    ctx: &DmlTxnCtx<'_>,
    implicit: bool,
    lift: impl Fn(crate::Error) -> E,
    work: impl AsyncFnOnce(&DmlTxnCtx<'_>) -> Result<T, E>,
) -> Result<T, E> {
    if !implicit {
        return work(ctx).await;
    }
    scoped_system_txn(async {
        let txn = OpenSystemTxn::begin(state, identity.clone(), EventSource::ImplicitClient)
            .map_err(|error| lift(crate::Error::from(error)))?;
        let result = {
            let implicit_ctx = txn.txn_ctx().map_err(&lift)?;
            work(&implicit_ctx).await
        };
        match result {
            Ok(value) => {
                txn.commit()
                    .await
                    .map_err(|error| lift(crate::Error::from(error)))?;
                Ok(value)
            }
            Err(error) => {
                txn.rollback().await;
                Err(error)
            }
        }
    })
    .await
}
