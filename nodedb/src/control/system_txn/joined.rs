// SPDX-License-Identifier: BUSL-1.1

//! A trigger body that joins its triggering statement's transaction.
//!
//! A BEFORE, INSTEAD OF or SYNC AFTER body runs inside the statement that
//! fired it. Its writes stage into the statement's session, so the statement
//! and its bodies commit in one redo record: a failed statement leaves no body
//! write, and a failed body fails the statement.
//!
//! The body opens a savepoint in that transaction when it begins. A body that
//! succeeds releases it. A body that fails rolls back to it, so an exception
//! handler in the body, or the statement, sees the transaction as it was
//! before the body ran.
//!
//! The body stages with source `Trigger`. The overlay tags the rows it stages
//! (see the Data Plane's row tags), and the redo record lists them, so they
//! fire no trigger when the statement commits.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, savepoint_ops};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::TenantId;

use super::live::{OpenSystemTxn, TxnSession, savepoint_error};

/// Numbers every body savepoint, so nested and sibling bodies never share a
/// name.
static BODY_SAVEPOINTS: AtomicU64 = AtomicU64::new(0);

impl<'s> OpenSystemTxn<'s> {
    /// Join the transaction `ctx` names: open a savepoint in it for one
    /// trigger body. The body's writes stage under `Trigger`.
    pub async fn join(
        state: &'s SharedState,
        identity: AuthenticatedIdentity,
        ctx: &'s DmlTxnCtx<'s>,
        tenant_id: TenantId,
    ) -> crate::Result<Self> {
        let savepoint = format!(
            "__trigger_body_{}",
            BODY_SAVEPOINTS.fetch_add(1, Ordering::Relaxed)
        );
        let txn = Self::on_session(
            state,
            TxnSession::Joined {
                ctx,
                savepoint: savepoint.clone(),
                tenant_id,
            },
            identity,
            EventSource::Trigger,
        );
        savepoint_ops::run_savepoint(
            ctx.sessions,
            ctx.session_id,
            tenant_id,
            &txn.dp(),
            &savepoint,
        )
        .await
        .map_err(savepoint_error)?;
        Ok(txn)
    }

    /// End a joined body that succeeded: keep its writes in the statement's
    /// transaction.
    pub(super) fn release_joined(&self, session: TxnSession<'s>) -> crate::Result<()> {
        let TxnSession::Joined { ctx, savepoint, .. } = session else {
            return Err(crate::Error::Internal {
                detail: "a system transaction released a savepoint it does not hold".into(),
            });
        };
        savepoint_ops::run_release_savepoint(ctx.sessions, ctx.session_id, &savepoint)
            .map_err(savepoint_error)
    }

    /// End a joined body that failed: rewind the statement's transaction to
    /// before the body, then drop the savepoint.
    pub(super) async fn rollback_joined(&self, session: TxnSession<'s>) {
        let TxnSession::Joined {
            ctx,
            savepoint,
            tenant_id,
        } = session
        else {
            return;
        };
        let rewound = savepoint_ops::run_rollback_to_savepoint(
            ctx.sessions,
            ctx.session_id,
            tenant_id,
            &self.dp(),
            &savepoint,
        )
        .await
        .and_then(|()| {
            savepoint_ops::run_release_savepoint(ctx.sessions, ctx.session_id, &savepoint)
        });
        // The statement fails with the body's error, and its own rollback
        // discards every write the rewind cannot reach.
        if let Err(error) = rewound {
            tracing::error!(
                error = %savepoint_error(error),
                "rewinding a failed trigger body's writes failed"
            );
        }
    }
}
