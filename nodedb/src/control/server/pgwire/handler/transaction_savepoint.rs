// SPDX-License-Identifier: BUSL-1.1

//! Savepoint and deferred-offset adapters for `NodeDbPgHandler`.
//!
//! Thin pgwire shims over the protocol-neutral savepoint orchestrator
//! (`control/server/shared/session/savepoint_ops.rs`): they parse the wire
//! statement, drive the neutral op, and shape the tag / SQLSTATE. The overlay
//! marker capture/decode and the COMMIT OFFSET parsing live in the neutral
//! core.

use std::collections::HashMap;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::TransactionState;
use crate::control::server::shared::session::savepoint_ops::{
    self, DeferredOffsetCmd, SavepointError,
};

use super::core::NodeDbPgHandler;
use super::transaction_cmds::PgwireTxnDp;

/// Map a neutral savepoint error to the pgwire error the pre-extraction path
/// emitted (`25P01` outside a transaction, `3B001` for an unknown savepoint).
fn savepoint_error_to_pgerror(e: &SavepointError) -> PgWireError {
    let (code, message) = match e {
        SavepointError::NoActiveTransaction => (
            "25P01",
            "SAVEPOINT can only be used in transaction blocks".to_owned(),
        ),
        SavepointError::NotFound { message } => ("3B001", message.clone()),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

impl NodeDbPgHandler {
    /// Handle deferred COMMIT OFFSET inside a transaction block.
    ///
    /// Returns `Some(response)` if handled, `None` if not a deferred offset commit.
    pub(super) fn try_handle_deferred_offset(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
        upper: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        let cmd = savepoint_ops::parse_deferred_offset(sql_trimmed, upper)?;
        if self.sessions.transaction_state(addr) != TransactionState::InBlock {
            return None;
        }
        let tenant_id = identity.tenant_id.as_u64();

        match cmd {
            DeferredOffsetCmd::Single {
                stream,
                group,
                partition_id,
                lsn,
            } => {
                self.sessions.defer_offset_commit(
                    addr,
                    tenant_id,
                    stream,
                    group,
                    partition_id,
                    lsn,
                );
                Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]))
            }
            DeferredOffsetCmd::Batch { stream, group } => {
                if let Some(buffer) = self.state.cdc_router.get_buffer(tenant_id, &stream) {
                    let events = buffer.read_from_lsn(0, usize::MAX);
                    let mut latest: HashMap<u32, u64> = HashMap::new();
                    for e in &events {
                        let entry = latest.entry(e.partition).or_insert(0);
                        if e.lsn > *entry {
                            *entry = e.lsn;
                        }
                    }
                    for (pid, lsn) in latest {
                        self.sessions.defer_offset_commit(
                            addr,
                            tenant_id,
                            stream.clone(),
                            group.clone(),
                            pid,
                            lsn,
                        );
                    }
                }
                Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]))
            }
        }
    }

    /// Handle SAVEPOINT <name>.
    pub(super) async fn handle_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().nth(1).unwrap_or("sp");
        let dp = PgwireTxnDp { handler: self };
        match savepoint_ops::run_savepoint(&self.sessions, addr, identity.tenant_id, &dp, sp_name)
            .await
        {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }

    /// Handle RELEASE SAVEPOINT <name>.
    pub(super) fn handle_release_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
        match savepoint_ops::run_release_savepoint(&self.sessions, addr, sp_name) {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("RELEASE"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }

    /// Handle ROLLBACK TO SAVEPOINT <name>.
    pub(super) async fn handle_rollback_to_savepoint(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed.split_whitespace().last().unwrap_or("sp");
        let dp = PgwireTxnDp { handler: self };
        match savepoint_ops::run_rollback_to_savepoint(
            &self.sessions,
            addr,
            identity.tenant_id,
            &dp,
            sp_name,
        )
        .await
        {
            Ok(()) => Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]),
            Err(e) => Err(savepoint_error_to_pgerror(&e)),
        }
    }
}
