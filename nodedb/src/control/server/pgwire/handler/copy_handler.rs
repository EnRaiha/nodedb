// SPDX-License-Identifier: BUSL-1.1

//! pgwire COPY-protocol bridge for backup/restore wire surface.
//!
//! - `intent_to_response()` runs in `SimpleQueryHandler::do_query`
//!   when `backup::detect()` recognises a wire COPY shape. It either
//!   returns a `Response::CopyOut` whose stream pulls bytes from
//!   `backup_tenant`, or registers per-connection state and returns
//!   `Response::CopyIn` so the client begins streaming bytes.
//! - `CopyHandler::on_copy_data` accumulates client bytes (size-capped).
//! - `CopyHandler::on_copy_done` validates the envelope and dispatches
//!   `restore::restore_tenant`.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use nodedb_types::error::sqlstate as ss;

use async_trait::async_trait;
use futures::stream;
use futures::{Sink, SinkExt};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone};

use crate::control::backup;
use crate::control::backup::CopyIntent;
use crate::control::backup::state::AppendError;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::core::NodeDbPgHandler;

/// Hard cap on accumulated COPY IN bytes for one restore. 16 GiB matches
/// the envelope's default total cap; any larger payload is rejected
/// before it can drive unbounded server allocation.
const COPY_IN_CAP: u64 = 16 * 1024 * 1024 * 1024;

impl NodeDbPgHandler {
    /// Translate a recognised `CopyIntent` into a pgwire COPY response.
    pub(super) async fn intent_to_response(
        &self,
        identity: &AuthenticatedIdentity,
        addr: SocketAddr,
        intent: CopyIntent,
    ) -> PgWireResult<Response> {
        // Backup and restore both operate on a whole tenant — authorize
        // against the tenant-scoped `Backup` permission. Superuser bypasses
        // the grant check; everyone else needs `GRANT BACKUP ON TENANT`.
        let tenant_id = match &intent {
            CopyIntent::BackupTenant { tenant_id } => *tenant_id,
            CopyIntent::RestoreTenant { tenant_id, .. } => *tenant_id,
        };
        if !identity.is_superuser {
            let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
            let allowed = self.state.permissions.check_tenant(
                identity,
                Permission::Backup,
                TenantId::new(tenant_id),
                &self.state.roles,
                &emitter,
            );
            if !allowed {
                return Err(sqlstate(
                    ss::INSUFFICIENT_PRIVILEGE,
                    "permission denied: BACKUP permission on the tenant required",
                ));
            }
        }

        match intent {
            CopyIntent::BackupTenant { tenant_id } => {
                let bytes = backup::backup_tenant(&self.state, tenant_id)
                    .await
                    .map_err(internal)?;
                let copy_data = Ok(CopyData::new(bytes));
                let stream = stream::once(async move { copy_data });
                Ok(Response::CopyOut(CopyResponse::new(0, 0, stream)))
            }
            CopyIntent::RestoreTenant {
                tenant_id,
                dry_run,
                force,
            } => {
                self.restore_state.begin(
                    conn_id(&addr),
                    backup::RestorePending::new(tenant_id, dry_run, force, COPY_IN_CAP),
                );
                // Empty out-stream — server tells client "send me bytes".
                let empty = stream::empty();
                Ok(Response::CopyIn(CopyResponse::new(0, 0, empty)))
            }
        }
    }
}

/// CopyHandler shared by the factory's per-connection handler. Holds
/// only the `Arc<RestoreState>` it needs — the rest of the SharedState
/// is reachable via the cloned handle for the dispatch on `on_copy_done`.
pub struct NodeDbCopyHandler {
    pub state: Arc<SharedState>,
    pub restore_state: Arc<backup::RestoreState>,
}

#[async_trait]
impl CopyHandler for NodeDbCopyHandler {
    async fn on_copy_data<C>(&self, client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = conn_id(&client.socket_addr());
        match self.restore_state.append(id, &copy_data.data) {
            Ok(()) => Ok(()),
            Err(e @ AppendError::NotPending) => {
                Err(sqlstate(ss::FEATURE_NOT_SUPPORTED, &e.to_string()))
            }
            Err(e @ AppendError::OverCap { .. }) => {
                self.restore_state.cancel(id);
                Err(sqlstate(ss::PROGRAM_LIMIT_EXCEEDED, &e.to_string()))
            }
        }
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = conn_id(&client.socket_addr());
        let pending = self.restore_state.take(id).ok_or_else(|| {
            sqlstate(
                ss::FEATURE_NOT_SUPPORTED,
                "no restore pending on this connection",
            )
        })?;
        let stats = backup::restore_tenant(
            &self.state,
            pending.tenant_id,
            &pending.bytes,
            pending.dry_run,
            pending.force,
        )
        .await
        .map_err(internal)?;
        // pgwire does not auto-send CommandComplete after `on_copy_done`
        // returns Ok — the trait contract leaves message construction to
        // the handler. Send a `RESTORE TENANT N <op-count>` tag so the
        // client's COPY IN sink can complete.
        let rows =
            stats.documents + stats.kv_tables + stats.vectors + stats.timeseries + stats.edges;
        let tag = Tag::new("RESTORE TENANT").with_rows(rows);
        client
            .send(PgWireBackendMessage::CommandComplete(tag.into()))
            .await
            .map_err(|e| {
                sqlstate(
                    ss::INTERNAL_ERROR,
                    &format!("CommandComplete send failed: {e:?}"),
                )
            })?;
        // Leave the COPY-in-progress state so the next Sync from the
        // client gets dispatched normally. pgwire's `process_message`
        // only routes Sync via the `AwaitingSync` arm.
        client.set_state(PgWireConnectionState::AwaitingSync);
        Ok(())
    }
}

fn conn_id(addr: &SocketAddr) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    addr.hash(&mut h);
    h.finish()
}

fn sqlstate(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".into(),
        code.into(),
        message.into(),
    )))
}

fn internal(e: crate::Error) -> PgWireError {
    // Surface error string but never echo deserializer context — the
    // restore orchestrator already scrubs envelope errors. We pass
    // through everything else (RPC failures, dispatch errors).
    sqlstate(ss::INTERNAL_ERROR, &e.to_string())
}
