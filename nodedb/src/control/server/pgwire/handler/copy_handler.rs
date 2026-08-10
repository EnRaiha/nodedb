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
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};

use crate::control::backup;
use crate::control::backup::CopyIntent;
use crate::control::backup::state::AppendError;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::{ConnectionId, SessionId};
use crate::control::state::SharedState;
use crate::types::TenantId;
use nodedb_types::calvin::EngineTag;

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
        session_id: SessionId,
        intent: CopyIntent,
    ) -> PgWireResult<Response> {
        // Blacklist + account status, no rate limit: backup/restore is
        // admin-scoped bulk data movement, not the per-query traffic the
        // rate-limiter's cost table models, so charging it against a query
        // rate limit would throttle a legitimate restore. A blacklisted or
        // suspended/banned account must not be able to run backup or
        // restore, though — `check_blacklist_and_status` runs that half of
        // `check_request_admission`'s gate (plus the internal-service
        // exemption every other transport gets) using this connection's
        // real peer address.
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XX000".to_owned(),
                        "connection session metadata is unavailable".to_owned(),
                    )))
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let database_id = identity
            .default_database
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let peer_addr = peer_addr.to_string();
        let request = crate::control::security::request_scope::ClientRequestScope::for_database(
            identity,
            self.state.auth_stores(),
            database_id,
            &peer_addr,
        );
        crate::control::server::session_auth::check_blacklist_and_status(&self.state, &request)
            .map_err(|e| {
                let (severity, code, message) =
                    crate::control::server::pgwire::types::error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

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
                // A spent hard quota refuses the backup before it reads a
                // byte. The charge below is on the success path and so can
                // never be where a cap blocks anything.
                admit_backup_restore_quota(&self.state, request.scope(), tenant_id)
                    .map_err(internal)?;
                let bytes = backup::backup_tenant(&self.state, tenant_id)
                    .await
                    .map_err(internal)?;
                // Metered here, on the success path, before the response is
                // built below — there is no `PhysicalPlan` for a whole-tenant
                // backup, so the collection dimension is a synthetic
                // `tenant:<id>` marker rather than a real collection name.
                // `rows: None` — the backup produces a byte blob, not a row
                // count; `meter_dispatch` charges one unit for `None`.
                meter_backup_restore(&self.state, request.scope(), tenant_id, None);
                let copy_data = Ok(CopyData::new(bytes));
                let stream = stream::once(async move { copy_data });
                Ok(Response::CopyOut(CopyResponse::new(0, 0, stream)))
            }
            CopyIntent::RestoreTenant {
                tenant_id,
                dry_run,
                force,
            } => {
                let connection_id = match session_id {
                    SessionId::Connection(connection_id) => connection_id,
                    SessionId::LegacySocket(_) => {
                        return Err(sqlstate(
                            ss::INTERNAL_ERROR,
                            "COPY restore requires a typed connection",
                        ));
                    }
                };
                self.restore_state.begin(
                    connection_id.get(),
                    backup::RestorePending::new(
                        tenant_id,
                        dry_run,
                        force,
                        COPY_IN_CAP,
                        identity.clone(),
                    ),
                );
                // Empty out-stream — server tells client "send me bytes".
                let empty = stream::empty();
                Ok(Response::CopyIn(CopyResponse::new(0, 0, empty)))
            }
        }
    }
}

/// Meter one completed whole-tenant backup or restore.
///
/// Shared by the backup branch of `intent_to_response` and
/// `on_copy_done`'s restore completion below — both operate on a whole
/// tenant rather than a `PhysicalPlan`-shaped single-collection dispatch, so
/// this builds a [`PlanMeteringInfo`] directly via
/// [`PlanMeteringInfo::for_collection`] instead of extracting one from a
/// plan.
/// Refuse a backup/restore whose covering scope has already spent its cap.
///
/// The sibling of [`meter_backup_restore`], and it describes the request the
/// same way: a whole-tenant operation has no `PhysicalPlan`, so the collection
/// dimension is the synthetic `tenant:<id>` marker that function bills under.
/// A scope grant therefore only caps this if it was written against that same
/// marker — which is exactly the entitlement an operator would define to cap
/// backups.
fn admit_backup_restore_quota(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
) -> crate::Result<()> {
    if !state.metering_config.enabled {
        return Ok(());
    }
    let info = PlanMeteringInfo::for_collection(
        format!("tenant:{tenant_id}"),
        EngineTag::Meta,
        "sql",
        Permission::Backup,
    );
    crate::control::server::shared::quota_admission::admit_quota_for_dispatch(state, scope, &info)
}

fn meter_backup_restore(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    tenant_id: u64,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::for_collection(
        format!("tenant:{tenant_id}"),
        EngineTag::Meta,
        "sql",
        Permission::Backup,
    );
    meter_dispatch(state, scope, &info, rows);
}

/// CopyHandler shared by the factory's per-connection handler. Holds
/// only the `Arc<RestoreState>` it needs — the rest of the SharedState
/// is reachable via the cloned handle for the dispatch on `on_copy_done`.
pub struct NodeDbCopyHandler {
    pub state: Arc<SharedState>,
    pub restore_state: Arc<backup::RestoreState>,
    pub connection_id: ConnectionId,
}

#[async_trait]
impl CopyHandler for NodeDbCopyHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = self.connection_id.get();
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

    async fn on_copy_fail<C>(&self, _client: &mut C, _fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        cancel_restore(&self.restore_state, self.connection_id);
        sqlstate(ss::QUERY_CANCELED, "COPY restore aborted")
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let id = self.connection_id.get();
        let pending = self.restore_state.take(id).ok_or_else(|| {
            sqlstate(
                ss::FEATURE_NOT_SUPPORTED,
                "no restore pending on this connection",
            )
        })?;
        // Scope resolved here rather than only below, so the quota gate can
        // refuse the restore before it writes anything — the charge further
        // down is on the success path and so can never refuse anything.
        let database_id = pending
            .identity
            .default_database
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let scope = RequestAuthScope::for_database(
            &pending.identity,
            self.state.auth_stores(),
            database_id,
        );
        admit_backup_restore_quota(&self.state, &scope, pending.tenant_id).map_err(internal)?;

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
        // Metered on the success path, using the same `rows` count the
        // command tag below reports, and the same scope the quota gate above
        // resolved — `pending.identity` is the caller who opened this COPY IN
        // back in `intent_to_response`, carried through `RestorePending`
        // since this per-connection `CopyHandler` has no identity of its own.
        meter_backup_restore(&self.state, &scope, pending.tenant_id, Some(rows as u64));
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

fn cancel_restore(state: &backup::RestoreState, connection_id: ConnectionId) {
    state.cancel(connection_id.get());
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bridge::dispatch::Dispatcher;
    use crate::config::auth::AuthMode;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::wal::WalManager;

    fn test_handler() -> (NodeDbPgHandler, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (NodeDbPgHandler::new(state, AuthMode::Trust), dir)
    }

    fn identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "copy-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    /// The regression this admission check exists to prevent: before it
    /// existed, `intent_to_response` ran no blacklist or account-status
    /// check at all, so a blacklisted client could still run backup/restore.
    #[tokio::test]
    async fn intent_to_response_rejects_blacklisted_identity() {
        let (handler, _dir) = test_handler();
        let identity = identity(101);
        handler
            .state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let result = handler
            .intent_to_response(
                &identity,
                handler.session_id,
                CopyIntent::BackupTenant { tenant_id: 1 },
            )
            .await;

        assert!(
            result.is_err(),
            "a blacklisted identity must be rejected before backup/restore runs"
        );
    }

    /// A suspended JIT-provisioned account must not be able to run
    /// backup/restore, even though this door skips the rate limiter.
    #[tokio::test]
    async fn intent_to_response_rejects_suspended_account() {
        let (handler, _dir) = test_handler();
        let identity = identity(102);
        handler
            .state
            .auth_users
            .upsert(crate::control::security::jit::auth_user::AuthUserRecord {
                id: identity.user_id.to_string(),
                username: identity.username.clone(),
                email: String::new(),
                tenant_id: identity.tenant_id.as_u64(),
                provider: "test".into(),
                first_seen: 0,
                last_seen: 0,
                is_active: false,
                status: crate::control::security::auth_context::AuthStatus::Suspended,
                is_external: true,
                synced_claims: std::collections::HashMap::new(),
                escalation_suspensions: 0,
            })
            .expect("register suspended auth user");

        let result = handler
            .intent_to_response(
                &identity,
                handler.session_id,
                CopyIntent::BackupTenant { tenant_id: 1 },
            )
            .await;

        assert!(
            result.is_err(),
            "a suspended account must be rejected before backup/restore runs"
        );
    }

    #[test]
    fn copy_fail_cancels_only_the_exact_pending_restore() {
        let state = backup::RestoreState::new();
        let first = ConnectionId::new(1).unwrap_or_else(|_| unreachable!());
        let second = ConnectionId::new(2).unwrap_or_else(|_| unreachable!());
        state.begin(
            first.get(),
            backup::RestorePending::new(1, false, false, 16, identity(1)),
        );
        state.begin(
            second.get(),
            backup::RestorePending::new(1, false, false, 16, identity(2)),
        );

        cancel_restore(&state, first);

        assert_eq!(
            state.append(first.get(), b"x"),
            Err(AppendError::NotPending)
        );
        assert!(state.append(second.get(), b"x").is_ok());
    }

    /// `meter_backup_restore` is called unconditionally from both the backup
    /// success arm of `intent_to_response` and the restore success tail of
    /// `on_copy_done` — never from an error path in either (see the source
    /// above: it runs after `?` has already propagated any failure) — so
    /// covering it directly here exercises the same enabled/disabled and
    /// row-count behavior every call site relies on.
    #[test]
    fn metering_disabled_by_default_records_nothing() {
        let (handler, _dir) = test_handler();
        assert!(
            !handler.state.metering_config.enabled,
            "default is disabled"
        );
        let user = identity(1);
        let scope = RequestAuthScope::for_database(
            &user,
            handler.state.auth_stores(),
            crate::types::DatabaseId::DEFAULT,
        );

        meter_backup_restore(&handler.state, &scope, 7, Some(42));

        assert_eq!(handler.state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn enabled_backup_restore_records_one_event_for_the_tenant() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("construct shared state");
        // Sole owner at this point — construction below is the only clone.
        std::sync::Arc::get_mut(&mut state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
        let handler = NodeDbPgHandler::new(state, AuthMode::Trust);
        let user = identity(1);
        let scope = RequestAuthScope::for_database(
            &user,
            handler.state.auth_stores(),
            crate::types::DatabaseId::DEFAULT,
        );

        meter_backup_restore(&handler.state, &scope, 7, Some(42));

        let events = handler.state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "tenant:7");
        assert_eq!(events[0].engine, "meta");
        assert_eq!(events[0].tokens, 42);
    }
}
