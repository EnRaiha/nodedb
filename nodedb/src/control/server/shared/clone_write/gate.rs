// SPDX-License-Identifier: BUSL-1.1

//! The dispatch capability gate: clone-write interception, clone-read
//! interception, and authorization merged into one step, so a caller can
//! only reach the Data-Plane dispatch boundary through
//! [`intercept_and_authorize`]. A caller holding a bare `AuthorizedTask` has
//! no way to produce a [`CloneCheckedTask`] itself, so an entry point that
//! forgets either clone hook fails to compile instead of silently bypassing it.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::security::audit::AuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission, required_permission};
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::server::shared::authorization::{AuthorizedTask, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{TraceId, VShardId};

use super::entry::{CloneWriteOutcome, maybe_intercept_clone_write};

/// A physical task that has passed clone-write interception, then
/// authorization, in that order. Only [`intercept_and_authorize`] can produce
/// one — every dispatch boundary that reaches the Data Plane consumes this
/// type instead of a bare `AuthorizedTask`.
///
/// Boxed so this stays small relative to [`Response`]: `AuthorizedTask` wraps
/// a `PhysicalTask`, whose `PhysicalPlan` is the largest enum in the crate, so
/// an unboxed field here would force [`CloneCheckedOutcome`] to size itself to
/// the bigger of the two variants either way.
pub struct CloneCheckedTask(Box<AuthorizedTask>);

impl CloneCheckedTask {
    /// Unwrap into the inner authorization capability, for a caller (e.g. Raft
    /// replication) that dispatches through a lower-level path than the ones
    /// gated on this type.
    pub fn into_authorized(self) -> AuthorizedTask {
        *self.0
    }

    pub fn tenant_id(&self) -> TenantId {
        self.0.tenant_id()
    }

    pub fn database_id(&self) -> DatabaseId {
        self.0.database_id()
    }

    pub fn vshard_id(&self) -> VShardId {
        self.0.vshard_id()
    }

    pub fn txn_id(&self) -> Option<crate::types::TxnId> {
        self.0.txn_id()
    }

    pub fn plan(&self) -> &PhysicalPlan {
        self.0.plan()
    }
}

/// Outcome of [`intercept_and_authorize`].
pub enum CloneCheckedOutcome {
    /// The clone-write hook fully handled the write; use this response.
    Handled(Response),
    /// Clear to dispatch (not clone-relevant, or the plan was retargeted).
    Proceed(CloneCheckedTask),
}

/// Inputs for [`intercept_and_authorize`] (and, with a `trace_id`, for
/// [`intercept_authorize_and_dispatch`]). Grouped into a struct because the
/// gate needs everything `maybe_intercept_clone_write` and `authorize_task_set`
/// each need — state, the task, the requester's identity and tenant, and the
/// authorization stores — which exceeds a readable positional argument count.
pub struct InterceptAndAuthorizeParams<'a> {
    pub state: &'a SharedState,
    pub task: PhysicalTask,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub permissions: &'a PermissionStore,
    pub roles: &'a RoleStore,
    pub emitter: &'a dyn AuditEmitter,
}

/// Clone-check, then authorize, one physical task — the single function that
/// can produce a [`CloneCheckedTask`].
///
/// `classify()` inside [`maybe_intercept_clone_write`] is `O(1)` with no I/O
/// for a read plan (`ANALYZE`, `COPY TO`, cursor reads never reach a catalog
/// lookup here). A write shape none of `document`/`kv`/`kv_insert` claims
/// does one collection lookup to decide whether it must be refused as an
/// unsupported clone write. Every caller pays this gate uniformly rather
/// than special-casing itself out of it.
pub async fn intercept_and_authorize(
    params: InterceptAndAuthorizeParams<'_>,
) -> crate::Result<CloneCheckedOutcome> {
    let InterceptAndAuthorizeParams {
        state,
        mut task,
        identity,
        tenant_id,
        permissions,
        roles,
        emitter,
    } = params;
    if let CloneWriteOutcome::Handled(resp) =
        maybe_intercept_clone_write(state, &mut task, identity, tenant_id).await?
    {
        return Ok(CloneCheckedOutcome::Handled(resp));
    }
    if required_permission(&task.plan) == Permission::Read
        && let super::super::clone_read::CloneReadOutcome::Handled(resp) =
            super::super::clone_read::maybe_intercept_clone_read(
                super::super::clone_read::CloneReadInterceptParams {
                    state,
                    task: &task,
                    identity,
                    tenant_id,
                    permissions,
                    roles,
                    emitter,
                },
            )
            .await?
    {
        return Ok(CloneCheckedOutcome::Handled(resp));
    }
    let authorized = authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        permissions,
        roles,
        emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned an empty capability set".into(),
    })?;
    Ok(CloneCheckedOutcome::Proceed(CloneCheckedTask(Box::new(
        authorized,
    ))))
}

/// Intercept, authorize, and dispatch one task to the Data Plane in one call —
/// the shape every read-only internal DDL scan needs (`ANALYZE`, `COPY TO`,
/// CHECK subquery evaluation, `VALIDATE TYPEGUARD`): no branching on the
/// capability besides "run it".
pub async fn intercept_authorize_and_dispatch(
    params: InterceptAndAuthorizeParams<'_>,
    trace_id: TraceId,
) -> crate::Result<Response> {
    let state = params.state;
    match intercept_and_authorize(params).await? {
        CloneCheckedOutcome::Handled(resp) => Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => {
            crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                state, checked, trace_id,
            )
            .await
        }
    }
}
