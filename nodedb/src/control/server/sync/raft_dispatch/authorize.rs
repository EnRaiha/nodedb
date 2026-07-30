// SPDX-License-Identifier: BUSL-1.1

//! Authorization for sync writes, before any plan reaches Raft or the Data
//! Plane. Both entry points fail closed on an absent identity: a sync write
//! with no authenticated principal has no tenant to be scoped to.

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::{
    AuthorizedTask, authorize_collection, authorize_task_set,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub fn authorize_sync_collection(
    state: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
) -> crate::Result<()> {
    let identity = identity.ok_or_else(|| crate::Error::RejectedAuthz {
        tenant_id,
        resource: "authenticated sync identity required".into(),
    })?;
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)
}

pub fn authorize_sync_task(
    state: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<AuthorizedTask> {
    let identity = identity.ok_or_else(|| crate::Error::RejectedAuthz {
        tenant_id,
        resource: "authenticated sync identity required".into(),
    })?;
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "sync authorization returned no capability".into(),
    })
}
