// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane dispatch for DDL and DSL statements a user issued.
//!
//! These statements have a principal behind them, so they take the authorized
//! door: the plan is authorized into a capability, row-level security is
//! applied, and the capability is what reaches storage. Statement handlers use
//! this instead of the system door, which exists only for work no user asked
//! for.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::authorization::authorize_task_set;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::sync_dispatch::dispatch_authorized;

/// Authorize `plan` for `identity`, apply row-level security, and dispatch it.
///
/// Returns the Data-Plane payload. Authorization failures and policy refusals
/// surface as typed errors before anything reaches storage.
pub(crate) async fn dispatch_for_identity(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    timeout: Duration,
) -> crate::Result<Vec<u8>> {
    let mut plan = plan;
    let auth_ctx = crate::control::server::session_auth::context::build_auth_context(identity);
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        identity.tenant_id.as_u64(),
        &mut plan,
        &state.rls,
        &auth_ctx,
    )?;

    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        vshard_id: VShardId::from_collection_in_database(database_id, collection),
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    let authorized = authorize_task_set(
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
        detail: "authorization returned an empty capability set".into(),
    })?;

    dispatch_authorized(state, authorized, collection, timeout).await
}
