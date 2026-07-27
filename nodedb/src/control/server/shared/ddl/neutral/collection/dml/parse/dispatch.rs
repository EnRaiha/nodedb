// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::{
    AuthorizationError, AuthorizedTask, AuthorizedTaskSet, authorize_collection, authorize_task_set,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::{
    DmlTxnCtx, InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::types::ddl_err;

/// Dispatch a plan to WAL + Data Plane, returning an error response on failure.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn dispatch_plan(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    vshard_id: crate::types::VShardId,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id: identity.tenant_id,
        database_id,
        vshard_id,
        plan,
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id: None,
    };
    let authorized = match authorize_final_task(state, identity, &task) {
        Ok(authorized) => authorized,
        Err(error) => return Some(Err(error)),
    };

    if let Err(error) =
        crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
            state,
            authorized,
            TraceId::ZERO,
        )
        .await
    {
        return Some(Err(ddl_err("XX000", error.to_string())));
    }
    None
}

/// Authorize a write target before triggers, sequences, or catalog reads run.
pub(in crate::control::server::shared::ddl::neutral::collection) fn authorize_write_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
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
    .map_err(authorization_error_to_ddl)
}

/// Plan SQL through nodedb-sql, authorize the final task set, and dispatch it.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<(), DdlError> {
    let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
    let (mut tasks, _output_schema) = query_ctx
        .plan_sql(sql, tenant_id, database_id)
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;

    // The final set includes implicit graph-edge writes and must be authorized
    // before Calvin classification, transaction staging, or local dispatch.
    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| ddl_err("XX000", error.to_string()))?;

    let authorized_tasks = authorize_final_task_set(state, identity, &tasks)?;

    if state.sequencer_inbox.get().is_some()
        && matches!(
            crate::control::planner::calvin::classify_dispatch(
                &tasks,
                &std::collections::BTreeSet::new(),
            ),
            crate::control::planner::calvin::DispatchClass::MultiShard { .. }
        )
    {
        crate::control::planner::calvin::dispatch_authorized_tasks_to_calvin(
            state,
            authorized_tasks,
            tenant_id,
            crate::control::planner::calvin::CrossShardTxnMode::Strict,
            crate::control::planner::calvin::TxnDispatchPosition::Autocommit,
            &[],
            None,
        )
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;
        return Ok(());
    }

    for (task, initial_authorized) in tasks.into_iter().zip(authorized_tasks.into_tasks()) {
        let routed = route_in_tx_write(
            state,
            txn_ctx.sessions,
            txn_ctx.session_id,
            task,
            |staged| {
                let authorized = authorize_final_task_crate_error(state, identity, &staged);
                async move {
                    crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                        state,
                        authorized?,
                        TraceId::ZERO,
                    )
                    .await
                }
            },
        )
        .await;

        let task = match routed {
            Ok(InTxnRoute::Read(task)) => *task,
            Ok(InTxnRoute::Buffered) | Ok(InTxnRoute::Staged(_)) => {
                drop(initial_authorized);
                continue;
            }
            Err(StagingGateError::Dispatch(error)) => {
                return Err(ddl_err("XX000", error.to_string()));
            }
            Err(StagingGateError::Rejected { code }) => {
                let (_, sqlstate, message) = match code {
                    Some(code) => error_code_to_sqlstate(&code),
                    None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
                };
                return Err(ddl_err(sqlstate, message));
            }
        };

        drop(initial_authorized);
        let authorized = authorize_final_task(state, identity, &task)?;
        let response =
            crate::control::server::dispatch_utils::dispatch_authorized_autocommit_write(
                state,
                authorized,
                TraceId::ZERO,
            )
            .await
            .map_err(|error| ddl_err("XX000", error.to_string()))?;

        if response.status == crate::bridge::envelope::Status::Error {
            let detail = match response.error_code.as_deref() {
                Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
                Some(other) => format!("{other:?}"),
                None => String::from_utf8_lossy(&response.payload).into_owned(),
            };
            let sqlstate = if detail.to_lowercase().contains("unique") {
                "23505"
            } else {
                "XX000"
            };
            return Err(ddl_err(sqlstate, detail));
        }
    }
    Ok(())
}

fn authorize_final_task_set(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
) -> Result<AuthorizedTaskSet, DdlError> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(identity, tasks, &state.permissions, &state.roles, &emitter)
        .map_err(authorization_error_to_ddl)
}

fn authorize_final_task(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    task: &nodedb_physical::physical_task::PhysicalTask,
) -> Result<AuthorizedTask, DdlError> {
    authorize_final_task_set(state, identity, std::slice::from_ref(task))?
        .into_tasks()
        .into_iter()
        .next()
        .ok_or_else(|| ddl_err("XX000", "authorization returned no task capability"))
}

fn authorize_final_task_crate_error(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    task: &nodedb_physical::physical_task::PhysicalTask,
) -> crate::Result<AuthorizedTask> {
    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_task_set(
        identity,
        std::slice::from_ref(task),
        &state.permissions,
        &state.roles,
        &emitter,
    )
    .map_err(crate::Error::from)?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned no task capability".into(),
    })
}

fn authorization_error_to_ddl(error: AuthorizationError) -> DdlError {
    DdlError {
        sqlstate: nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE.to_owned(),
        message: error.resource().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TenantId;

    #[test]
    fn authorization_denial_preserves_insufficient_privilege_sqlstate() {
        let error = AuthorizationError::new(
            TenantId::new(1),
            "permission denied on collection".to_owned(),
        );
        let ddl_error = authorization_error_to_ddl(error);

        assert_eq!(ddl_error.sqlstate, "42501");
        assert!(ddl_error.message.contains("permission denied"));
    }
}
