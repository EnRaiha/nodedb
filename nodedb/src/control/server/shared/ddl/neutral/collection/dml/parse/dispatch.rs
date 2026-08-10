// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use crate::control::planner::context::PlanSecurityContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::types::error_to_sqlstate;
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::request::MaterializedShapeRequest;
use crate::control::server::response_shape::types::{PlanKind, ShapedRows};
use crate::control::server::shared::authorization::{
    AuthorizationError, AuthorizedTask, AuthorizedTaskSet, authorize_collection, authorize_task_set,
};
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::returning;
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
///
/// Returns the rows a `RETURNING` clause on `sql` produced, empty when the
/// statement carries none. The rows are decoded from the Data Plane's own
/// response — the STORED post-image — and are redacted before they leave, so
/// this path answers `RETURNING` exactly as the pgwire planner does rather than
/// echoing back the values the caller submitted.
pub(in crate::control::server::shared::ddl::neutral::collection) async fn plan_and_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: nodedb_types::TenantId,
    database_id: crate::types::DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    // The clause is stripped from the rebuilt statement before planning and
    // re-attached to each plan below — the planner itself does not parse it.
    let (sql, returning_spec) = returning::strip_returning(sql).map_err(|error| {
        let (_, sqlstate, message) = error_to_sqlstate(&error);
        ddl_err(sqlstate, message)
    })?;
    let sql = sql.as_str();
    // This is a client statement — the object-literal `INSERT INTO c { … }` and
    // `UPSERT` forms land here after being rewritten to standard SQL — so it
    // plans under the requester's own scope, the same one it is authorized and
    // metered as below. Planning it as the system would apply no row policy to
    // it: read filters would not be injected, and the write gates would decide
    // nothing, on a transport a client can reach directly.
    //
    // Injection happens HERE, before the task set is consumed: implicit-edge
    // extraction, authorization, staging, and dispatch all read `tasks` after
    // this point, and injecting later would hand them un-injected copies.
    let (mut tasks, versions) = {
        let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
        let permission_cache = state.permission_cache.read().await;
        let sec = PlanSecurityContext {
            identity,
            auth: scope.auth(),
            rls_store: &state.rls,
            redaction_store: &state.redaction,
            permissions: &state.permissions,
            roles: &state.roles,
            permission_cache: Some(&*permission_cache),
        };
        let query_ctx = crate::control::planner::context::QueryContext::for_state(state);
        let (tasks, _output_schema, versions, _) = query_ctx
            .plan_sql_with_rls_and_versions(sql, tenant_id, database_id, &sec, false)
            .await
            .map_err(|error| {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                ddl_err(sqlstate, message)
            })?;
        (tasks, versions)
    };

    // Attach the projection to every planned write, refusing any insert shape
    // that has nowhere to carry it rather than dropping the clause in silence.
    if let Some(ref spec) = returning_spec {
        for task in &mut tasks {
            returning::refuse_unprojectable_insert_returning(&task.plan).map_err(|error| {
                let (_, sqlstate, message) = error_to_sqlstate(&error);
                ddl_err(sqlstate, message)
            })?;
            returning::inject_returning_spec(&mut task.plan, spec.clone());
        }
    }

    // Extraction marks catalog state and allocates surrogates. Reject an
    // unauthorized original DML task set before either side effect can occur.
    let _preauthorized_tasks = authorize_final_task_set(state, identity, &tasks)?;

    // The final set includes implicit graph-edge writes and must be authorized
    // before descriptor admission, Calvin classification, transaction staging,
    // or local dispatch.
    crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
        TraceId::ZERO,
    )
    .await
    .map_err(|error| ddl_err("XX000", error.to_string()))?;

    // The entries cover the row images every cross-shard balance this pass
    // settled was folded from. They travel on the dispatch read-set so the
    // Calvin OCC check aborts, before any row moves, if those images have been
    // written since.
    let sum_target_reads =
        crate::control::planner::materialized_sum::resolve_materialized_sum_targets(
            state,
            &mut tasks,
            tenant_id,
            database_id,
            TraceId::ZERO,
        )
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;

    crate::control::planner::materialized_sum::append_cross_shard_balance_tasks(
        state,
        &mut tasks,
        tenant_id,
        database_id,
    )
    .map_err(|error| ddl_err("XX000", error.to_string()))?;

    let authorized_tasks = authorize_final_task_set(state, identity, &tasks)?;
    // Admission follows final authorization so an implicit-edge target denied
    // by policy does not consume a descriptor lease. The scope remains live
    // through expansion's successors, transaction staging, and dispatch below.
    let plan_lease_scope =
        Arc::new(state.acquire_plan_lease_scope(&versions).map_err(|error| {
            let (_, sqlstate, message) = error_to_sqlstate(&error);
            ddl_err(sqlstate, message)
        })?);

    if state.sequencer_inbox.get().is_some()
        && matches!(
            crate::control::planner::calvin::classify_dispatch(
                &tasks,
                &crate::control::planner::calvin::read_vshards_of(&sum_target_reads),
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
            &sum_target_reads,
            None,
        )
        .await
        .map_err(|error| ddl_err("XX000", error.to_string()))?;
        // A cross-shard Calvin dispatch returns no per-task payload here, so
        // there is no stored row to project. Refused rather than answered with
        // an empty row set, which would read as "the write matched nothing".
        if returning_spec.is_some() {
            return Err(ddl_err(
                "0A000",
                "RETURNING is not supported on a write that spans multiple shards",
            ));
        }
        return Ok(Vec::new());
    }

    let mut returned_rows: Option<ShapedRows> = None;
    let statement_buffer_start = txn_ctx.sessions.buffered_task_count(txn_ctx.session_id);
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

        if txn_ctx.sessions.buffered_task_count(txn_ctx.session_id) > statement_buffer_start
            && !txn_ctx.sessions.attach_tx_lease_scope_since(
                txn_ctx.session_id,
                statement_buffer_start,
                Arc::clone(&plan_lease_scope),
            )
        {
            return Err(ddl_err(
                "XX000",
                "internal error: failed to retain descriptor leases for buffered transaction tasks",
            ));
        }

        let task = match routed {
            Ok(InTxnRoute::Read(task)) => *task,
            Ok(InTxnRoute::Buffered) | Ok(InTxnRoute::Staged(_)) => {
                drop(initial_authorized);
                // A buffered/staged write produces its rows at COMMIT, not
                // here, so the clause cannot be answered on this path. Refused
                // through the shared rule so this transport's message is the
                // one the pgwire and native loops give for the same limitation.
                if returning_spec.is_some() {
                    let (_, sqlstate, message) =
                        error_to_sqlstate(&returning::in_transaction_returning_unsupported());
                    return Err(ddl_err(sqlstate, message));
                }
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

        // Shape the STORED rows the write returned, redacted for the caller —
        // the same choke point the pgwire dispatch loop uses, so a redaction
        // policy masks identically on both transports.
        if returning_spec.is_some() {
            let scope = RequestAuthScope::for_database(identity, state.auth_stores(), database_id);
            let redaction = QueryRedaction::for_plan(tenant_id, scope.auth(), &task.plan);
            let outcome = shape_response_materialized(MaterializedShapeRequest {
                payload: response.payload.as_bytes(),
                plan: &task.plan,
                plan_kind: PlanKind::ReturningRows,
                projection: None,
                state,
                database_id,
                tenant_id,
                redaction: Some(redaction.ctx(&state.redaction)),
            })
            .map_err(|error| ddl_err("XX000", error.message().to_string()))?;
            // Folded rather than pushed: a statement is ONE result set, however
            // many tasks it planned to.
            if let ShapeOutcome::Rows(shaped) = outcome {
                match returned_rows {
                    Some(ref mut accumulated) => accumulated.append(shaped),
                    None => returned_rows = Some(shaped),
                }
            }
        }
    }
    Ok(returned_rows.map(DdlResult::Rows).into_iter().collect())
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
