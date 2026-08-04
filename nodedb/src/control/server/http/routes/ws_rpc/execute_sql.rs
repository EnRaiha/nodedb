// SPDX-License-Identifier: BUSL-1.1

//! SQL execution via the SPSC gateway for WebSocket RPC.

use std::sync::Arc;

use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId};

/// Execute SQL and return result as JSON.
///
/// Routes through the gateway when available (cluster-aware dispatch);
/// falls back to direct local SPSC dispatch on single-node boot before
/// the gateway is initialised.
pub async fn execute_sql(
    shared: &Arc<SharedState>,
    query_ctx: &crate::control::planner::context::QueryContext,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    trace_id: TraceId,
) -> crate::Result<serde_json::Value> {
    let tenant_id = identity.tenant_id;
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_database(identity, database_id, &emitter)?;

    // Quota enforcement — reject before planning or dispatch.
    shared.check_tenant_quota(tenant_id)?;

    // The RPC-selected database is authoritative for RLS variables — passed
    // as the session database so `scope.database_id()` resolves to exactly
    // `database_id` rather than falling back through `identity`'s default.
    let scope = RequestAuthScope::builder(identity, &shared.scope_grants)
        .with_session_database(Some(database_id))
        .build();
    let (clean_sql, scope) =
        crate::control::server::session_auth::apply_per_query_on_deny(sql, scope);
    // Planning and lease admission run as one retried unit so a descriptor
    // drain starting between them is absorbed rather than surfaced. The scope
    // is retained through every orchestrated or Data-Plane execution and
    // response decode below.
    let admission = plan_authorize_and_admit(PlanAdmissionRequest {
        state: shared,
        query_ctx,
        scope: &scope,
        sql: &clean_sql,
        trace_id,
    })
    .await?;
    let tasks = admission.tasks;
    let authorized_tasks = admission.authorized_tasks.into_tasks();
    let _lease_scope = admission.lease_scope;

    let _request = shared.tenant_request_guard(tenant_id);

    let mut results = Vec::new();
    for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks) {
        // `INSERT ... SELECT` is orchestrated on the Control Plane (fresh,
        // registered surrogate per target row + atomic `BatchInsert`), never
        // dispatched to the Data Plane as a single op.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. },
        ) = &task.plan
        {
            match crate::control::insert_select::run_authorized_insert_select(
                shared,
                authorized_task,
            )
            .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            continue;
        }

        // Autocommit `MERGE` is orchestrated on the Control Plane (fresh,
        // registered surrogate per NOT-MATCHED insert row + atomic apply),
        // never dispatched to the Data Plane as a single op.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::Merge {
                target_collection: _,
                source_collection: _,
                source_alias: _,
                target_join_col: _,
                source_join_col: _,
                clauses: _,
                returning: _,
                resolve_only: false,
                resolved_inserts: None,
                source_rows: _,
            },
        ) = &task.plan
        {
            match crate::control::merge_orchestrator::run_authorized_merge(shared, authorized_task)
                .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            continue;
        }

        // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control
        // Plane (source scanned on its own core + shipped into the plan), never
        // dispatched to the Data Plane as a single op reading a possibly-
        // non-resident source.
        if let crate::bridge::envelope::PhysicalPlan::Document(
            nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                target_collection: _,
                source_collection: _,
                source_alias: _,
                target_join_col: _,
                source_join_col: _,
                updates: _,
                target_filters: _,
                returning: _,
                resolve_only: false,
                source_rows: None,
            },
        ) = &task.plan
        {
            match crate::control::update_from_join_orchestrator::run_authorized_update_from_join(
                shared,
                authorized_task,
            )
            .await
            {
                Ok(resp) => {
                    let payload = resp.payload.to_vec();
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            continue;
        }

        let payloads: crate::Result<Vec<Vec<u8>>> = match shared.gateway.get() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id: task.tenant_id,
                    trace_id,
                    database_id: task.database_id,
                    txn_id: None,
                };
                gw.execute(&gw_ctx, authorized_task).await
            }
            None => {
                // Single-node boot: gateway not yet initialised — dispatch locally.
                crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
                    shared,
                    authorized_task,
                    trace_id,
                )
                .await
                .map(|r| vec![r.payload.to_vec()])
            }
        };

        match payloads {
            Ok(vecs) => {
                for payload in vecs {
                    if !payload.is_empty() {
                        let json =
                            crate::data::executor::response_codec::decode_payload_to_json(&payload);
                        match sonic_rs::from_str::<serde_json::Value>(&json) {
                            Ok(v) => results.push(v),
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    match results.len() {
        0 => Ok(serde_json::Value::Null),
        1 => Ok(results
            .into_iter()
            .next()
            .unwrap_or(serde_json::Value::Null)),
        _ => Ok(serde_json::Value::Array(results)),
    }
}
