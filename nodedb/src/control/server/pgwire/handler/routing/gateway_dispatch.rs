// SPDX-License-Identifier: BUSL-1.1

//! Gateway-based dispatch: routes tasks through `Gateway::execute` instead of
//! the old SQL-string `ForwardRequest` forwarding path.
//!
//! `should_forward_via_gateway` mirrors the old `remote_leader_for_tasks`
//! detection logic but returns a bool rather than the leader node id, because
//! the gateway handles the node selection internally.
//!
//! `dispatch_tasks_via_gateway` replaces `forward_sql`: each task is dispatched
//! via `gateway.execute(ctx, plan)` which ships pre-planned `PhysicalPlan` bytes
//! over QUIC via `ExecuteRequest`, rather than raw SQL text.

use pgwire::api::results::{FieldFormat, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::gateway::GatewayErrorMap;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::types::{ReadConsistency, TenantId, TraceId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::sqlstate_error;
use super::super::core::NodeDbPgHandler;
use super::super::plan::{PlanKind, multirow_payload_to_response};
use super::super::shape_encode;

/// Meter one gateway-forwarded task, once its response has already shaped
/// successfully — mirrors `calvin_dispatch::meter_calvin_task`, the sibling
/// remote-dispatch door that bypasses `dispatch_task_loop` the same way.
///
/// `rows` is `Some(shaped.rows.len())` when the response was decoded into
/// rows by the shaping step just above the call site, `None` for a
/// `Passthrough` shape (no decoded row count) or an empty-payload `OK` tag
/// (no row payload at all) — `meter_dispatch` charges one unit for `None`,
/// correct for a write or a zero-row read.
fn meter_gateway_task(
    state: &crate::control::state::SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::id::DatabaseId,
    plan: &crate::bridge::envelope::PhysicalPlan,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(plan);
    let scope = RequestAuthScope::builder(identity, state.auth_stores())
        .with_session_database(Some(database_id))
        .build();
    meter_dispatch(state, &scope, &info, rows);
}

/// Everything a gateway dispatch needs besides the tasks themselves.
///
/// These travel together because they all describe the same request: the
/// principal it runs as, the tenant and database it is scoped to, and how its
/// rows are shaped back to the client.
pub(super) struct GatewayDispatchParams<'a> {
    pub(super) identity: &'a AuthenticatedIdentity,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: nodedb_types::id::DatabaseId,
    pub(super) projection: Option<&'a OutputSchema>,
    pub(super) result_formats: &'a [FieldFormat],
    /// The requester's resolved context; its roles drive column-level
    /// redaction of the forwarded rows.
    pub(super) auth: &'a crate::control::security::auth_context::AuthContext,
}

impl NodeDbPgHandler {
    /// Returns `true` when every task targets a single remote leader and the
    /// gateway is available to forward them. This replaces the old
    /// `remote_leader_for_tasks` helper which returned the leader node id.
    pub(super) fn should_forward_via_gateway(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
    ) -> bool {
        if self.state.gateway.get().is_none() {
            return false;
        }
        let routing = match self.state.cluster_routing.as_ref() {
            Some(r) => r,
            None => return false,
        };
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;
        for task in tasks {
            let vshard_id = task.vshard_id.as_u32();
            let group_id = match routing.group_for_vshard(vshard_id) {
                Ok(g) => g,
                Err(_) => return false,
            };
            let info = match routing.group_info(group_id) {
                Some(i) => i,
                None => return false,
            };
            let leader = info.leader;

            // Task is local — don't forward.
            if leader == my_node {
                return false;
            }
            // Local replica acceptable for non-strong reads — don't forward.
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return false;
            }
            // No known leader — can't forward.
            if leader == 0 {
                return false;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                // Tasks fan out across multiple leaders — don't use gateway forward.
                Some(prev) if prev != leader => return false,
                _ => {}
            }
        }

        remote_leader.is_some()
    }

    /// Execute all tasks via the gateway. Each task's plan is dispatched
    /// through `gateway.execute()` which ships the pre-planned physical
    /// plan to the target node via `ExecuteRequest`.
    pub(super) async fn dispatch_tasks_via_gateway(
        &self,
        tasks: Vec<PhysicalTask>,
        authorized_tasks: crate::control::server::shared::authorization::AuthorizedTaskSet,
        params: GatewayDispatchParams<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let GatewayDispatchParams {
            identity,
            tenant_id,
            database_id,
            projection,
            result_formats,
            auth,
        } = params;
        // Resolved once for the whole forwarded task set, before the loop.
        let redaction = QueryRedaction::for_plans(tenant_id, auth, tasks.iter().map(|t| &t.plan));
        let gateway = self.state.gateway.get().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "55000".to_owned(),
                "gateway not available".to_owned(),
            )))
        })?;

        let gw_ctx = crate::control::gateway::core::QueryContext {
            tenant_id,
            trace_id: TraceId::generate(),
            database_id,
            txn_id: None,
        };

        let mut responses: Vec<Response> = Vec::with_capacity(tasks.len());
        for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks.into_tasks()) {
            let payloads = gateway
                .execute(&gw_ctx, authorized_task)
                .await
                .map_err(|e| {
                    let (code, msg) = GatewayErrorMap::to_pgwire(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        code.to_owned(),
                        msg,
                    )))
                })?;

            if payloads.is_empty() {
                responses.push(Response::Execution(Tag::new("OK")));
                meter_gateway_task(&self.state, identity, database_id, &task.plan, None);
            } else {
                // One task can yield several payloads (e.g. a multi-page
                // scan). Metered once per task below, on the total row count
                // across every payload — never per payload, or a single task
                // would be billed multiple times.
                let mut task_rows: Option<u64> = None;
                for payload in &payloads {
                    match compose::shape_payload_no_plan(
                        payload,
                        PlanKind::MultiRow,
                        projection,
                        Some(redaction.ctx(&self.state.redaction)),
                    )
                    .map_err(|e| sqlstate_error("XX000", e.message()))?
                    {
                        ShapeOutcome::Rows(shaped) => {
                            task_rows = Some(task_rows.unwrap_or(0) + shaped.rows.len() as u64);
                            let (response, notice) =
                                shape_encode::shaped_query_response(shaped, result_formats);
                            // The gateway has no `addr` to route a NOTICE to; the
                            // MultiRow shape never carries one, so assert loudly
                            // rather than silently swallowing.
                            debug_assert!(
                                notice.is_none(),
                                "MultiRow gateway response must not carry a NOTICE"
                            );
                            responses.push(response);
                        }
                        ShapeOutcome::Passthrough => {
                            responses.push(multirow_payload_to_response(payload).response);
                        }
                    }
                }
                meter_gateway_task(&self.state, identity, database_id, &task.plan, task_rows);
            }
        }

        if responses.is_empty() {
            responses.push(Response::Execution(Tag::new("OK")));
        }

        Ok(responses)
    }
}
