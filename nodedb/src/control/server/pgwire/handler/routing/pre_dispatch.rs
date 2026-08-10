// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch routing gates for pgwire planned task sets.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::plan_needs_implicit_edge_recon;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;

use super::planning::consistency_for_tasks;
use super::result_shaping::ResultShaping;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Route an implicit-edge dependent predicate through OLLP/Calvin when its
    /// catalog and session prerequisites require atomic edge maintenance.
    pub(super) async fn maybe_dispatch_implicit_edge_recon(
        &self,
        tasks: &[PhysicalTask],
        tenant_id: TenantId,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        result_formats: &[FieldFormat],
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let tx_state = self.sessions.transaction_state(session_id);
        if tx_state == crate::control::server::shared::session::TransactionState::InBlock
            || self.state.calvin_completion_registry.get().is_none()
        {
            return Ok(None);
        }

        let needs_recon =
            plan_needs_implicit_edge_recon(&self.state, tasks, tenant_id).map_err(|error| {
                let (severity, code, message) = error_to_sqlstate(&error);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        if needs_recon.is_none() {
            return Ok(None);
        }

        self.dispatch_calvin_multishard(
            tasks.to_vec(),
            tenant_id,
            super::calvin_dispatch::CalvinDispatchSession {
                identity,
                session_id,
                result_formats,
                auth,
            },
            // The implicit-edge recon gate fires before any materialized-sum
            // settlement is reachable on this path, so there is no settled
            // image read to carry.
            &[],
        )
        .await
        .map(Some)
    }

    /// Forward an ordinary remote-leader task set through the gateway.
    ///
    /// Unresolved multi-step DML remains local so its capability-bearing
    /// orchestrator can resolve the final plans before authorization.
    pub(super) async fn maybe_dispatch_tasks_via_gateway(
        &self,
        tasks: &[PhysicalTask],
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
        session_id: SessionId,
        shaping: ResultShaping<'_>,
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let ResultShaping {
            projection,
            formats: result_formats,
        } = shaping;
        let consistency = consistency_for_tasks(tasks);
        if has_orchestrated_dml(tasks) || !self.should_forward_via_gateway(tasks, consistency) {
            return Ok(None);
        }

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        let authorized_tasks = self.authorize_tasks(identity, tasks)?;
        self.dispatch_tasks_via_gateway(
            tasks.to_vec(),
            authorized_tasks,
            super::gateway_dispatch::GatewayDispatchParams {
                identity,
                tenant_id,
                database_id,
                projection,
                result_formats,
                auth,
            },
        )
        .await
        .map(Some)
    }
}

fn has_orchestrated_dml(tasks: &[PhysicalTask]) -> bool {
    tasks.iter().any(|task| {
        matches!(
            &task.plan,
            crate::bridge::envelope::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::InsertSelect { .. }
                    | nodedb_physical::physical_plan::DocumentOp::Merge {
                        resolve_only: false,
                        resolved_inserts: None,
                        ..
                    }
                    | nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
                        resolve_only: false,
                        source_rows: None,
                        ..
                    }
            )
        )
    })
}
