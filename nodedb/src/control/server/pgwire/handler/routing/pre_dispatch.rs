// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch routing gates for pgwire planned task sets.

use pgwire::api::results::{FieldFormat, Response};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::plan_needs_implicit_edge_recon;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;

use super::placement::TaskPlacement;
use super::planning::{consistency_for_tasks, has_replicated_writes};
use super::result_shaping::ResultShaping;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;

/// How long a linearizable read waits for a quorum to confirm this node still
/// leads the group. Several election timeouts (150-300ms), so an ordinary
/// round trip always fits and a partition is reported rather than hung on.
const LEADERSHIP_CONFIRM_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(750);

/// No leader is known for a group whose read requires one.
fn no_serving_leader() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER.to_owned(),
        "no leader is currently serving this range; retry".to_owned(),
    )))
}

impl NodeDbPgHandler {
    /// Prove against a quorum that this node still leads `group_id`.
    ///
    /// The routing table said so, but it is a cached view: a partitioned
    /// leader keeps its entry long after the rest of the cluster has elected
    /// a successor. Reads served on that entry alone return state the new
    /// leader has already moved past.
    async fn confirm_local_leadership(&self, group_id: u64) -> PgWireResult<()> {
        let Some(confirmer) = self.state.read_index_confirmer.get() else {
            // Reached only if a clustered node routed here before `start_raft`
            // published the confirmer. Refusing is retriable; serving would be
            // the unproven read this exists to prevent.
            return Err(no_serving_leader());
        };
        use crate::control::cluster::read_index::ReadIndexRefusal;
        match confirmer
            .confirm(group_id, LEADERSHIP_CONFIRM_TIMEOUT)
            .await
        {
            Ok(_read_index) => Ok(()),
            Err(ReadIndexRefusal::NotLeader) => Err(no_serving_leader()),
            Err(ReadIndexRefusal::Timeout { waited_ms }) => {
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    nodedb_types::error::sqlstate::STALE_READ_NOT_LEADER.to_owned(),
                    format!("no quorum confirmed leadership within {waited_ms}ms; retry"),
                ))))
            }
        }
    }

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
        if has_orchestrated_dml(tasks) {
            return Ok(None);
        }
        let consistency = consistency_for_tasks(&self.sessions, tasks, session_id);
        let needs_confirmed_leader = consistency.requires_leader() && !has_replicated_writes(tasks);
        match self.placement_for_tasks(tasks, consistency, needs_confirmed_leader) {
            TaskPlacement::Local => return Ok(None),
            TaskPlacement::LocalLeader { group_id } => {
                self.confirm_local_leadership(group_id).await?;
                return Ok(None);
            }
            TaskPlacement::NoLeader => return Err(no_serving_leader()),
            TaskPlacement::Gateway => {}
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
