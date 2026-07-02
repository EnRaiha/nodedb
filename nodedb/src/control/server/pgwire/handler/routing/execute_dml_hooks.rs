// SPDX-License-Identifier: BUSL-1.1

//! Pre-dispatch hook interception for the `dispatch_task_loop` write path:
//! BEFORE/INSTEAD OF trigger firing (with OLD-row fetch and probe-driven
//! event reclassification), truncate `restart_identity` extraction, and
//! clone CoW write-path interception. Split out of `execute.rs` to keep
//! that file under the file-size limit; behavior is unchanged — this is
//! the same code that used to run inline in the per-task dispatch loop.

use std::collections::HashMap;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::trigger::dml_hook::DmlWriteInfo;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::super::plan::PlanKind;

/// Outcome of running the pre-dispatch hooks for a single task.
pub(super) enum PreDispatchOutcome {
    /// The task was fully handled (trigger short-circuit, or clone write
    /// interception). Caller pushes this response and continues the loop.
    Handled(Response),
    /// No interception occurred (or a mutation was applied in place);
    /// caller proceeds to normal dispatch with the (possibly mutated) task
    /// and the trigger bookkeeping needed for the AFTER-trigger phase.
    /// Boxed: `PhysicalTask` makes this variant far larger than `Handled`,
    /// which would otherwise bloat every `PreDispatchOutcome` on the stack.
    Proceed(Box<PreDispatchProceed>),
}

/// Payload for [`PreDispatchOutcome::Proceed`], boxed to keep the enum small.
pub(super) struct PreDispatchProceed {
    pub(super) task: PhysicalTask,
    pub(super) dml_info: Option<DmlWriteInfo>,
    pub(super) old_row: Option<HashMap<String, nodedb_types::Value>>,
    pub(super) truncate_restart_collection: Option<String>,
}

impl NodeDbPgHandler {
    /// Run trigger interception and clone write-path interception for a
    /// single write task, before it reaches normal dispatch.
    pub(super) async fn run_pre_dispatch_hooks(
        &self,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        plan_kind: PlanKind,
        mut task: PhysicalTask,
    ) -> PgWireResult<PreDispatchOutcome> {
        // --- Trigger interception for DML writes ---
        let mut dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

        // Fetch OLD row and fire BEFORE/INSTEAD OF triggers if applicable.
        let old_row = if let Some(ref info) = dml_info
            && info.document_id.is_some()
            && (matches!(
                info.event,
                crate::control::trigger::DmlEvent::Update
                    | crate::control::trigger::DmlEvent::Delete
            ) || info.needs_existence_probe)
        {
            let doc_id = info.document_id.as_deref().unwrap_or("");
            let row = crate::control::trigger::dml_hook::fetch_old_row(
                &self.state,
                tenant_id,
                &info.collection,
                doc_id,
            )
            .await;
            if !row.is_empty() { Some(row) } else { None }
        } else {
            None
        };

        // Probe-driven reclassification.
        if let Some(ref mut info) = dml_info
            && info.needs_existence_probe
        {
            info.event = if old_row.is_some() {
                crate::control::trigger::DmlEvent::Update
            } else {
                crate::control::trigger::DmlEvent::Insert
            };
        }

        if let Some(ref info) = dml_info {
            use crate::control::trigger::dml_hook_fire::PreDispatchResult;
            match crate::control::trigger::dml_hook_fire::fire_pre_dispatch_triggers(
                &self.state,
                identity,
                tenant_id,
                info,
                &old_row,
                0,
            )
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })? {
                PreDispatchResult::Handled => {
                    return Ok(PreDispatchOutcome::Handled(Response::Execution(Tag::new(
                        "OK",
                    ))));
                }
                PreDispatchResult::Proceed {
                    mutated_fields: Some(fields),
                } => {
                    crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                        &mut task, &fields,
                    );
                }
                PreDispatchResult::Proceed {
                    mutated_fields: None,
                } => {}
            }
        }

        // Extract truncate restart_identity info before task is moved.
        let truncate_restart_collection =
            if let nodedb_physical::physical_plan::PhysicalPlan::Document(
                nodedb_physical::physical_plan::DocumentOp::Truncate {
                    collection,
                    restart_identity: true,
                },
            ) = &task.plan
            {
                Some(collection.clone())
            } else {
                None
            };

        // --- Clone write-path interception ---
        // For PointUpdate / PointDelete on Shadowed/Materializing clones,
        // apply copy-up or tombstone before (or instead of) normal dispatch.
        // Non-cloned collections and Materialized clones short-circuit here.
        {
            use super::clone_write_dispatch::CloneWriteOutcome;
            match self.maybe_intercept_clone_write(&task, tenant_id).await? {
                CloneWriteOutcome::Handled(resp) => {
                    let shaped = crate::control::server::pgwire::handler::plan::payload_to_response(
                        resp.payload.as_ref(),
                        plan_kind,
                    );
                    if let Some(notice) = shaped.notice {
                        self.sessions.push_notice(addr, notice);
                    }
                    return Ok(PreDispatchOutcome::Handled(shaped.response));
                }
                CloneWriteOutcome::Passthrough => {}
            }
        }

        Ok(PreDispatchOutcome::Proceed(Box::new(PreDispatchProceed {
            task,
            dml_info,
            old_row,
            truncate_restart_collection,
        })))
    }
}
