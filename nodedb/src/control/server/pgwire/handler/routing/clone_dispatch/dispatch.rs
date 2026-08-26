// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW read-path interception for the pgwire handler.
//!
//! Runs after planning, before dispatch. For `Shadowed`/`Materializing` clones,
//! builds an augmented task list and merges the source response with tombstone
//! filtering. Non-cloned / fully `Materialized` databases return `None`.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::clone::resolver::{
    CloneReadParams, ResolveOutcome, filter_tombstoned_rows, resolve_read,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::pgwire::handler::plan::{PlanKind, multirow_payload_to_response};
use crate::control::server::pgwire::handler::routing::result_shaping::ResultShaping;
use crate::control::server::pgwire::handler::shape_encode;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::response_shape::kv::apply_kv_wrap;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::super::types::{
    error_to_sqlstate, response_status_to_sqlstate, sqlstate_error,
};
use super::super::super::core::NodeDbPgHandler;
use super::merge::{filter_kv_tombstoned_rows, merge_msgpack_arrays, wrap_single_map_as_array};
use super::temporal::extract_system_as_of_ms;

/// Meter one clone-read sub-task after its dispatch returns success.
///
/// Clone CoW reads bypass `dispatch_task_loop`, so neither half is metered
/// elsewhere; `rows: None` charges one unit rather than an early row count.
fn meter_clone_task(
    state: &crate::control::state::SharedState,
    identity: &AuthenticatedIdentity,
    task: &PhysicalTask,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(&task.plan);
    let scope = RequestAuthScope::builder(identity, state.auth_stores())
        .with_session_database(Some(task.database_id))
        .build();
    meter_dispatch(state, &scope, &info, None);
}

/// Raise a Data-Plane error status as a pgwire error.
///
/// A clone read dispatches both halves itself, bypassing `dispatch_task_loop`'s
/// status check — without this, an errored half looks like empty success.
fn raise_clone_task_error(resp: &crate::bridge::envelope::Response) -> PgWireResult<()> {
    match response_status_to_sqlstate(resp.status, resp.error_code.as_deref()) {
        None => Ok(()),
        Some((severity, code, message)) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            severity.to_owned(),
            code.to_owned(),
            message,
        )))),
    }
}

/// The source surrogate a rewritten document point-read fetches.
/// A point-get answers with the row body alone, no surrogate — deciding
/// suppression from the plan keeps it consistent with scans.
fn point_read_surrogate(plan: &PhysicalPlan) -> Option<u32> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { surrogate, .. }) => Some(surrogate.as_u32()),
        _ => None,
    }
}

impl NodeDbPgHandler {
    /// Intercept read tasks for cloned collections.
    ///
    /// `Some(responses)` when clone resolution handled dispatch completely —
    /// return that directly. `None` when the task doesn't target a clone.
    pub(in crate::control::server::pgwire::handler::routing) async fn maybe_dispatch_clone_reads(
        &self,
        tasks: Vec<PhysicalTask>,
        identity: &crate::control::security::identity::AuthenticatedIdentity,
        tenant_id: TenantId,
        session_id: SessionId,
        shaping: ResultShaping<'_>,
        auth: &crate::control::security::auth_context::AuthContext,
    ) -> PgWireResult<Option<Vec<Response>>> {
        let ResultShaping {
            projection,
            formats: result_formats,
        } = shaping;
        // Resolved before `resolve_read` consumes `tasks` — a clone read merges
        // source rows into the target's, so both branches' sources govern redaction.
        let redaction = QueryRedaction::for_plans(tenant_id, auth, tasks.iter().map(|t| &t.plan));
        // If the first task carries `system_as_of_ms`, derive query_lsn from that
        // wall-clock time; otherwise fall back to the current WAL LSN.
        let (query_lsn, query_ms) =
            if let Some(as_of_ms) = extract_system_as_of_ms(tasks.first().map(|t| &t.plan)) {
                let lsn = self.state.ms_to_lsn(as_of_ms);
                (lsn, Some(as_of_ms))
            } else {
                let lsn = self.state.wal.next_lsn();
                let ms = self.state.ms_to_lsn_inverse(lsn);
                (lsn, ms)
            };

        let params = CloneReadParams {
            query_lsn,
            query_ms,
        };

        let outcome = resolve_read(&self.state, tasks, tenant_id, &params).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        match outcome {
            None => Ok(None),

            Some(ResolveOutcome::Passthrough(_tasks)) => {
                // Fully materialized — let the normal dispatch path handle it.
                Ok(None)
            }

            Some(ResolveOutcome::PreDatesClone(note)) => {
                // Query time predates the clone's creation — return empty.
                tracing::debug!(
                    message = note.message,
                    query_lsn = %note.query_lsn,
                    clone_created_at = %note.clone_created_at,
                    "clone read predates clone creation — returning empty result"
                );
                let empty: Vec<u8> =
                    nodedb_types::json_to_msgpack(&serde_json::json!([])).unwrap_or_default();
                match compose::shape_payload_no_plan(
                    &empty,
                    PlanKind::MultiRow,
                    projection,
                    Some(redaction.ctx(&self.state.redaction)),
                )
                .map_err(|e| sqlstate_error("XX000", e.message()))?
                {
                    ShapeOutcome::Rows(shaped) => {
                        let (response, notice) =
                            shape_encode::shaped_query_response(shaped, result_formats);
                        if let Some(n) = notice {
                            self.sessions.push_notice(session_id, n);
                        }
                        Ok(Some(vec![response]))
                    }
                    ShapeOutcome::Passthrough => {
                        let shaped = multirow_payload_to_response(&empty);
                        if let Some(notice) = shaped.notice {
                            self.sessions.push_notice(session_id, notice);
                        }
                        Ok(Some(vec![shaped.response]))
                    }
                }
            }

            Some(ResolveOutcome::Augmented {
                tasks,
                source_start_idx,
                origin: _,
                target_collection_key,
                note,
            }) => {
                if let Some(note) = note {
                    tracing::debug!(
                        message = note.message,
                        "clone read: T_lsn < clone_created_at (note attached)"
                    );
                }

                // Re-authorize the augmented set (target + source) before dispatch.
                let _authorized_tasks = self.authorize_tasks(identity, &tasks)?;

                // Split tasks into target and source halves.
                let (target_tasks, source_tasks) = tasks.split_at(source_start_idx);

                // Dispatch target tasks (these are the primary tasks).
                let mut responses = Vec::with_capacity(target_tasks.len());
                for task in target_tasks {
                    let resp = self
                        .dispatch_authorized_task(task.clone(), None, identity)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;
                    raise_clone_task_error(&resp)?;
                    meter_clone_task(&self.state, identity, task);
                    responses.push(resp);
                }

                // Suppressed source surrogates: tombstones plus copy-ups (a copy-up
                // leaves the superseded source row in place, so merging it back would double it).
                let mut tombstoned = self
                    .state
                    .credentials
                    .catalog()
                    .list_clone_tombstones(&target_collection_key)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;
                let copied_up = self
                    .state
                    .credentials
                    .catalog()
                    .list_clone_copyups(&target_collection_key)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;
                tombstoned.extend(copied_up);

                // Load KV tombstones for KV-engine key-based filtering.
                let kv_tombstoned = self
                    .state
                    .credentials
                    .catalog()
                    .list_kv_clone_tombstones(&target_collection_key)
                    .map_err(|e| {
                        let (severity, code, message) = error_to_sqlstate(&e);
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            severity.to_owned(),
                            code.to_owned(),
                            message,
                        )))
                    })?;

                // A multi-level clone chain emits one source task per ancestor per target task;
                // tasks sharing `source_idx % target_tasks.len()` share a response slot.
                let target_count = target_tasks.len().max(1);
                for (source_idx, source_task) in source_tasks.iter().enumerate() {
                    if point_read_surrogate(&source_task.plan)
                        .is_some_and(|s| tombstoned.contains(&s))
                    {
                        continue;
                    }
                    let response_idx = source_idx % target_count;
                    let source_resp = self
                        .dispatch_authorized_task(source_task.clone(), None, identity)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;
                    raise_clone_task_error(&source_resp)?;
                    meter_clone_task(&self.state, identity, source_task);

                    // KvOp::Get: inject the primary key field for projection/column checks.
                    let normalized_payload =
                        apply_kv_wrap(&source_task.plan, source_resp.payload.as_ref());

                    // KvOp::Get responses arrive as a single map; normalize to a 1-element
                    // array so tombstone filters and merge work uniformly.
                    let normalized_payload = wrap_single_map_as_array(normalized_payload);

                    // Post-normalization the input is always a valid array, so `None`
                    // signals upstream corruption — log and pass through unchanged.
                    let source_payload = match filter_tombstoned_rows(
                        &normalized_payload,
                        &tombstoned,
                    ) {
                        Some(p) => p,
                        None => {
                            tracing::warn!(
                                payload_len = normalized_payload.len(),
                                "clone read: filter_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                            );
                            normalized_payload
                        }
                    };

                    // Apply KV key tombstone filter (KV engine rows).
                    let source_payload = if !kv_tombstoned.is_empty() {
                        match filter_kv_tombstoned_rows(&source_payload, &kv_tombstoned) {
                            Some(p) => p,
                            None => {
                                tracing::warn!(
                                    payload_len = source_payload.len(),
                                    "clone read: filter_kv_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                                );
                                source_payload
                            }
                        }
                    } else {
                        source_payload
                    };

                    // `response_idx` maps ancestor tasks back to their original query-task slot.
                    if response_idx < responses.len() {
                        // Normalize target payload to array shape for uniform merge.
                        let target_payload = wrap_single_map_as_array(
                            responses[response_idx].payload.as_ref().to_vec(),
                        );
                        let merged = merge_msgpack_arrays(&target_payload, &source_payload)
                            .map_err(|e| {
                                let (severity, code, message) = error_to_sqlstate(&e);
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    severity.to_owned(),
                                    code.to_owned(),
                                    message,
                                )))
                            })?;
                        responses[response_idx] = crate::bridge::envelope::Response {
                            payload: merged.into(),
                            ..responses[response_idx].clone()
                        };
                    } else {
                        // More source tasks than target tasks — append standalone.
                        responses.push(crate::bridge::envelope::Response {
                            payload: source_payload.into(),
                            ..source_resp
                        });
                    }
                }

                // Convert raw Response objects to pgwire Responses.
                let mut pg_responses = Vec::with_capacity(responses.len());
                for resp in responses {
                    match compose::shape_payload_no_plan(
                        resp.payload.as_ref(),
                        PlanKind::MultiRow,
                        projection,
                        Some(redaction.ctx(&self.state.redaction)),
                    )
                    .map_err(|e| sqlstate_error("XX000", e.message()))?
                    {
                        ShapeOutcome::Rows(shaped) => {
                            let (response, notice) =
                                shape_encode::shaped_query_response(shaped, result_formats);
                            if let Some(n) = notice {
                                self.sessions.push_notice(session_id, n);
                            }
                            pg_responses.push(response);
                        }
                        ShapeOutcome::Passthrough => {
                            let shaped = multirow_payload_to_response(resp.payload.as_ref());
                            if let Some(notice) = shaped.notice {
                                self.sessions.push_notice(session_id, notice);
                            }
                            pg_responses.push(shaped.response);
                        }
                    }
                }

                Ok(Some(pg_responses))
            }
        }
    }
}
