// SPDX-License-Identifier: BUSL-1.1

//! Plan-and-dispatch entry points for SQL queries on the simple-query and
//! extended-query (prepared-statement) paths.

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::compose::{self, ShapeOutcome};
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::types::{error_to_sqlstate, response_status_to_sqlstate, sqlstate_error};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{describe_plan, payload_to_response};
use super::super::shape_encode;
use super::result_shaping::ResultShaping;
use super::set_ops;
use super::streaming::StreamSelectContext;

struct DispatchTaskContext<'a> {
    plan_lease_scope: Arc<crate::control::lease::QueryLeaseScope>,
    tenant_id: TenantId,
    identity: &'a AuthenticatedIdentity,
    auth_ctx: &'a crate::control::security::auth_context::AuthContext,
    session_id: SessionId,
    shaping: ResultShaping<'a>,
}

impl NodeDbPgHandler {
    pub(super) async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        params: &[nodedb_sql::ParamValue],
        shaping: ResultShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let (mut tasks, output_schema, versions, auth_ctx) = self
            .plan_statement_to_tasks(identity, sql, tenant_id, session_id, params)
            .await?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // An externally-supplied prepared-statement schema (from the Describe
        // phase) wins; otherwise use the planner's fresh output schema for this
        // statement.
        let effective_schema = shaping.projection.or(Some(&output_schema));

        // Extraction marks catalog state and allocates surrogates. Authorize
        // the original planned tasks before it can perform either side effect.
        let _preauthorized_tasks = self.authorize_tasks(identity, &tasks)?;

        // Implicit graph-edge extraction: a schemaless document carrying
        // `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task, homed and
        // surrogate-resolved per endpoint so it routes through the same
        // classify/Calvin/single-shard path as an explicit edge.
        let edge_database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        crate::control::planner::implicit_edges::append_implicit_edge_tasks(
            &self.state,
            &mut tasks,
            tenant_id,
            edge_database_id,
            crate::types::TraceId::ZERO,
        )
        .await
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        // The final task set must be authorized before any clone interception,
        // orchestration, staging, or dispatch path can observe it. Descriptor
        // admission follows this check so an implicit-edge denial consumes no
        // descriptor lease.
        let _authorized_tasks = self.authorize_tasks(identity, &tasks)?;
        let plan_lease_scope = Arc::new(self.state.acquire_plan_lease_scope(&versions).map_err(
            |e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            },
        )?);

        // Clone CoW read-path interception: for Shadowed/Materializing clones,
        // augment tasks with source-database reads and merge results.
        // Returns Some(responses) when clone dispatch is fully handled.
        // Returns None when this is not a cloned collection (fast path).
        if let Some(clone_responses) = self
            .maybe_dispatch_clone_reads(
                tasks.clone(),
                identity,
                tenant_id,
                session_id,
                effective_schema,
                shaping.formats,
            )
            .await?
        {
            return Ok(clone_responses);
        }

        // Implicit-edge dependent predicates must be preempted onto the
        // OLLP/Calvin path before gateway forwarding or ordinary dispatch.
        if let Some(responses) = self
            .maybe_dispatch_implicit_edge_recon(
                &tasks,
                tenant_id,
                identity,
                session_id,
                shaping.formats,
            )
            .await?
        {
            return Ok(responses);
        }

        if let Some(responses) = self
            .maybe_dispatch_tasks_via_gateway(
                &tasks,
                identity,
                tenant_id,
                session_id,
                effective_schema,
                shaping.formats,
            )
            .await?
        {
            return Ok(responses);
        }

        let tx_state = self.sessions.transaction_state(session_id);
        // Autocommit statement routing: no session read-set to widen with.
        match classify_dispatch(&tasks, &std::collections::BTreeSet::new()) {
            DispatchClass::SingleShard { .. } => {
                // A single-shard dependent-predicate write (e.g. `DELETE ...
                // WHERE <non-pk>`) doesn't need OLLP/Calvin: one shard is one
                // Raft group, so the normal replicated-write dispatch path
                // applies it deterministically. Edge-bearing dependent
                // predicates are already preempted onto Calvin above; only
                // genuine multi-shard bulk writes need OLLP. Fall through.
            }
            DispatchClass::MultiShard { .. } => {
                if tx_state == crate::control::server::shared::session::TransactionState::InBlock {
                    let (severity, code, message) =
                        error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    ))));
                }

                let cross_shard_mode = self.sessions.cross_shard_txn_mode(session_id);
                if cross_shard_mode
                    == crate::control::server::shared::session::cross_shard_mode::CrossShardTxnMode::Strict
                {
                    return self
                        .dispatch_calvin_multishard(
                            tasks,
                            tenant_id,
                            identity,
                            session_id,
                            shaping.formats,
                        )
                        .await;
                }
            }
        }

        self.dispatch_task_loop(
            tasks,
            DispatchTaskContext {
                plan_lease_scope: Arc::clone(&plan_lease_scope),
                tenant_id,
                identity,
                auth_ctx: &auth_ctx,
                session_id,
                shaping: ResultShaping {
                    projection: effective_schema,
                    formats: shaping.formats,
                },
            },
        )
        .await
    }

    /// Execute the per-task dispatch loop for non-Calvin queries.
    async fn dispatch_task_loop(
        &self,
        tasks: Vec<PhysicalTask>,
        context: DispatchTaskContext<'_>,
    ) -> PgWireResult<Vec<Response>> {
        let DispatchTaskContext {
            plan_lease_scope,
            tenant_id,
            identity,
            auth_ctx,
            session_id,
            shaping,
        } = context;
        let projection = shaping.projection;
        let result_formats = shaping.formats;
        let needs_set_op = tasks.iter().any(|t| t.post_set_op != PostSetOp::None);
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_set_op = PostSetOp::None;
        let mut responses = Vec::with_capacity(tasks.len());

        for mut task in tasks {
            if task.tenant_id != tenant_id {
                tracing::error!(
                    expected = %tenant_id,
                    actual = %task.tenant_id,
                    "SECURITY: task tenant_id mismatch — rejecting"
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "tenant isolation violation: task targets wrong tenant".to_owned(),
                ))));
            }

            // ClusterArray plans are handled entirely on the Control Plane by the
            // ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercept them here and short-circuit.
            if matches!(
                task.plan,
                nodedb_physical::physical_plan::PhysicalPlan::ClusterArray(_)
            ) {
                let authorized = self
                    .authorize_tasks(identity, std::slice::from_ref(&task))?
                    .into_tasks()
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            "ClusterArray authorization returned no capability".to_owned(),
                        )))
                    })?;
                let response = self
                    .dispatch_cluster_array_task(authorized, projection, result_formats, session_id)
                    .await?;
                responses.push(response);
                continue;
            }

            // In-transaction write-routing gate: protocol-neutral decision of
            // read / buffer-for-COMMIT / stage-now-and-buffer, shared with
            // every other dispatch loop (native, DSL/UPSERT). Moved to
            // `execute_dml_hooks.rs` to keep this file under the size limit;
            // behavior is unchanged.
            match self
                .route_task_in_txn(session_id, identity, task, Arc::clone(&plan_lease_scope))
                .await?
            {
                super::execute_dml_hooks::TxnRouteOutcome::Proceed(routed_task) => {
                    task = *routed_task;
                }
                super::execute_dml_hooks::TxnRouteOutcome::Handled(resp) => {
                    responses.push(resp);
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let resp_post_set_op = task.post_set_op;
            let task_database_id = task.database_id;
            let task_vshard = task.vshard_id;
            let plan_for_response = task.plan.clone();

            // Single-node pgwire streaming fast path (autocommit SELECT only).
            // In-transaction reads skip streaming so the transaction id rides on
            // the request and the data plane merges the transaction's own staged
            // writes into the scan (read-your-own-writes); the streaming path
            // builds per-core requests without the transaction id.
            let in_transaction = self.sessions.transaction_state(session_id)
                == crate::control::server::shared::session::TransactionState::InBlock;
            if !in_transaction
                && let Some(stream_response) = self
                    .maybe_stream_select(
                        &task,
                        StreamSelectContext {
                            identity,
                            plan_kind,
                            session_id,
                            shaping: ResultShaping {
                                projection,
                                formats: result_formats,
                            },
                            lease_scope: Arc::clone(&plan_lease_scope),
                        },
                    )
                    .await?
            {
                responses.push(stream_response);
                continue;
            }

            // --- Pre-dispatch hooks: trigger interception + clone write-path
            // interception (moved to execute_dml_hooks.rs to keep this file
            // under the size limit; behavior is unchanged).
            let (dml_info, old_row, truncate_restart_collection) = match self
                .run_pre_dispatch_hooks(identity, auth_ctx, tenant_id, session_id, plan_kind, task)
                .await?
            {
                super::execute_dml_hooks::PreDispatchOutcome::Handled(resp) => {
                    responses.push(resp);
                    continue;
                }
                super::execute_dml_hooks::PreDispatchOutcome::Proceed(proceed) => {
                    let super::execute_dml_hooks::PreDispatchProceed {
                        task: proceeding_task,
                        dml_info,
                        old_row,
                        truncate_restart_collection,
                    } = *proceed;
                    task = proceeding_task;
                    (dml_info, old_row, truncate_restart_collection)
                }
            };

            // --- Normal dispatch ---
            let user_id: Option<std::sync::Arc<str>> =
                Some(std::sync::Arc::from(identity.username.as_str()));
            let (resp, shard_watermarks, distributed_reads) = self
                .dispatch_authorized_task_with_watermarks(task, user_id, identity)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            // Track reads for snapshot-isolation / cross-shard conflict detection
            // at the protocol-neutral layer. Recorded BEFORE the error
            // short-circuit so an absent-key point read (a `NotFound` from the
            // Data Plane) is still captured — a "not found" is a validatable
            // phantom observation, not a no-op. Only successful reads and
            // not-found reads record; a genuine dispatch failure does not.
            let records_read = resp.status == crate::bridge::envelope::Status::Ok
                || resp.error_code.as_deref()
                    == Some(&crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && self.sessions.transaction_state(session_id)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                let watermarks = if shard_watermarks.is_empty() {
                    vec![(task_vshard, resp.watermark_lsn)]
                } else {
                    shard_watermarks
                };
                crate::control::server::shared::session::record_reads_for_response(
                    &self.state,
                    &self.sessions,
                    session_id,
                    identity.tenant_id,
                    crate::control::server::shared::session::ResponseReads {
                        plan: &plan_for_response,
                        watermarks: &watermarks,
                        read_version_lsn: resp.read_version_lsn,
                        found: resp.status == crate::bridge::envelope::Status::Ok,
                        distributed_reads: &distributed_reads,
                        read_lsn_vshard: task_vshard,
                    },
                )
                .await;
            }

            // Record the session's OWN committed write-version so a later
            // transaction's read-set capture can be floored at it
            // (read-your-writes floor for cross-shard OCC). A prior autocommit
            // write must still floor a later transaction's read, so this records
            // regardless of transaction state — the version is the write's
            // committed per-collection `coll_write_lsn`, carried on
            // `read_version_lsn` by the replicated-write dispatch path. Only
            // successful writes with a non-zero version are recorded.
            if resp.status == crate::bridge::envelope::Status::Ok
                && resp.read_version_lsn > crate::types::Lsn::ZERO
                && matches!(
                    crate::control::security::identity::required_permission(&plan_for_response),
                    crate::control::security::identity::Permission::Write
                )
                && let Some(collection) =
                    crate::control::server::shared::plan_util::extract_collection(
                        &plan_for_response,
                    )
            {
                self.sessions.note_own_write(
                    session_id,
                    task_database_id,
                    identity.tenant_id,
                    collection,
                    resp.read_version_lsn,
                );
            }

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, resp.error_code.as_deref())
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // --- TRUNCATE RESTART IDENTITY ---
            if let Some(collection) = &truncate_restart_collection {
                self.state
                    .sequence_registry
                    .restart_sequences_for_collection(tenant_id.as_u64(), collection);
            }

            // --- AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook_fire::fire_post_dispatch_triggers(
                    crate::control::trigger::dml_hook_fire::DispatchTriggerParams {
                        state: &self.state,
                        identity,
                        database_id: task_database_id,
                        tenant_id,
                        info,
                        old_row: &old_row,
                        cascade_depth: 0,
                    },
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u64(), &info.collection);
            }

            if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                match compose::shape_response_materialized(
                    &resp.payload,
                    &plan_for_response,
                    plan_kind,
                    projection,
                    &self.state,
                    task_database_id,
                    tenant_id,
                )
                .map_err(|e| sqlstate_error("XX000", e.message()))?
                {
                    ShapeOutcome::Rows(shaped) => {
                        let (response, notice) =
                            shape_encode::shaped_query_response(shaped, result_formats);
                        if let Some(n) = notice {
                            self.sessions.push_notice(session_id, n);
                        }
                        responses.push(response);
                    }
                    ShapeOutcome::Passthrough => {
                        let shaped = payload_to_response(&resp.payload, plan_kind)?;
                        if let Some(notice) = shaped.notice {
                            self.sessions.push_notice(session_id, notice);
                        }
                        responses.push(shaped.response);
                    }
                }
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            let (response, notice) =
                set_ops::apply_set_ops(&dedup_payloads, dedup_set_op, projection, result_formats);
            if let Some(n) = notice {
                self.sessions.push_notice(session_id, n);
            }
            responses.push(response);
        }

        Ok(responses)
    }
}
