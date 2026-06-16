// SPDX-License-Identifier: BUSL-1.1

//! Plan-and-dispatch entry points for SQL queries on the simple-query and
//! extended-query (prepared-statement) paths.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::calvin::{DispatchClass, classify_dispatch};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::TenantId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::types::{error_to_sqlstate, response_status_to_sqlstate};
use super::super::core::NodeDbPgHandler;
use super::super::plan::{describe_plan, extract_collection, payload_to_response};
use super::kv_wrapping::maybe_wrap_kv_point_get;
use super::planning::consistency_for_tasks;
use super::set_ops;

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block (BEGIN..COMMIT), write operations are
    /// buffered instead of dispatched. Read operations execute immediately.
    /// The buffer is dispatched atomically on COMMIT.
    ///
    /// This is the simple-query entry point (no bound parameters). After
    /// dispatching, the SELECT projection list is parsed from `sql` and
    /// each query response is re-encoded with one pgwire field per projected
    /// column. The extended-query path (`execute_planned_sql_with_params`)
    /// skips this step because `execute_prepared` applies column projection
    /// using the richer schema from the Describe phase.
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let responses = self
            .execute_planned_sql_inner(identity, sql, tenant_id, addr, &[])
            .await?;

        // Column projection: re-encode each query response with one pgwire
        // field per named column from the SELECT list.
        use super::super::projection::{
            ProjectionItem, fields_for_projection, lookup_keys_for_projection, needs_projection,
            parse_select_projection, reproject_star_response,
        };
        let items_opt = parse_select_projection(sql);

        // SELECT * — expand each row's JSON object into individual columns.
        if matches!(items_opt.as_deref(), Some([ProjectionItem::Star])) {
            let mut projected = Vec::with_capacity(responses.len());
            for resp in responses {
                projected.push(reproject_star_response(resp).await.map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("star projection failed: {e}"),
                    )))
                })?);
            }
            return Ok(projected);
        }

        // Named columns — re-encode with the declared column list.
        if let Some(items) = items_opt.filter(|items| needs_projection(items)) {
            let fields = fields_for_projection(&items);
            let keys = lookup_keys_for_projection(&items);
            let mut projected = Vec::with_capacity(responses.len());
            for resp in responses {
                projected.push(
                    super::super::projection::reproject_response(resp, &fields, &keys).map_err(
                        |e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "XX000".to_owned(),
                                format!("column projection failed: {e}"),
                            )))
                        },
                    )?,
                );
            }
            return Ok(projected);
        }

        Ok(responses)
    }

    /// Execute planned SQL with bound parameters (prepared statement path).
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql_with_params(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, addr, params)
            .await
    }

    async fn execute_planned_sql_inner(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
        params: &[nodedb_sql::ParamValue],
    ) -> PgWireResult<Vec<Response>> {
        let (tasks, _plan_lease_scope) = self
            .plan_statement_to_tasks(identity, sql, tenant_id, addr, params)
            .await?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // Clone CoW read-path interception: for Shadowed/Materializing clones,
        // augment tasks with source-database reads and merge results.
        // Returns Some(responses) when clone dispatch is fully handled.
        // Returns None when this is not a cloned collection (fast path).
        if let Some(clone_responses) = self
            .maybe_dispatch_clone_reads(tasks.clone(), tenant_id, addr)
            .await?
        {
            return Ok(clone_responses);
        }

        let consistency = consistency_for_tasks(&tasks);

        // When all tasks target a remote leader, route through the gateway.
        if self.should_forward_via_gateway(&tasks, consistency) {
            let database_id = self
                .sessions
                .get_current_database(addr)
                .unwrap_or(crate::types::DatabaseId::DEFAULT);
            return self
                .dispatch_tasks_via_gateway(tasks, tenant_id, database_id)
                .await;
        }

        let tx_state = self.sessions.transaction_state(addr);
        match classify_dispatch(&tasks) {
            DispatchClass::SingleShard { .. } => {}
            DispatchClass::MultiShard { .. } => {
                if tx_state == crate::control::server::pgwire::session::TransactionState::InBlock {
                    let (severity, code, message) =
                        error_to_sqlstate(&crate::Error::CrossShardInExplicitTransaction);
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    ))));
                }

                let cross_shard_mode = self.sessions.cross_shard_txn_mode(addr);
                if cross_shard_mode
                    == crate::control::server::pgwire::session::cross_shard_mode::CrossShardTxnMode::Strict
                {
                    return self
                        .dispatch_calvin_multishard(tasks, tenant_id, identity, addr)
                        .await;
                }
            }
        }

        self.dispatch_task_loop(tasks, tenant_id, identity, addr)
            .await
    }

    /// Execute the per-task dispatch loop for non-Calvin queries.
    async fn dispatch_task_loop(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
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

            self.check_permission(identity, &task.plan)?;

            // ClusterArray plans are handled entirely on the Control Plane by the
            // ArrayCoordinator — they must never reach the SPSC bridge or
            // trigger/DML machinery. Intercept them here and short-circuit.
            if let nodedb_physical::physical_plan::PhysicalPlan::ClusterArray(ref cluster_op) =
                task.plan
            {
                use crate::control::cluster::ClusterArrayExecutor;
                use crate::control::server::pgwire::handler::plan::PlanKind;
                use std::sync::Arc;

                let transport = self.state.cluster_transport.as_ref().ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        "cluster transport not available for ClusterArray dispatch".to_owned(),
                    )))
                })?;
                let routing = self.state.cluster_routing.as_ref().ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        "cluster routing not available for ClusterArray dispatch".to_owned(),
                    )))
                })?;
                let executor = ClusterArrayExecutor::new(
                    Arc::clone(transport),
                    Arc::clone(routing),
                    self.state.node_id,
                    Arc::clone(&self.state),
                );
                let payload_bytes = executor.execute(cluster_op).await.map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;
                let cluster_plan_kind = match cluster_op {
                    nodedb_physical::physical_plan::ClusterArrayOp::Slice { .. } => {
                        PlanKind::ArraySlice
                    }
                    _ => PlanKind::MultiRow,
                };
                let shaped = payload_to_response(&payload_bytes, cluster_plan_kind);
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                responses.push(shaped.response);
                continue;
            }

            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
            {
                let is_write = crate::control::wal_replication::to_replicated_entry(
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                )
                .is_some();
                if is_write {
                    self.sessions.buffer_write(addr, task);
                    responses.push(Response::Execution(Tag::new("OK")));
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let collection_for_si = extract_collection(&task.plan).map(String::from);
            let resp_post_set_op = task.post_set_op;
            let plan_for_response = task.plan.clone();

            // --- Single-node pgwire streaming fast path ---
            //
            // Permission was already checked at the top of the loop. An eligible
            // autocommit unordered SELECT streams its rows straight to the
            // client (see `maybe_stream_select`); everything else falls through
            // to the normal dispatch path below.
            if let Some(stream_response) = self
                .maybe_stream_select(&task, plan_kind, resp_post_set_op, addr)
                .await?
            {
                responses.push(stream_response);
                continue;
            }

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
                        responses.push(Response::Execution(Tag::new("OK")));
                        continue;
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
                        let shaped =
                            crate::control::server::pgwire::handler::plan::payload_to_response(
                                resp.payload.as_ref(),
                                plan_kind,
                            );
                        if let Some(notice) = shaped.notice {
                            self.sessions.push_notice(addr, notice);
                        }
                        responses.push(shaped.response);
                        continue;
                    }
                    CloneWriteOutcome::Passthrough => {}
                }
            }

            // --- Normal dispatch ---
            let user_id: Option<std::sync::Arc<str>> =
                Some(std::sync::Arc::from(identity.username.as_str()));
            let resp = self
                .dispatch_task(task, user_id, Some(identity))
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
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
                })?;

                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u64(), &info.collection);
            }

            // Track reads for snapshot isolation conflict detection.
            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
                && let Some(collection) = collection_for_si
            {
                self.sessions
                    .record_read(addr, collection, String::new(), resp.watermark_lsn);
            }

            if needs_set_op && resp_post_set_op != PostSetOp::None {
                dedup_payloads.push(resp.payload.to_vec());
                if dedup_set_op == PostSetOp::None {
                    dedup_set_op = resp_post_set_op;
                }
            } else {
                let payload = maybe_wrap_kv_point_get(&plan_for_response, &resp.payload);
                let payload = crate::control::server::response_translate::translate_if_vector(
                    &payload,
                    &plan_for_response,
                    &self.state,
                );
                let shaped = payload_to_response(&payload, plan_kind);
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                responses.push(shaped.response);
            }
        }

        // Set operations: merge sub-query payloads.
        if needs_set_op && !dedup_payloads.is_empty() {
            responses.push(set_ops::apply_set_ops(&dedup_payloads, dedup_set_op));
        }

        Ok(responses)
    }
}
