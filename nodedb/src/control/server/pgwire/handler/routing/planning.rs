// SPDX-License-Identifier: BUSL-1.1

//! SQL planning: converts SQL text into physical task lists.

use std::sync::Arc;

use pgwire::api::results::Tag;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::catalog::current_descriptor_version;
use super::setup_error::StatementSetupError;

impl NodeDbPgHandler {
    /// Plan a SQL statement to physical tasks, handling session auth, RETURNING
    /// strip, CHECK constraints, plan cache, and RETURNING injection.
    ///
    /// This is the single planning code path shared by both the simple-query
    /// (`execute_planned_sql_inner`) and any future callers that need typed
    /// physical plans without driving the dispatch loop. Returns the ready-to-
    /// dispatch task list and the descriptor versions to admit after execution
    /// has expanded and authorized every implicit task.
    ///
    /// Errors stay typed ([`StatementSetupError`]) rather than pre-rendered:
    /// the caller wraps this call and the later lease acquisition in ONE retry
    /// unit, which needs to tell a descriptor-drain race apart from a terminal
    /// failure. Retrying is the caller's job — this function makes exactly one
    /// planning attempt so the budget is never nested.
    pub(in crate::control::server::pgwire::handler) async fn plan_statement_to_tasks(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        params: &[nodedb_sql::ParamValue],
    ) -> Result<
        (
            Vec<PhysicalTask>,
            crate::control::server::response_shape::schema::OutputSchema,
            crate::control::planner::descriptor_set::DescriptorVersionSet,
            crate::control::security::auth_context::AuthContext,
        ),
        StatementSetupError,
    > {
        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        // Network provenance is immutable accept-time metadata; all mutable
        // session state remains keyed by the collision-free SessionId.
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    StatementSetupError::protocol(
                        "FATAL",
                        "XX000",
                        "connection session metadata is unavailable",
                    )
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let caller_fp = crate::control::security::session_handle::ClientFingerprint::from_peer(
            identity.tenant_id,
            &peer_addr,
        );
        let conn_key = format!("{session_id:?}");
        let mut auth_ctx = if let Some(handle) = self
            .sessions
            .get_parameter(session_id, "nodedb.auth_session")
        {
            use crate::control::security::session_handle::ResolveOutcome;
            match self
                .state
                .session_handles
                .resolve(&handle, &conn_key, &caller_fp)
            {
                ResolveOutcome::Resolved(cached) => *cached,
                ResolveOutcome::RateLimited => {
                    return Err(StatementSetupError::protocol(
                        "FATAL",
                        "53300",
                        "session handle resolve rate limit exceeded on this \
                         connection — closing",
                    ));
                }
                ResolveOutcome::Miss => {
                    crate::control::server::session_auth::build_auth_context_with_session(
                        identity,
                        &self.sessions,
                        session_id,
                    )
                }
            }
        } else {
            crate::control::server::session_auth::build_auth_context_with_session(
                identity,
                &self.sessions,
                session_id,
            )
        };

        // Extract per-query ON DENY override.
        let clean_sql =
            crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, returning_spec) = super::super::returning::strip_returning(&clean_sql)
            .map_err(StatementSetupError::from)?;
        let has_returning = returning_spec.is_some();

        // Forward every per-session planning GUC (vector-dim quota, force-shuffle
        // join/agg overrides + partition counts, broadcast / shuffle-aggregate
        // cost thresholds) into the shared query context. Protocol-neutral so
        // pgwire and native honor these identically; the returned flags drive the
        // plan-cache bypass decision below.
        let override_flags =
            crate::control::server::shared::planning_overrides::apply_planning_session_overrides(
                &self.query_ctx,
                &self.sessions,
                &self.state,
                session_id,
                tenant_id,
            );

        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);
        // A resolved opaque auth-session can predate `USE DATABASE`; the
        // selected database for this statement is authoritative for RLS.
        auth_ctx.database_id = Some(database_id);

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(
            &clean_sql,
            identity,
            tenant_id,
            database_id,
            &auth_ctx,
        )
        .await
        .map_err(StatementSetupError::from)?;

        // Validate enum-typed column values for INSERT/UPDATE before planning.
        self.enforce_enum_labels_if_needed(&clean_sql, tenant_id, database_id)
            .await
            .map_err(StatementSetupError::from)?;

        // Check plan cache before full planning. The cache key is
        // `(sql_hash, schema_version)` and does NOT vary by session knob, so it
        // is bypassed entirely while any strategy override (force-shuffle
        // join/agg, or a non-default broadcast / shuffle-aggregate threshold) is
        // engaged: a cached plan built under a different join-strategy assumption
        // would otherwise be served (and a strategy-specific plan must not be
        // cached for a later default query). Skipping read AND put keeps the
        // cache strategy-knob-free.
        let bypass_cache = override_flags.bypass_plan_cache();
        let cached_tasks = if bypass_cache {
            None
        } else {
            let state = Arc::clone(&self.state);
            let tenant = tenant_id.as_u64();
            let db = database_id;
            self.sessions
                .get_cached_plan(session_id, &clean_sql, move |id| {
                    current_descriptor_version(&state, tenant, db, id)
                })
        };

        let (tasks, output_schema, versions) = if !params.is_empty() {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let (tasks, output_schema, versions) = self
                .query_ctx
                .plan_sql_with_params_and_rls_and_versions(
                    &clean_sql,
                    params,
                    tenant_id,
                    database_id,
                    &sec,
                )
                .await
                .map_err(StatementSetupError::from)?;
            (tasks, output_schema, versions)
        } else if let Some((tasks, versions, output_schema)) = cached_tasks {
            (tasks, output_schema, versions)
        } else {
            let (planned, output_schema, versions, cache_eligibility) = {
                let perm_cache = self.state.permission_cache.read().await;
                let sec = crate::control::planner::context::PlanSecurityContext {
                    identity,
                    auth: &auth_ctx,
                    rls_store: &self.state.rls,
                    permissions: &self.state.permissions,
                    roles: &self.state.roles,
                    permission_cache: Some(&*perm_cache),
                };
                self.query_ctx
                    .plan_sql_with_rls_and_versions(
                        &clean_sql,
                        tenant_id,
                        database_id,
                        &sec,
                        has_returning,
                    )
                    .await
                    .map_err(StatementSetupError::from)?
            };

            // Strategy overrides and data-dependent identity lowering are not
            // represented by the cache key. Document point plans resolve a
            // mutable PK→surrogate binding while lowering, so caching either a
            // sentinel miss or a partially resolved target set would preserve
            // stale row identity across later writes.
            if !bypass_cache && cache_eligibility.is_cacheable() {
                self.sessions.put_cached_plan(
                    session_id,
                    &clean_sql,
                    planned.clone(),
                    versions.clone(),
                    output_schema.clone(),
                );
            }
            (planned, output_schema, versions)
        };

        // Inject RETURNING spec into DML plans.
        let tasks = if let Some(ref spec) = returning_spec {
            tasks
                .into_iter()
                .map(|mut task| {
                    inject_returning_spec(&mut task.plan, spec.clone());
                    task
                })
                .collect()
        } else {
            tasks
        };

        // Preauthorize the originally planned tasks before execution expands
        // implicit edges. Expansion can mark catalog state and allocate
        // surrogates, while descriptor admission must wait until the expanded
        // task set has received final authorization in the execute path.
        let _preauthorized_tasks = self
            .authorize_tasks(identity, &tasks)
            .map_err(StatementSetupError::from)?;

        Ok((tasks, output_schema, versions, auth_ctx))
    }
}

/// Determine read consistency for a set of tasks.
pub(super) fn consistency_for_tasks(tasks: &[PhysicalTask]) -> crate::types::ReadConsistency {
    let has_writes = tasks.iter().any(|t| {
        crate::control::wal_replication::to_replicated_entry(
            t.tenant_id,
            t.database_id,
            t.vshard_id,
            &t.plan,
        )
        .is_some()
    });

    if has_writes {
        crate::types::ReadConsistency::Strong
    } else {
        crate::types::ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
    }
}

/// Inject a RETURNING spec into a DML physical plan variant.
///
/// Only `PointUpdate`, `BulkUpdate`, `PointDelete`, `BulkDelete`,
/// `UpdateFromJoin`, and the CRDT `DocUpsert` / `DocDelete` ops are affected.
/// All other plan variants are left unchanged.
pub(super) fn inject_returning_spec(
    plan: &mut crate::bridge::envelope::PhysicalPlan,
    spec: nodedb_physical::physical_plan::ReturningSpec,
) {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::{CrdtOp, DocumentOp};

    match plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkUpdate { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::PointDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocUpsert { returning, .. }) => {
            *returning = Some(spec);
        }
        PhysicalPlan::Crdt(CrdtOp::DocDelete { returning, .. }) => {
            *returning = Some(spec);
        }
        _ => {}
    }
}

/// Build the pgwire response for one task of a completed Calvin batch.
///
/// A task whose plan carries a RETURNING clause emits its deleted/updated rows
/// as a `Response::Query` decoded from `apply_resp`'s Data-Plane payload — the
/// site that previously dropped those rows, surfacing a bare command tag
/// instead. Every other task (and a RETURNING task with no carried payload)
/// keeps the synthesised `Response::Execution` command tag.
pub(super) fn calvin_execution_response(
    task: &PhysicalTask,
    apply_resp: Option<&crate::bridge::envelope::Response>,
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: crate::types::DatabaseId,
    formats: &[pgwire::api::results::FieldFormat],
) -> pgwire::error::PgWireResult<pgwire::api::results::Response> {
    use super::super::plan::{calvin_tag_for_plan, is_calvin_foldable};
    use crate::control::server::response_shape::compose::{
        ShapeOutcome, shape_response_materialized,
    };
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};

    // RETURNING path: shape the applied payload into DATA-ROWs, exactly as the
    // non-Calvin dispatch loop does for a RETURNING write.
    if let (PlanKind::ReturningRows, Some(resp)) = (describe_plan(&task.plan), apply_resp)
        && let Ok(ShapeOutcome::Rows(shaped)) = shape_response_materialized(
            resp.payload.as_bytes(),
            &task.plan,
            PlanKind::ReturningRows,
            None,
            state,
            database_id,
            tenant_id,
        )
    {
        let (response, _notice) =
            super::super::shape_encode::shaped_query_response(shaped, formats);
        return Ok(response);
    }

    // Plain (non-RETURNING) write: surface its ACTUAL affected count from the
    // payload — exactly as the non-Calvin write path does.
    //
    // Every primary-write participant deposits its applied `Response` before
    // proposing the completion ack (cross-node it rides back on the routed
    // submit's RPC reply), so a count-bearing plan ALWAYS has one here. If it
    // does not, the deposit path regressed: fail loudly rather than synthesise a
    // count, which is what made a delete of an absent row report a removed row.
    if let PlanKind::DmlResult(tag) = describe_plan(&task.plan) {
        let resp = apply_resp.ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "internal: Calvin {tag} completed with no applied response to read its \
                     affected-row count from"
                ),
            )))
        })?;
        return Ok(super::super::plan::payload_to_response(
            resp.payload.as_bytes(),
            describe_plan(&task.plan),
        )?
        .response);
    }

    let tag = if is_calvin_foldable(&task.plan) {
        calvin_tag_for_plan(&task.plan)?
    } else {
        Tag::new("OK")
    };
    Ok(pgwire::api::results::Response::Execution(tag))
}
