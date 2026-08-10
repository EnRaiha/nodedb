// SPDX-License-Identifier: BUSL-1.1

//! SQL planning: converts SQL text into physical task lists, and selects the
//! read consistency a planned task set requires.
//!
//! Calvin batch response shaping lives in `calvin_response.rs`.

use std::sync::Arc;

use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::returning;
use crate::control::server::shared::session::SessionId;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;
use super::catalog::current_descriptor_version;
use super::setup_error::StatementSetupError;

impl NodeDbPgHandler {
    /// Run the request-admission gate exactly once for a pgwire statement.
    ///
    /// Called from `execute_single_sql` before it branches to
    /// `shared::ddl::dispatch` or falls through to the DataFusion planner —
    /// one call covers both DDL/DSL text and ordinary DML/SELECT statements,
    /// so `plan_statement_to_tasks` (the planner's own entry point) must not
    /// admit again.
    pub(in crate::control::server::pgwire::handler) async fn admit_statement(
        &self,
        identity: &AuthenticatedIdentity,
        session_id: SessionId,
        database_id: DatabaseId,
    ) -> pgwire::error::PgWireResult<()> {
        let peer_addr = match session_id {
            SessionId::Connection(connection_id) => self
                .sessions
                .connection_metadata(connection_id)
                .map(|metadata| metadata.peer_addr)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "XX000".to_owned(),
                        "connection session metadata is unavailable".to_owned(),
                    )))
                })?,
            SessionId::LegacySocket(peer_addr) => peer_addr,
        };
        let peer_addr = peer_addr.to_string();
        let request = RequestAuthScope::builder(identity, self.state.auth_stores())
            .with_session_database(Some(database_id))
            .build_for_client(&peer_addr);
        crate::control::server::session_auth::check_request_admission(&self.state, &request, "sql")
            .map_err(|e| {
                let (severity, code, message) =
                    crate::control::server::pgwire::types::error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;
        Ok(())
    }

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
            AuthContext,
        ),
        StatementSetupError,
    > {
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

        // Resolve the request database ONCE, up front, so every downstream
        // consumer — the scope's own `auth.database_id`, constraint/enum
        // enforcement, plan-cache keying, and the planner itself — reads the
        // exact same value instead of each re-querying session state
        // independently and risking drift.
        let database_id = self
            .sessions
            .get_current_database(session_id)
            .unwrap_or(crate::types::DatabaseId::DEFAULT);

        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        // Network provenance is immutable accept-time metadata; all mutable
        // session state remains keyed by the collision-free SessionId.
        let adopted_auth_ctx = if let Some(handle) = self
            .sessions
            .get_parameter(session_id, "nodedb.auth_session")
        {
            use crate::control::security::session_handle::ResolveOutcome;
            match self
                .state
                .session_handles
                .resolve(&handle, &conn_key, &caller_fp)
            {
                ResolveOutcome::Resolved(cached) => Some(*cached),
                ResolveOutcome::RateLimited => {
                    return Err(StatementSetupError::protocol(
                        "FATAL",
                        "53300",
                        "session handle resolve rate limit exceeded on this \
                         connection — closing",
                    ));
                }
                ResolveOutcome::Miss => None,
            }
        } else {
            None
        };

        // Session-level `ON DENY` override lives only in session parameters —
        // the one piece of the old `build_auth_context_with_session` this
        // builder chain cannot absorb via `with_session_database` alone.
        let session_on_deny = crate::control::server::session_auth::session_on_deny_override(
            &self.sessions,
            session_id,
        );

        // Adopt the pooled handle's cached context when present, else let the
        // builder construct a fresh one from `identity`. Either way this
        // re-stamps `database_id` through the same single path every other
        // transport's `RequestAuthScope` resolution uses, and (new for the
        // pooled-handle case) runs scope-grant enrichment, which a cached
        // context never received after the moment it was created.
        let mut scope_builder = RequestAuthScope::builder(identity, self.state.auth_stores())
            .with_session_database(Some(database_id))
            .with_on_deny(session_on_deny);
        if let Some(adopted) = adopted_auth_ctx {
            scope_builder = scope_builder.with_adopted_auth_context(adopted);
        }
        // The planning scope is a different value than the one
        // `admit_statement` built, so it is resolved against the same
        // connection address — otherwise `$auth.risk_score` would be unset and
        // an IP-conditional grant withheld on every pgwire statement, even
        // though admission itself passed. This scope is never presented to an
        // admission door (that already ran), so the binding is unwrapped
        // immediately.
        let scope = scope_builder
            .build_for_client(&peer_addr.to_string())
            .into_scope();

        // Request-admission already ran once for this statement in
        // `execute_single_sql`, before it branched to `shared::ddl::dispatch`
        // or fell through to this planner — that single call covers both DDL
        // and the DataFusion path, so this function must not admit again.

        // Extract per-query ON DENY override. Per-query always wins over the
        // session-level override already baked into `scope`.
        let (clean_sql, scope) =
            crate::control::server::session_auth::apply_per_query_on_deny(sql, scope);

        // Strip RETURNING clause before DataFusion planning.
        let (clean_sql, returning_spec) =
            returning::strip_returning(&clean_sql).map_err(StatementSetupError::from)?;
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

        // The database resolved once above, at the top of this function, is
        // authoritative for both `PhysicalTask::database_id` and
        // `$auth.database_id` — `scope` already carries both in lockstep.
        let database_id = scope.database_id();

        // Enforce general CHECK constraints for INSERT/UPDATE before planning.
        self.enforce_check_constraints_if_needed(
            &clean_sql,
            identity,
            tenant_id,
            database_id,
            scope.auth(),
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
                auth: scope.auth(),
                rls_store: &self.state.rls,
                redaction_store: &self.state.redaction,
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
            // The fail-closed redaction refusal is a property of the CURRENT
            // policy set, not of the compiled plan: a `CREATE REDACTION POLICY`
            // issued after this statement was cached must refuse the cached
            // aggregate on its very next execution. The per-session plan cache
            // is keyed on collection descriptor versions, which a policy write
            // does not (and should not) bump, so re-running the pass on every
            // cache hit is what keeps the verdict live — exactly as the masking
            // hook reads the store live at shaping time.
            crate::control::planner::redaction_refusal::refuse_unredactable_tasks(
                &tasks,
                scope.auth(),
                &self.state.redaction,
            )
            .map_err(StatementSetupError::from)?;
            (tasks, output_schema, versions)
        } else {
            let (planned, output_schema, versions, cache_eligibility) = {
                let perm_cache = self.state.permission_cache.read().await;
                let sec = crate::control::planner::context::PlanSecurityContext {
                    identity,
                    auth: scope.auth(),
                    rls_store: &self.state.rls,
                    redaction_store: &self.state.redaction,
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
        //
        // An insert shape with no `returning` slot is refused here rather than
        // silently dropped: the shape the planner produced is only visible on
        // the plan, so only the plan can tell whether the clause has anywhere
        // to go.
        let tasks = if let Some(ref spec) = returning_spec {
            let mut injected = Vec::with_capacity(tasks.len());
            for mut task in tasks {
                returning::refuse_unprojectable_insert_returning(&task.plan)
                    .map_err(StatementSetupError::from)?;
                returning::inject_returning_spec(&mut task.plan, spec.clone());
                injected.push(task);
            }
            injected
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

        // The caller (`execute_planned_sql_inner`) only needs the resolved
        // `AuthContext` by value from here on (e.g. for trigger OLD-row RLS,
        // keyed by the task's own `database_id` rather than this scope's) —
        // `scope` itself does not need to outlive this function.
        Ok((tasks, output_schema, versions, scope.auth().clone()))
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
