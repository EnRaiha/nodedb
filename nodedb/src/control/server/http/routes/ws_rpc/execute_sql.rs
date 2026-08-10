// SPDX-License-Identifier: BUSL-1.1

//! SQL execution via the SPSC gateway for WebSocket RPC.

use std::sync::Arc;

use crate::control::gateway::core::QueryContext;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_decoded_value};
use crate::control::server::shared::authorization::authorize_database;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::plan_admission::{
    PlanAdmissionRequest, plan_authorize_and_admit,
};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
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
    peer_addr: &str,
) -> crate::Result<serde_json::Value> {
    let tenant_id = identity.tenant_id;
    let emitter = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_database(identity, database_id, &emitter)?;

    // Quota enforcement — reject before planning or dispatch.
    shared.check_tenant_quota(tenant_id)?;

    // The RPC-selected database is authoritative for RLS variables — passed
    // as the session database so `scope.database_id()` resolves to exactly
    // `database_id` rather than falling back through `identity`'s default.
    let request = RequestAuthScope::builder(identity, shared.auth_stores())
        .with_session_database(Some(database_id))
        .build_for_client(peer_addr);

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — before any planning/dispatch, so load is
    // shed before it is spent. WebSocket RPC has no HTTP response headers to
    // attach `X-RateLimit-*` to, so the rate-limit outcome is discarded on
    // success; a denial still fails the request closed via `?`.
    crate::control::server::session_auth::check_request_admission(shared, &request, "sql")?;

    let (clean_sql, scope) =
        crate::control::server::session_auth::apply_per_query_on_deny(sql, request.into_scope());
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

    // Resolved once per request from the tasks' plans and the caller's own
    // roles, then reused (never re-derived) at every RETURNING/result decode
    // site below — an `InsertSelect`/`Merge`/`UpdateFromJoin` row and a plain
    // dispatch row must be redacted by the same policy snapshot.
    let redaction =
        QueryRedaction::for_plans(tenant_id, scope.auth(), tasks.iter().map(|t| &t.plan));

    let mut results = Vec::new();
    // Checked once rather than per task — metering is disabled by default,
    // so this keeps the per-task extraction below (which clones the
    // collection name) a true no-op on the hot path for every deployment
    // that hasn't turned it on.
    let metering_enabled = shared.metering_config.enabled;
    for (task, authorized_task) in tasks.into_iter().zip(authorized_tasks) {
        // Extracted from `task.plan` before it's cloned/moved into any
        // branch below — metering needs the collection/engine shape after
        // this task's dispatch succeeds. `results.len()` before dispatch
        // gives this task's row-count baseline for the delta metered below.
        let plan_metering_info = metering_enabled.then(|| PlanMeteringInfo::extract(&task.plan));
        // A spent hard quota refuses the task before it runs; the charging
        // calls below are all on the success path and so can never refuse
        // anything themselves.
        if let Some(info) = &plan_metering_info {
            admit_quota_for_dispatch(shared, &scope, info)?;
        }
        let rows_before = results.len();
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
                            Ok(mut v) => {
                                redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                                results.push(v);
                            }
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
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
                rls_filters: _,
                rls_write_check: _,
                resolved_sum_targets: _,
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
                            Ok(mut v) => {
                                redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                                results.push(v);
                            }
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
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
                rls_filters: _,
                rls_write_check: _,
                resolved_sum_targets: _,
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
                            Ok(mut v) => {
                                redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                                results.push(v);
                            }
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
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
                            Ok(mut v) => {
                                redact_decoded_value(Some(&redaction), &shared.redaction, &mut v);
                                results.push(v);
                            }
                            Err(_) => results.push(serde_json::Value::String(json)),
                        }
                    }
                }
            }
            Err(e) => return Err(e),
        }
        meter_task(shared, &scope, &plan_metering_info, rows_before, &results);
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

/// Meter one task's dispatch, once its rows (if any) have already been
/// pushed onto `results` — the row count is the delta since `rows_before`,
/// so this must run after every push point for the task, never before.
fn meter_task(
    shared: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &Option<PlanMeteringInfo>,
    rows_before: usize,
    results: &[serde_json::Value],
) {
    if let Some(info) = info {
        let task_rows = (results.len() - rows_before) as u64;
        meter_dispatch(shared, scope, info, Some(task_rows));
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::planner::context::QueryContext as PlannerQueryContext;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use, mirroring
    /// `session_auth::admission`'s test harness.
    async fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    fn regular_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// Guards against the `peer_addr` thread regressing back to a hardcoded
    /// placeholder (e.g. `"ws"`): a real client IP that has been
    /// `BLACKLIST IP`'d must be rejected by `execute_sql`'s admission gate.
    /// With a non-IP placeholder standing in for the peer address,
    /// `normalize_peer_ip` can't parse it, `check_ip` silently matches
    /// nothing, and this request would incorrectly succeed.
    #[tokio::test]
    async fn blacklisted_peer_ip_is_rejected() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9101);
        state
            .blacklist
            .blacklist_ip("203.0.113.0/24", "test ban", "admin", 0)
            .expect("blacklist CIDR range");

        let query_ctx = PlannerQueryContext::new();
        let trace_id = TraceId::generate();

        let result = execute_sql(
            &state,
            &query_ctx,
            &identity,
            DatabaseId::DEFAULT,
            "SELECT 1",
            trace_id,
            "203.0.113.42:54321",
        )
        .await;

        let error = result.expect_err("blacklisted peer IP must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("IP blacklisted"),
            "expected an IP-blacklist rejection, got: {message}"
        );
    }

    /// A peer address outside the blacklisted range must still be admitted
    /// through the blacklist gate (it can fail later in planning/dispatch
    /// for unrelated reasons, but not on the IP check).
    #[tokio::test]
    async fn non_blacklisted_peer_ip_passes_blacklist_gate() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9102);
        state
            .blacklist
            .blacklist_ip("203.0.113.0/24", "test ban", "admin", 0)
            .expect("blacklist CIDR range");

        let query_ctx = PlannerQueryContext::new();
        let trace_id = TraceId::generate();

        let result = execute_sql(
            &state,
            &query_ctx,
            &identity,
            DatabaseId::DEFAULT,
            "SELECT 1",
            trace_id,
            "198.51.100.7:54321",
        )
        .await;

        if let Err(error) = result {
            let message = error.to_string();
            assert!(
                !message.contains("IP blacklisted"),
                "peer outside the blacklisted range must not be rejected by the IP check, got: {message}"
            );
        }
    }

    fn kv_get_plan() -> crate::bridge::envelope::PhysicalPlan {
        crate::bridge::envelope::PhysicalPlan::Kv(nodedb_physical::physical_plan::KvOp::Get {
            collection: "widgets".into(),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    fn scope_for<'a>(
        identity: &'a AuthenticatedIdentity,
        state: &'a SharedState,
    ) -> RequestAuthScope<'a> {
        RequestAuthScope::for_database(identity, state.auth_stores(), DatabaseId::DEFAULT)
    }

    /// `meter_task` is called unconditionally after every dispatch branch in
    /// `execute_sql` (see the source above — always right after `results` is
    /// updated, never on the `Err(e) => return Err(e)` arms), so covering it
    /// directly here exercises the same enabled/disabled and row-count
    /// behavior every call site relies on.
    #[tokio::test]
    async fn meter_task_disabled_by_default_records_nothing() {
        let (state, _dir) = test_state().await;
        assert!(!state.metering_config.enabled, "default is disabled");
        let identity = regular_identity(9201);
        let scope = scope_for(&identity, &state);
        // `Some(...)` regardless of the disabled config, to prove
        // `meter_dispatch`'s own internal enabled-check (not just the call
        // site's `metering_enabled.then(...)` gate) protects this call.
        let info = Some(PlanMeteringInfo::extract(&kv_get_plan()));
        let results = vec![serde_json::Value::Null; 3];

        meter_task(&state, &scope, &info, 0, &results);

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[tokio::test]
    async fn meter_task_enabled_records_one_event_with_row_delta() {
        let (mut state, _dir) = test_state().await;
        std::sync::Arc::get_mut(&mut state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
        let identity = regular_identity(9202);
        let scope = scope_for(&identity, &state);
        let info = metering_enabled_info(&state, &kv_get_plan());
        let results = vec![serde_json::Value::Null; 5];

        // rows_before = 2: this task contributed 3 of the 5 entries in `results`.
        meter_task(&state, &scope, &info, 2, &results);

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(events[0].tokens, expected_cost * 3);
    }

    fn metering_enabled_info(
        state: &SharedState,
        plan: &crate::bridge::envelope::PhysicalPlan,
    ) -> Option<PlanMeteringInfo> {
        state
            .metering_config
            .enabled
            .then(|| PlanMeteringInfo::extract(plan))
    }
}
