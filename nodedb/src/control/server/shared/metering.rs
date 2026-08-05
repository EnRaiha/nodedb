// SPDX-License-Identifier: BUSL-1.1

//! Post-dispatch usage metering: attribute a completed [`PhysicalTask`] to
//! its collection/engine/operation and record it against the caller's usage
//! bucket.
//!
//! [`PhysicalTask`]: nodedb_physical::physical_task::PhysicalTask

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::metering::counter::UsageEvent;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::plan_util::{extract_collection, plan_engine};
use crate::control::state::SharedState;
use nodedb_types::calvin::EngineTag;

/// Map an [`EngineTag`] to its stable metering dimension string. Exhaustive
/// over every variant so a new engine forces this mapping to be updated
/// rather than silently billing under a wrong or missing label.
fn engine_tag_str(tag: EngineTag) -> &'static str {
    match tag {
        EngineTag::Vector => "vector",
        EngineTag::Graph => "graph",
        EngineTag::Document => "document",
        EngineTag::Kv => "kv",
        EngineTag::Text => "text",
        EngineTag::Columnar => "columnar",
        EngineTag::Timeseries => "timeseries",
        EngineTag::Spatial => "spatial",
        EngineTag::Crdt => "crdt",
        EngineTag::Query => "query",
        EngineTag::Meta => "meta",
        EngineTag::Array => "array",
        EngineTag::ClusterArray => "cluster_array",
    }
}

/// Map a physical plan to the metering `operation` cost-table key (see
/// `MeteringConfig::operation_costs`).
///
/// Moved here from `shared::ddl::user_dispatch` so the rate-limiter's
/// operation classification and the metering cost-table lookup share one
/// mapping instead of two copies that could silently diverge and mis-bill.
///
/// This door carries only a handful of engine-specific DSL/TVF operations
/// (CRDT read/merge, timeseries last-value, GraphRAG fusion, snapshot scan),
/// so a coarse top-level match is enough to apply the right cost tier; an
/// engine with no natural cost-table counterpart falls back to the default
/// cost of 1.
pub(crate) fn operation_for_plan(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::Vector(_) => "vector_search",
        PhysicalPlan::Graph(_) => "graph_hop",
        PhysicalPlan::Document(_) => "document_scan",
        PhysicalPlan::Kv(_) => "kv_scan",
        PhysicalPlan::Text(_) => "text_search",
        PhysicalPlan::Columnar(_) | PhysicalPlan::Timeseries(_) | PhysicalPlan::Spatial(_) => {
            "document_scan"
        }
        PhysicalPlan::Crdt(_) => "point_get",
        PhysicalPlan::Query(_) => "aggregate",
        PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => "sql",
    }
}

/// The metering-relevant shape of a [`PhysicalPlan`], captured before
/// dispatch consumes the plan.
///
/// Callers that need to meter after dispatch (dispatch takes the plan by
/// value to build the `PhysicalTask`, so the original plan is gone by the
/// time the response comes back) extract this narrow shape instead of
/// `plan.clone()`-ing the whole plan: a `PhysicalPlan` can carry large
/// payloads (vector floats, row upserts, filter trees), while metering only
/// ever reads the collection name, engine, and operation classification.
pub(crate) struct PlanMeteringInfo {
    collection: Option<String>,
    engine: EngineTag,
    operation: &'static str,
}

impl PlanMeteringInfo {
    /// Extract `plan`'s metering shape.
    ///
    /// Call this only when `state.metering_config.enabled` — it clones the
    /// collection name, which is wasted work otherwise (metering is
    /// disabled by default).
    pub(crate) fn extract(plan: &PhysicalPlan) -> Self {
        Self {
            collection: extract_collection(plan).map(str::to_string),
            engine: plan_engine(plan),
            operation: operation_for_plan(plan),
        }
    }

    /// Build a metering shape directly, for dispatch doors with no
    /// [`PhysicalPlan`] to extract from — e.g. whole-tenant backup/restore,
    /// which operates on a tenant, not a single collection's plan.
    pub(crate) fn for_collection(
        collection: String,
        engine: EngineTag,
        operation: &'static str,
    ) -> Self {
        Self {
            collection: Some(collection),
            engine,
            operation,
        }
    }
}

/// Meter one completed [`PhysicalTask`](nodedb_physical::physical_task::PhysicalTask)
/// dispatch against the caller's usage bucket.
///
/// Metering is per `PhysicalTask`, not per statement. A single statement can
/// expand into several tasks — an implicit graph edge write alongside its
/// node write, or an `INSERT ... SELECT`'s read and write halves — each
/// dispatched and billed independently. There is no cross-task aggregation:
/// the per-task unit is the natural one because each task is authorized,
/// admitted, and executed as its own capability.
///
/// Callers MUST only call this on the success path. A denied, errored, or
/// timed-out request performed no billable engine work; metering it would
/// charge the caller for work that never happened.
///
/// Returns immediately when metering is disabled (the default) or when
/// `scope` belongs to an internal-service identity (WAL replay, triggers,
/// the scheduler, CRDT sync) — billing a tenant for server-owned work would
/// be wrong. Plans with no extractable collection (cluster/algo/meta ops
/// with no user-facing collection) are not metered: there is nothing to
/// attribute the usage to.
pub(crate) fn meter_dispatch(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &PlanMeteringInfo,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    if scope.identity().is_internal_service() {
        return;
    }
    let Some(collection) = info.collection.as_deref() else {
        return;
    };
    let engine = engine_tag_str(info.engine);
    let operation = info.operation;
    let operation_cost = state
        .metering_config
        .operation_costs
        .get(operation)
        .copied()
        .unwrap_or(1);
    // Never charge zero: even a point-get miss performed a lookup.
    let tokens = operation_cost.saturating_mul(rows.unwrap_or(1).max(1));

    state.usage_counter.record(&UsageEvent {
        auth_user_id: scope.auth().id.clone(),
        org_id: scope.auth().org_id.clone().unwrap_or_default(),
        tenant_id: scope.tenant_id().as_u64(),
        collection: collection.to_string(),
        engine: engine.to_string(),
        operation: operation.to_string(),
        tokens,
        // Filled in by `UsageCounter::drain`, not the caller.
        timestamp_secs: 0,
    });
}

/// A metering accumulator for a streaming response, live for the whole
/// stream's lifetime and independent of how it ends.
///
/// The non-streaming [`meter_dispatch`] fires once, right after dispatch
/// returns — but a streaming response (NDJSON, `ws_rpc` scan streams) writes
/// rows to the client incrementally, and the client can disconnect before
/// the last one. Billing must reflect rows the client actually received, not
/// rows the plan would have produced had the stream run to completion. Rows
/// are added as they are written via [`Self::add_rows`]; the accumulated
/// total is recorded as a single usage event when this guard drops —
/// whether that is normal stream completion, a mid-stream error, or an early
/// client disconnect (dropping the response body future drops every local
/// the stream generator holds, including this guard).
pub(crate) struct DetachedMeterGuard {
    state: Arc<SharedState>,
    auth_user_id: String,
    org_id: String,
    tenant_id: u64,
    collection: String,
    engine: &'static str,
    operation: &'static str,
    rows: u64,
}

impl DetachedMeterGuard {
    /// Build a guard for `info`, or `None` when nothing should be metered —
    /// mirrors [`meter_dispatch`]'s own gating (disabled config, internal
    /// service identity, no extractable collection) so a streaming caller
    /// gets identical gating without duplicating the checks.
    pub(crate) fn new(
        state: &Arc<SharedState>,
        scope: &RequestAuthScope<'_>,
        info: &PlanMeteringInfo,
    ) -> Option<Self> {
        if !state.metering_config.enabled || scope.identity().is_internal_service() {
            return None;
        }
        let collection = info.collection.clone()?;
        Some(Self {
            state: Arc::clone(state),
            auth_user_id: scope.auth().id.clone(),
            org_id: scope.auth().org_id.clone().unwrap_or_default(),
            tenant_id: scope.tenant_id().as_u64(),
            collection,
            engine: engine_tag_str(info.engine),
            operation: info.operation,
            rows: 0,
        })
    }

    /// Record that `n` more rows were actually written to the client.
    pub(crate) fn add_rows(&mut self, n: u64) {
        self.rows += n;
    }
}

impl Drop for DetachedMeterGuard {
    fn drop(&mut self) {
        let operation_cost = self
            .state
            .metering_config
            .operation_costs
            .get(self.operation)
            .copied()
            .unwrap_or(1);
        // Never charge zero: even a stream that closed before any row was
        // written performed a lookup — see `meter_dispatch`'s identical rule.
        let tokens = operation_cost.saturating_mul(self.rows.max(1));
        self.state.usage_counter.record(&UsageEvent {
            auth_user_id: self.auth_user_id.clone(),
            org_id: self.org_id.clone(),
            tenant_id: self.tenant_id,
            collection: self.collection.clone(),
            engine: self.engine.to_string(),
            operation: self.operation.to_string(),
            tokens,
            timestamp_secs: 0,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::{DatabaseId, TenantId};
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use, or the WAL's
    /// backing file is removed out from under it.
    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
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
            DatabaseSet::All,
        )
    }

    fn internal_service_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            user_id,
            "internal-service",
            TenantId::new(1),
            vec![],
            false,
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    fn kv_get_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: "widgets".into(),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// A plan with no extractable collection (a graph hop carries no
    /// top-level collection field).
    fn no_collection_plan() -> PhysicalPlan {
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::CreateSnapshot)
    }

    /// `metering_config` has no live-mutation path by design (see
    /// `SharedState::metering_config`'s doc comment) — tests that need it
    /// enabled reach in via `Arc::get_mut` while the test is still the sole
    /// owner of the freshly constructed state, before any clone escapes.
    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    fn scope_for<'a>(
        identity: &'a AuthenticatedIdentity,
        state: &'a SharedState,
    ) -> RequestAuthScope<'a> {
        RequestAuthScope::for_database(identity, &state.scope_grants, DatabaseId::DEFAULT)
    }

    #[test]
    fn disabled_config_records_nothing() {
        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = regular_identity(1);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn internal_service_identity_is_never_metered() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = internal_service_identity(2);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn plan_with_no_collection_is_not_metered() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(3);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&no_collection_plan()),
            None,
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn enabled_plan_records_exactly_one_event_with_correct_fields() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(4);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.collection, "widgets");
        assert_eq!(event.engine, "kv");
        assert_eq!(event.operation, "kv_scan");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(event.tokens, expected_cost * 3);
    }

    #[test]
    fn none_and_zero_rows_both_charge_at_least_one_unit() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(5);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            None,
        );
        let events_none = state.usage_counter.drain();
        assert_eq!(events_none.len(), 1);
        assert!(events_none[0].tokens >= 1);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(0),
        );
        let events_zero = state.usage_counter.drain();
        assert_eq!(events_zero.len(), 1);
        assert!(events_zero[0].tokens >= 1);
    }

    /// `operation_for_plan` moved here from `shared::ddl::user_dispatch` —
    /// this pins its existing behavior for the operation strings that
    /// module's rate-limiter classification depends on.
    #[test]
    fn operation_for_plan_matches_expected_vocabulary() {
        assert_eq!(operation_for_plan(&kv_get_plan()), "kv_scan");
        assert_eq!(
            operation_for_plan(&no_collection_plan()),
            "sql",
            "Meta ops with no cost-table counterpart fall back to \"sql\""
        );
    }

    #[test]
    fn detached_guard_disabled_config_records_nothing() {
        let (state, _dir) = test_state();
        let identity = regular_identity(6);
        let scope = scope_for(&identity, &state);

        let guard =
            DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()));
        assert!(guard.is_none(), "disabled config must not build a guard");
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// The whole point of the guard: a stream that writes some rows and then
    /// drops early (client disconnect, mid-stream error) still bills exactly
    /// what it wrote — not zero, and not what a full scan would have
    /// produced. Dropping the guard mid-accumulation, without ever calling
    /// a "finish" method, is the case that matters: it is what actually
    /// happens when `async_stream::stream!`'s generator future is dropped.
    #[test]
    fn detached_guard_bills_rows_written_before_early_drop() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(7);
        let scope = scope_for(&identity, &state);

        {
            let mut guard =
                DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()))
                    .expect("metering enabled, collection present");
            guard.add_rows(3);
            guard.add_rows(4);
            // Dropped here, mid-"stream", with no explicit finish call —
            // simulates an early client disconnect after 7 rows were sent.
        }

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1, "exactly one event, recorded on drop");
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(events[0].tokens, expected_cost * 7);
    }

    #[test]
    fn detached_guard_charges_one_unit_when_no_rows_were_ever_written() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(8);
        let scope = scope_for(&identity, &state);

        drop(
            DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()))
                .expect("metering enabled, collection present"),
        );

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert!(events[0].tokens >= 1);
    }
}
