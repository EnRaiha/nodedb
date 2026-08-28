// SPDX-License-Identifier: BUSL-1.1

//! Shared Data-Plane read helpers used by both the Document and KV clone
//! write-interception paths: presence probes on the target collection, and
//! source-row/value fetches for copy-up.

use std::time::Duration;

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::{Priority, Request, Response, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

/// Probe whether `document_id` exists in target storage.
///
/// Issues a synchronous PointGet to the local Data Plane and returns `true`
/// if the row is present.  Uses `Surrogate::ZERO` when the catalog has no
/// registered surrogate for the PK — the handler will return "not found".
pub(super) async fn probe_row_in_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    db_id: DatabaseId,
    collection_qualified: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<bool> {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection_qualified.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, db_id, plan)?;
    let vshard_id = VShardId::from_collection_in_database(db_id, collection_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, db_id, plan).await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw msgpack bytes for a row from the source collection.
///
/// Returns `None` when the row is absent in source (PointGet returned empty).
pub(super) async fn fetch_source_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: source_coll_qualified.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, source_db_id, plan)?;
    let vshard_id = VShardId::from_collection_in_database(source_db_id, source_coll_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, source_db_id, plan).await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Probe whether `kv_key` exists in target KV storage.
///
/// Issues a KvOp::Get to the local Data Plane and returns `true` if the key
/// is present.
pub(super) async fn probe_kv_key_in_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    db_id: DatabaseId,
    collection_qualified: &str,
    kv_key: &[u8],
) -> crate::Result<bool> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: collection_qualified.to_string(),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Internal probe of the clone's own target collection — never
        // delegated to source, so no isolation ceiling applies.
        surrogate_ceiling: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, db_id, plan)?;
    let vshard_id = VShardId::from_collection_in_database(db_id, collection_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, db_id, plan).await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw value bytes for a KV row from the source collection.
///
/// Returns `None` when the key is absent in source (KvOp::Get returned empty).
pub(super) async fn fetch_kv_source_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    kv_key: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: source_coll_qualified.to_string(),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Copy-up reads must see every binding in the source — the
        // post-copy target write reflects the latest source state, and
        // a missed source row would silently drop data on the clone.
        surrogate_ceiling: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, source_db_id, plan)?;
    let vshard_id = VShardId::from_collection_in_database(source_db_id, source_coll_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, source_db_id, plan).await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Apply the requesting principal's row-level security to a probe plan.
///
/// Copy-up reads a row out of the source collection and writes it into the
/// clone, where the source's policies no longer govern it. Without this the
/// clone would launder policy-excluded rows into readable ones. Presence probes
/// carry the same filters so a row the caller cannot see is not reported as
/// present either.
///
/// `database_id` is the database the probe plan actually targets (the
/// source database for copy-up reads, the clone's own database for presence
/// probes) — `RequestAuthScope::for_database` stamps it into `$auth.database_id`
/// in lockstep with the scalar so a database-scoped RLS predicate evaluates
/// against the database this probe reads, not the caller's session default.
fn with_caller_rls(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    mut plan: PhysicalPlan,
) -> crate::Result<PhysicalPlan> {
    let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
        identity,
        state.auth_stores(),
        database_id,
    );
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &state.rls,
        scope.auth(),
    )?;
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        scope.auth(),
        &state.redaction,
    )?;
    Ok(plan)
}

/// Dispatch a plan directly to the local Data Plane, bypassing WAL and Raft.
/// Used only for read probes inside the clone write helper.
pub(super) async fn dispatch_data_plane_raw(
    state: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let req_id = RequestId::new(
        state
            .request_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    );
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        deadline: std::time::Instant::now() + deadline_dur,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ),
    };
    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    }
    tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone write probe: response channel closed".into(),
        })
}
